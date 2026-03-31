#!/usr/bin/env python3
"""Update TTW app-export CSV with data from a council register CSV.

Fuzzy-matches rows by name+postcode and updates election fields, tags, notes,
and dates in the app-export, respecting the TTW allowed value formats.

Usage:
    python3 tools/update_app_export.py APP_EXPORT.csv REGISTER.csv OUTPUT.csv
    python3 tools/update_app_export.py APP_EXPORT.csv REGISTER.csv OUTPUT.csv --report report.txt
    python3 tools/update_app_export.py APP_EXPORT.csv REGISTER.csv OUTPUT.csv --date 2026-Mar-31
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ttw_common import read_input, normalize_postcode, map_party_name, UK_POSTCODE_RE
from enrich_register import (
    _surname_forename_similarity,
    _normalize_address,
    _address_similarity,
    _get_postal_voter,
)
from clean_register import normalize_date

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Election column prefixes (verified from real app-export CSV)
LE2026_PREFIX = "Brent London Borough Council election (2026-May-07)"
GE2024_PREFIX = "Brent London Borough Council election (2024-Jul-04)"

# Target column names in app-export
LE2026_GVI = f"{LE2026_PREFIX} Most Recent Data - GVI"
LE2026_PARTY = f"{LE2026_PREFIX} Most Recent Data - Usual Party"
LE2026_POSTAL = f"{LE2026_PREFIX} Most Recent Data - Postal Voter"
LE2026_DATE = f"{LE2026_PREFIX} Most Recent Data - Date"
GE2024_VOTED = f"{GE2024_PREFIX} Voted"

# App-export identity columns
APP_SURNAME = "Surname"
APP_FORENAME = "First Name"
APP_POSTCODE = "Post Code"
APP_HOUSE_NAME = "House Name"
APP_HOUSE_NUMBER = "House Number"
APP_ROAD = "Road"

# Register column variants
REG_SURNAME_KEYS = ["ElectorSurname", "Surname", "Last Name"]
REG_FORENAME_KEYS = ["ElectorForename", "Forename", "First Name"]
REG_POSTCODE_KEYS = ["PostCode", "Postcode", "Post Code", "POSTCODE"]
REG_ADDRESS_KEYS = [
    ("RegisteredAddress1", "RegisteredAddress2", "RegisteredAddress3",
     "RegisteredAddress4", "RegisteredAddress5", "RegisteredAddress6"),
    ("Address1", "Address2", "Address3", "Address4", "Address5", "Address6"),
]

# Required app-export target columns (checked at startup)
REQUIRED_APP_TARGETS = [
    APP_SURNAME, APP_FORENAME, APP_POSTCODE,
    "Date of Attainment",
    "Poster ticked", "Board ticked", "Do Not Knock ticked",
    "Text of Note 1 (most recent)", "Date of Note 1 (most recent)",
    LE2026_GVI, LE2026_PARTY, LE2026_POSTAL, LE2026_DATE,
    GE2024_VOTED,
]

# Note column name patterns
NOTE_TEXT_KEYS = [
    "Text of Note 1 (most recent)",
    "Text of Note 2", "Text of Note 3", "Text of Note 4", "Text of Note 5",
    "Text of Note 6", "Text of Note 7", "Text of Note 8", "Text of Note 9",
    "Text of Note 10",
]
NOTE_DATE_KEYS = [
    "Date of Note 1 (most recent)",
    "Date of Note 2", "Date of Note 3", "Date of Note 4", "Date of Note 5",
    "Date of Note 6", "Date of Note 7", "Date of Note 8", "Date of Note 9",
    "Date of Note 10",
]

# Reverse party mapping: TTW code -> app-export full name
REVERSE_PARTY_MAP = {
    "G": "Greens",
    "Con": "Conservatives",
    "Lab": "Labour",
    "L": "Labour",
    "LD": "Liberal Democrats",
    "REF": "Reform/UKIP/Brexit",
    "PC": "Plaid Cymru",
    "Ind": "Independent",
    "RA": "Residents Association",
    "Oth": "Others",
}

# Matching thresholds
DEFAULT_THRESHOLD = 0.8
NO_POSTCODE_THRESHOLD = 0.95
AMBIGUITY_MARGIN = 0.15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_field(row, keys, default=""):
    """Try multiple column name variants, return first non-empty value."""
    for key in keys:
        val = row.get(key, "").strip()
        if val:
            return val
    return default


def _get_register_address(row):
    """Build concatenated address string from register row."""
    for addr_keys in REG_ADDRESS_KEYS:
        parts = [row.get(k, "").strip() for k in addr_keys]
        combined = " ".join(p for p in parts if p)
        if combined:
            return combined
    return ""


def _get_register_postcode(row):
    """Extract and normalize postcode from register row."""
    for key in REG_POSTCODE_KEYS:
        val = row.get(key, "").strip()
        if val:
            pc, _ = normalize_postcode(val)
            return pc or ""
    return ""


def _get_app_address(row):
    """Build concatenated address string from app-export row."""
    parts = [
        row.get(APP_HOUSE_NAME, "").strip(),
        row.get(APP_HOUSE_NUMBER, "").strip(),
        row.get(APP_ROAD, "").strip(),
    ]
    return " ".join(p for p in parts if p)


def to_app_date(dd_mm_yyyy):
    """Convert DD/MM/YYYY to YYYY-MMM-DD (e.g. '31/03/2026' -> '2026-Mar-31')."""
    if not dd_mm_yyyy:
        return ""
    try:
        parsed = datetime.strptime(dd_mm_yyyy, "%d/%m/%Y")
        return parsed.strftime("%Y-%b-%d")
    except ValueError:
        return ""


def reverse_map_party(value):
    """Map party code/name to app-export full name.
    Returns (app_name, warning_or_None)."""
    if not value or not value.strip():
        return "", None

    raw = value.strip()
    # First normalize to TTW code via map_party_name
    code, warning = map_party_name(raw)
    if not code:
        return "", warning

    # Then reverse-map to app full name
    app_name = REVERSE_PARTY_MAP.get(code)
    if app_name:
        return app_name, None
    return "", f"Unrecognized party code '{code}' after mapping from '{raw}'"


def shift_notes(row, report, row_key):
    """Shift notes 1->2, 2->3, ..., 9->10. Note 10 is lost if full."""
    # Check if Note 10 has content (will be lost)
    if row.get(NOTE_TEXT_KEYS[9], "").strip():
        report.warnings.append(
            f"{row_key}: Note 10 content lost during shift "
            f"(was: '{row.get(NOTE_TEXT_KEYS[9], '')[:50]}...')")

    # Shift from 9->10, 8->9, ..., 1->2
    for i in range(9, 0, -1):
        row[NOTE_TEXT_KEYS[i]] = row.get(NOTE_TEXT_KEYS[i - 1], "")
        row[NOTE_DATE_KEYS[i]] = row.get(NOTE_DATE_KEYS[i - 1], "")


# ---------------------------------------------------------------------------
# QA Report
# ---------------------------------------------------------------------------

class UpdateReport:
    """Collects report entries during app-export update."""

    def __init__(self):
        self.app_file = ""
        self.register_file = ""
        self.output_file = ""
        self.total_app = 0
        self.total_register = 0
        self.matched = 0
        self.unmatched = 0
        self.possible = 0
        self.ambiguous = 0
        self.duplicate_matches = 0

        self.matched_details = []     # [(reg_name, app_name, postcode, score)]
        self.unmatched_details = []   # [(reg_name, postcode)]
        self.possible_details = []    # [(reg_name, postcode, score, app_name)]
        self.ambiguous_details = []   # [(reg_name, postcode, [(candidate, score)])]

        # Per-field update counts
        self.field_updates = defaultdict(int)
        self.warnings = []

    def write(self, path):
        """Write human-readable report with machine-readable footer."""
        lines = []
        lines.append("=" * 60)
        lines.append("App-Export Update Report")
        lines.append("=" * 60)
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"App-export file: {self.app_file}")
        lines.append(f"Register file: {self.register_file}")
        lines.append(f"Output file: {self.output_file}")
        lines.append("")

        lines.append("--- Summary ---")
        lines.append(f"App-export rows: {self.total_app}")
        lines.append(f"Register rows: {self.total_register}")
        rate = (self.matched / self.total_register * 100) if self.total_register else 0
        lines.append(f"Matched: {self.matched} ({rate:.1f}%)")
        lines.append(f"Possible: {self.possible}")
        lines.append(f"Ambiguous: {self.ambiguous}")
        lines.append(f"Unmatched register rows: {self.unmatched}")
        if self.duplicate_matches:
            lines.append(f"Duplicate matches (ignored): {self.duplicate_matches}")
        lines.append("")

        # Field update counts
        if self.field_updates:
            lines.append("--- Field Updates ---")
            for field, count in sorted(self.field_updates.items()):
                lines.append(f"  {field}: {count}")
            lines.append("")

        # Unmatched
        if self.unmatched_details:
            lines.append("--- Unmatched Register Rows ---")
            for name, pc in self.unmatched_details[:50]:
                lines.append(f"  {name} ({pc or 'no postcode'})")
            if len(self.unmatched_details) > 50:
                lines.append(f"  ... and {len(self.unmatched_details) - 50} more")
            lines.append("")

        # Warnings
        if self.warnings:
            lines.append("--- Warnings ---")
            for w in self.warnings[:50]:
                lines.append(f"  {w}")
            if len(self.warnings) > 50:
                lines.append(f"  ... and {len(self.warnings) - 50} more")
            lines.append("")

        # Machine-readable
        lines.append("### MACHINE-READABLE SECTION ###")
        for reg_name, app_name, pc, score in self.matched_details:
            lines.append(f"MATCHED|Register={reg_name}|App={app_name}|PostCode={pc}|Score={score:.2f}")
        for reg_name, pc, score, app_name in self.possible_details:
            lines.append(f"POSSIBLE|Register={reg_name}|App={app_name}|PostCode={pc}|Score={score:.2f}")
        for reg_name, pc, candidates in self.ambiguous_details:
            cands = ";".join(f"{n}:{s:.2f}" for n, s in candidates)
            lines.append(f"AMBIGUOUS|Register={reg_name}|PostCode={pc}|Candidates={cands}")
        for reg_name, pc in self.unmatched_details:
            lines.append(f"UNMATCHED|Register={reg_name}|PostCode={pc}")
        for field, count in sorted(self.field_updates.items()):
            lines.append(f"UPDATE|Field={field}|Count={count}")
        lines.append("### END MACHINE-READABLE SECTION ###")

        Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def validate_app_export(headers):
    """Check app-export has required target columns. Returns list of errors."""
    errors = []
    for col in REQUIRED_APP_TARGETS:
        if col not in headers:
            errors.append(f"App-export missing required column: '{col}'")
    return errors


def validate_register(headers):
    """Check register has name and postcode fields. Returns list of errors."""
    errors = []
    if not any(k in headers for k in REG_SURNAME_KEYS):
        errors.append(f"Register missing surname column (expected one of: {', '.join(REG_SURNAME_KEYS)})")
    if not any(k in headers for k in REG_FORENAME_KEYS):
        errors.append(f"Register missing forename column (expected one of: {', '.join(REG_FORENAME_KEYS)})")
    if not any(k in headers for k in REG_POSTCODE_KEYS):
        errors.append(f"Register missing postcode column (expected one of: {', '.join(REG_POSTCODE_KEYS)})")
    return errors


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_register_to_app(register_rows, app_rows, threshold, report):
    """Match register rows to app-export by fuzzy name+postcode.
    Returns dict: app_idx -> register_row."""

    # Build postcode index from app-export
    pc_index = defaultdict(list)
    for i, row in enumerate(app_rows):
        pc_raw = row.get(APP_POSTCODE, "").strip()
        pc, _ = normalize_postcode(pc_raw) if pc_raw else ("", None)
        surname = row.get(APP_SURNAME, "").strip()
        forename = row.get(APP_FORENAME, "").strip()
        addr_str = _get_app_address(row)
        if pc:
            pc_index[pc].append((i, surname, forename, addr_str))

    # All app rows for no-postcode fallback
    all_app = [
        (i, row.get(APP_SURNAME, "").strip(),
         row.get(APP_FORENAME, "").strip(),
         _get_app_address(row))
        for i, row in enumerate(app_rows)
    ]

    report.total_app = len(app_rows)
    report.total_register = len(register_rows)

    matched = {}  # app_idx -> register_row
    app_claimed = {}  # app_idx -> (reg_idx, reg_name)

    for reg_idx, reg_row in enumerate(register_rows):
        reg_surname = _get_field(reg_row, REG_SURNAME_KEYS)
        reg_forename = _get_field(reg_row, REG_FORENAME_KEYS)
        reg_name = f"{reg_forename} {reg_surname}".strip() or "(unknown)"
        reg_pc = _get_register_postcode(reg_row)

        # Determine candidates
        if reg_pc:
            candidates = pc_index.get(reg_pc, [])
            effective_threshold = threshold
            if not candidates:
                candidates = all_app
                effective_threshold = NO_POSTCODE_THRESHOLD
        else:
            candidates = all_app
            effective_threshold = NO_POSTCODE_THRESHOLD

        if not candidates:
            report.unmatched += 1
            report.unmatched_details.append((reg_name, reg_pc))
            continue

        # Score candidates
        scored = []
        for app_idx, app_surname, app_forename, app_addr in candidates:
            score = _surname_forename_similarity(
                reg_surname, reg_forename, app_surname, app_forename)
            scored.append((score, app_idx, app_surname, app_forename, app_addr))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_idx, best_surname, best_forename, best_addr = scored[0]
        best_name = f"{best_forename} {best_surname}".strip()

        # Ambiguity check with address tiebreaker
        if len(scored) > 1 and best_score < 1.0:
            second_score = scored[1][0]
            if (best_score >= effective_threshold
                    and (best_score - second_score) < AMBIGUITY_MARGIN):
                # Try address tiebreaker
                reg_addr = _get_register_address(reg_row)
                if reg_addr:
                    addr_scores = [
                        (s[0] + 0.001 * _address_similarity(reg_addr, s[4]), s)
                        for s in scored[:2]
                    ]
                    addr_scores.sort(key=lambda x: x[0], reverse=True)
                    if addr_scores[0][0] - addr_scores[1][0] >= AMBIGUITY_MARGIN:
                        best = addr_scores[0][1]
                        best_score, best_idx, best_surname, best_forename, best_addr = best
                        best_name = f"{best_forename} {best_surname}".strip()
                    else:
                        cands = [(f"{s[3]} {s[2]}".strip(), s[0]) for s in scored[:2]]
                        report.ambiguous += 1
                        report.ambiguous_details.append((reg_name, reg_pc, cands))
                        continue
                else:
                    cands = [(f"{s[3]} {s[2]}".strip(), s[0]) for s in scored[:2]]
                    report.ambiguous += 1
                    report.ambiguous_details.append((reg_name, reg_pc, cands))
                    continue

        if best_score >= effective_threshold:
            # Check for duplicate claims
            if best_idx in app_claimed:
                report.duplicate_matches += 1
                prev_reg_idx, prev_name = app_claimed[best_idx]
                report.warnings.append(
                    f"Register row '{reg_name}' also matches app row '{best_name}' "
                    f"(already claimed by '{prev_name}') — skipped")
                continue
            app_claimed[best_idx] = (reg_idx, reg_name)
            matched[best_idx] = reg_row
            report.matched += 1
            report.matched_details.append(
                (reg_name, best_name, reg_pc, best_score))
        elif best_score >= 0.6:
            report.possible += 1
            report.possible_details.append(
                (reg_name, reg_pc, best_score, best_name))
        else:
            report.unmatched += 1
            report.unmatched_details.append((reg_name, reg_pc))

    return matched


# ---------------------------------------------------------------------------
# Field updates
# ---------------------------------------------------------------------------

def apply_updates(app_rows, matched, report, data_date):
    """Apply field updates from matched register rows to app-export rows."""

    for app_idx, reg_row in matched.items():
        app_row = app_rows[app_idx]
        app_name = f"{app_row.get(APP_FORENAME, '')} {app_row.get(APP_SURNAME, '')}".strip()
        le2026_updated = False

        # --- DateOfAttainment ---
        doa = reg_row.get("DateOfAttainment", "").strip()
        if doa:
            normalized, warn = normalize_date(doa)
            if normalized:
                app_date = to_app_date(normalized)
                if app_date:
                    app_row["Date of Attainment"] = app_date
                    report.field_updates["Date of Attainment"] += 1
            elif warn:
                report.warnings.append(f"{app_name}: DateOfAttainment: {warn}")

        # --- GE24 Voted ---
        ge24 = reg_row.get("GE24", "").strip()
        if ge24 and ge24.upper() in ("YES", "Y"):
            app_row[GE2024_VOTED] = "Y"
            report.field_updates["GE2024 Voted"] += 1

        # --- PostalVoter -> LE2026 ---
        pv = _get_postal_voter(reg_row)
        if pv and pv.upper() not in ("N", "NO"):
            app_row[LE2026_POSTAL] = "Y"
            report.field_updates["LE2026 Postal Voter"] += 1
            le2026_updated = True

        # --- Party -> LE2026 Usual Party ---
        party_raw = reg_row.get("Party", "").strip()
        if party_raw:
            app_name_party, warn = reverse_map_party(party_raw)
            if app_name_party:
                app_row[LE2026_PARTY] = app_name_party
                report.field_updates["LE2026 Usual Party"] += 1
                le2026_updated = True
            elif warn:
                report.warnings.append(f"{app_name}: Party: {warn}")

        # --- 1-5 (GVI) -> LE2026 GVI ---
        gvi = reg_row.get("1-5", "").strip()
        if gvi:
            if gvi in ("1", "2", "3", "4", "5"):
                app_row[LE2026_GVI] = gvi
                report.field_updates["LE2026 GVI"] += 1
                le2026_updated = True
            else:
                report.warnings.append(
                    f"{app_name}: Invalid GVI '{gvi}' (must be 1-5), skipped")

        # --- P/PB -> Poster/Board tags ---
        ppb = reg_row.get("P/PB", "").strip()
        if ppb:
            parts = [p.strip() for p in ppb.split("/")]
            for part in parts:
                if part.upper() == "P":
                    app_row["Poster ticked"] = "TRUE"
                    report.field_updates["Poster ticked"] += 1
                elif part.upper() == "PB":
                    app_row["Board ticked"] = "TRUE"
                    report.field_updates["Board ticked"] += 1

        # --- DNK -> Do Not Knock ---
        dnk = reg_row.get("DNK", "").strip()
        if dnk and dnk.upper() not in ("N", "NO", "FALSE"):
            app_row["Do Not Knock ticked"] = "TRUE"
            report.field_updates["Do Not Knock ticked"] += 1

        # --- Comments -> Note 1 ---
        comment = reg_row.get("Comments", "").strip()
        if comment:
            row_key = app_name
            shift_notes(app_row, report, row_key)
            app_row[NOTE_TEXT_KEYS[0]] = comment
            app_row[NOTE_DATE_KEYS[0]] = data_date
            report.field_updates["Notes"] += 1

        # --- LE2026 Date (record when data was entered) ---
        # Only set if we actually wrote an LE2026 field
        if le2026_updated:
            app_row[LE2026_DATE] = data_date
            report.field_updates["LE2026 Date"] += 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update TTW app-export CSV with council register data.")
    parser.add_argument("app_export", help="App-export CSV (from TTW)")
    parser.add_argument("register", help="Council register CSV (update source)")
    parser.add_argument("output", help="Output updated CSV")
    parser.add_argument("--report", default=None,
                        help="QA report path (default: OUTPUT.report.txt)")
    parser.add_argument("--match-threshold", type=float, default=DEFAULT_THRESHOLD,
                        help=f"Fuzzy match threshold (default: {DEFAULT_THRESHOLD})")
    parser.add_argument("--date", default=None,
                        help="Override today's date for notes/LE2026 date "
                             "(format: YYYY-MMM-DD, e.g. 2026-Mar-31)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress stdout output")
    args = parser.parse_args()

    report_path = args.report or (args.output + ".report.txt")

    # Determine data date
    if args.date:
        data_date = args.date
    else:
        data_date = datetime.now().strftime("%Y-%b-%d")

    # Overwrite protection
    for input_path in [args.app_export, args.register]:
        if os.path.abspath(args.output) == os.path.abspath(input_path):
            print(f"ERROR: Output path '{args.output}' would overwrite input '{input_path}'.",
                  file=sys.stderr)
            sys.exit(1)

    # Read inputs
    app_rows, _, app_headers = read_input(args.app_export)
    register_rows, _, register_headers = read_input(args.register)

    # Validate
    errors = validate_app_export(app_headers)
    errors.extend(validate_register(register_headers))
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if not app_rows:
        print("ERROR: App-export CSV is empty.", file=sys.stderr)
        sys.exit(1)
    if not register_rows:
        print("ERROR: Register CSV is empty.", file=sys.stderr)
        sys.exit(1)

    # Match
    report = UpdateReport()
    report.app_file = args.app_export
    report.register_file = args.register
    report.output_file = args.output

    matched = match_register_to_app(
        register_rows, app_rows, args.match_threshold, report)

    # Apply updates
    apply_updates(app_rows, matched, report, data_date)

    # Write output (preserve original column order)
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=app_headers,
                                lineterminator="\r\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(app_rows)

    # Write report
    report.write(report_path)

    # Summary
    if not args.quiet:
        print(f"App-export update complete.")
        print(f"  App-export rows: {report.total_app}")
        print(f"  Register rows: {report.total_register}")
        print(f"  Matched: {report.matched}")
        if report.possible:
            print(f"  Possible: {report.possible}")
        if report.ambiguous:
            print(f"  Ambiguous: {report.ambiguous}")
        print(f"  Unmatched register rows: {report.unmatched}")
        if report.field_updates:
            print(f"  Field updates:")
            for field, count in sorted(report.field_updates.items()):
                print(f"    {field}: {count}")
        print(f"  Report: {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
