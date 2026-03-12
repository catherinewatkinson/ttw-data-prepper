#!/usr/bin/env python3
"""Post-enrichment validation for TTW electoral register CSVs.

Verifies that enrichment didn't corrupt base data and that enrichment
data was actually imported. Standalone script — no changes to existing files.

Usage:
    python3 tools/validate_enrichment.py OUTPUT.csv --base BASE.csv \\
        [--report REPORT.txt] [--elections GE2024 2026] \\
        [--min-match-rate 0.7] [--strict] [--quiet]

Exit code: 0 = passed, 1 = failed.
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ttw_common import read_input, VALID_PARTY_CODES

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROTECTED_COLUMNS = [
    "Elector No. Prefix", "Elector No.", "Elector No. Suffix",
    "Full Elector No.", "Surname", "Forename", "Middle Names",
    "Date of Attainment",
    "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
    "PostCode", "UPRN",
]

ELECTION_SUFFIXES = ["Party", "Voted", "Postal Voter", "Green Voting Intention"]

VALID_GVI = {"1", "2", "3", "4", "5", ""}

DEFAULT_MIN_MATCH_RATE = 0.7


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class Level(Enum):
    INFO = "INFO"
    WARN = "WARN"
    FAIL = "FAIL"
    PASS = "PASS"


@dataclass
class CheckResult:
    level: Level
    category: str
    message: str
    details: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_csvs(output_path, base_path=None):
    """Load output and optionally base CSV.

    Returns (output_rows, output_headers, base_rows, base_headers).
    """
    output_rows, _, output_headers = read_input(output_path)
    base_rows = None
    base_headers = None
    if base_path:
        base_rows, _, base_headers = read_input(base_path)
    return output_rows, output_headers, base_rows, base_headers


# ---------------------------------------------------------------------------
# Report parsing
# ---------------------------------------------------------------------------

def parse_enrichment_report(report_path):
    """Parse the machine-readable section of an enrichment QA report.

    Returns dict with confident_matches, unmatched, conflicts, overwrites,
    warnings.  Returns None if report cannot be parsed.
    """
    try:
        text = Path(report_path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None

    lines = []
    in_section = False
    for line in text.splitlines():
        if line.strip() == "### MACHINE-READABLE SECTION ###":
            in_section = True
            continue
        if line.strip() == "### END MACHINE-READABLE SECTION ###":
            break
        if in_section and line.strip():
            lines.append(line.strip())

    if not lines:
        return None

    result = {
        "confident_matches": [],
        "unmatched": [],
        "conflicts": [],
        "overwrites": [],
        "warnings": [],
        "summaries": {},
    }

    for line in lines:
        parts = line.split("|")
        if not parts:
            continue

        line_type = parts[0]
        fields = {}
        for part in parts[1:]:
            if "=" in part:
                key, _, value = part.partition("=")
                fields[key] = value

        if line_type == "MATCH":
            status = fields.get("Status", "")
            if status == "confident":
                try:
                    score = float(fields.get("Score", "0"))
                except ValueError:
                    score = 0.0
                result["confident_matches"].append({
                    "ERName": fields.get("ERName", ""),
                    "BaseName": fields.get("BaseName", ""),
                    "PostCode": fields.get("PostCode", ""),
                    "Score": score,
                })
            elif status == "unmatched":
                result["unmatched"].append({
                    "PostCode": fields.get("PostCode", ""),
                    "Name": fields.get("Name", ""),
                })
        elif line_type == "CONFLICT":
            result["conflicts"].append({
                "Row": fields.get("Row", ""),
                "Field": fields.get("Field", ""),
                "EnrichedRegister": fields.get("EnrichedRegister", ""),
                "Canvassing": fields.get("Canvassing", ""),
                "Resolved": fields.get("Resolved", ""),
            })
        elif line_type == "OVERWRITE":
            result["overwrites"].append({
                "Row": fields.get("Row", ""),
                "Field": fields.get("Field", ""),
                "Old": fields.get("Old", ""),
                "New": fields.get("New", ""),
            })
        elif line_type == "WARNING":
            result["warnings"].append("|".join(parts[1:]))
        elif line_type == "SUMMARY":
            source = fields.get("Source", "")
            if source:
                summary = {}
                for k, v in fields.items():
                    if k == "Source":
                        continue
                    try:
                        summary[k] = int(v)
                    except (ValueError, TypeError):
                        summary[k] = v
                result["summaries"][source] = summary

    return result


# ---------------------------------------------------------------------------
# Election discovery
# ---------------------------------------------------------------------------

def discover_election_names(output_headers, explicit_elections=None):
    """Discover election names from output headers.

    Scans for known suffixes (Party, Voted, Postal Voter, Green Voting
    Intention).  ``--elections`` flag overrides.
    """
    if explicit_elections:
        return list(explicit_elections)

    elections = set()
    for header in output_headers:
        for suffix in ELECTION_SUFFIXES:
            if header.endswith(f" {suffix}"):
                election = header[: -(len(suffix) + 1)]
                elections.add(election)

    return sorted(elections)


# ---------------------------------------------------------------------------
# FAIL checks
# ---------------------------------------------------------------------------

def check_row_count(base_rows, output_rows):
    """Check output rows == base rows."""
    results = []
    if len(output_rows) != len(base_rows):
        results.append(CheckResult(
            Level.FAIL, "row_count",
            f"Row count mismatch: output has {len(output_rows)}, "
            f"base has {len(base_rows)}",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "row_count",
            f"Output has {len(output_rows)} rows, matching base "
            f"({len(base_rows)})",
        ))
    return results


def check_base_headers_present(base_headers, output_headers):
    """Check all base column headers appear in output."""
    results = []
    output_set = set(output_headers)
    missing = [h for h in base_headers if h not in output_set]
    if missing:
        results.append(CheckResult(
            Level.FAIL, "base_headers_present",
            f"Missing {len(missing)} base column(s) in output: {missing}",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "base_headers_present",
            f"All {len(base_headers)} base columns present in output",
        ))
    return results


def check_row_order(base_rows, output_rows):
    """Guard check: Full Elector No. at row 0 matches between base and
    output."""
    results = []
    if not base_rows or not output_rows:
        results.append(CheckResult(
            Level.FAIL, "row_order",
            "Cannot verify row order: base or output is empty",
        ))
        return results

    base_val = base_rows[0].get("Full Elector No.", "")
    output_val = output_rows[0].get("Full Elector No.", "")

    if base_val != output_val:
        results.append(CheckResult(
            Level.FAIL, "row_order",
            f"Row 0 Full Elector No. mismatch: base='{base_val}', "
            f"output='{output_val}' "
            f"(possible swapped arguments or reordered output)",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "row_order",
            f"Row order verified (row 0: '{base_val}')",
        ))
    return results


def check_base_column_integrity(base_rows, output_rows, base_headers):
    """Every protected column identical row-by-row.

    Only runs if row_order has passed (caller should check).
    """
    results = []
    header_set = set(base_headers)
    protected = [c for c in PROTECTED_COLUMNS if c in header_set]

    mismatches = []
    for i in range(min(len(base_rows), len(output_rows))):
        for col in protected:
            base_val = base_rows[i].get(col, "")
            output_val = output_rows[i].get(col, "")
            if base_val != output_val:
                elector = output_rows[i].get("Full Elector No.",
                                             f"row {i}")
                mismatches.append(
                    f"{elector}: {col} base='{base_val}' "
                    f"output='{output_val}'"
                )

    if mismatches:
        results.append(CheckResult(
            Level.FAIL, "base_column_integrity",
            f"{len(mismatches)} protected column value(s) differ from base",
            details=mismatches[:20],
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "base_column_integrity",
            f"All protected columns identical across "
            f"{len(output_rows)} rows",
        ))
    return results


def check_duplicate_elector_numbers(output_rows):
    """No duplicate Full Elector No. values AND no blank values."""
    results = []
    seen = {}
    blanks = 0
    duplicates = []

    for i, row in enumerate(output_rows):
        val = row.get("Full Elector No.", "")
        if not val.strip():
            blanks += 1
            continue
        if val in seen:
            duplicates.append(val)
        else:
            seen[val] = i

    issues = []
    if blanks:
        issues.append(f"{blanks} blank Full Elector No. value(s)")
    if duplicates:
        issues.append(
            f"{len(duplicates)} duplicate Full Elector No. value(s): "
            f"{duplicates[:10]}"
        )

    if issues:
        results.append(CheckResult(
            Level.FAIL, "duplicate_elector_numbers",
            "; ".join(issues),
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "duplicate_elector_numbers",
            f"All {len(output_rows)} Full Elector No. values unique "
            f"and non-blank",
        ))
    return results


def check_file_format(output_path):
    """Output has UTF-8 BOM and CRLF line endings."""
    results = []
    issues = []

    raw = Path(output_path).read_bytes()

    # Check UTF-8 BOM
    if not raw.startswith(b"\xef\xbb\xbf"):
        issues.append("Missing UTF-8 BOM (first 3 bytes should be EF BB BF)")

    # Check CRLF — strip all \r\n pairs, then check for remaining \n
    if b"\n" in raw.replace(b"\r\n", b""):
        issues.append(
            "Line endings are not CRLF (required for TTW Digital upload)"
        )

    if issues:
        results.append(CheckResult(
            Level.FAIL, "file_format",
            "; ".join(issues),
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "file_format",
            "UTF-8 BOM and CRLF line endings present",
        ))
    return results


# ---------------------------------------------------------------------------
# Canvassing accounting checks
# ---------------------------------------------------------------------------

def check_canvassing_accounting(report_data, canvassing_rows=None,
                                 unmatched_rows=None, unmatched_path=None):
    """Verify all canvassing data is accounted for.

    Sub-checks:
    - Internal consistency: confident + possible + ambiguous + unmatched == total
    - CSV row count: len(canvassing_rows) == total
    - Unmatched row count: len(unmatched_rows) == possible + ambiguous + unmatched
    - Cross-check: canvassing_rows - unmatched_rows == confident
    """
    results = []
    summaries = report_data.get("summaries", {}) if report_data else {}
    cs = summaries.get("canvassing")
    if not cs:
        return results

    total = cs.get("Total", 0)
    confident = cs.get("Confident", 0)
    possible = cs.get("Possible", 0)
    ambiguous = cs.get("Ambiguous", 0)
    unmatched = cs.get("Unmatched", 0)

    # Sub-check 1: Internal consistency
    category_sum = confident + possible + ambiguous + unmatched
    if category_sum != total:
        results.append(CheckResult(
            Level.FAIL, "canvassing_accounting",
            f"Category sum mismatch: confident({confident}) + "
            f"possible({possible}) + ambiguous({ambiguous}) + "
            f"unmatched({unmatched}) = {category_sum}, but total = {total}",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "canvassing_accounting",
            f"Canvassing categories sum correctly: "
            f"{confident}+{possible}+{ambiguous}+{unmatched} = {total}",
        ))

    # Sub-check 2: CSV row count matches report total
    if canvassing_rows is not None:
        csv_count = len(canvassing_rows)
        if csv_count != total:
            results.append(CheckResult(
                Level.FAIL, "canvassing_csv_count",
                f"Canvassing CSV has {csv_count} rows but report "
                f"total is {total}",
            ))
        else:
            results.append(CheckResult(
                Level.PASS, "canvassing_csv_count",
                f"Canvassing CSV row count ({csv_count}) matches "
                f"report total ({total})",
            ))

    # Expected unmatched count (possible + ambiguous + unmatched)
    expected_unmatched = possible + ambiguous + unmatched

    # Sub-check 3: Unmatched row count
    if unmatched_rows is not None:
        unmatched_csv_count = len(unmatched_rows)
        if unmatched_csv_count != expected_unmatched:
            results.append(CheckResult(
                Level.FAIL, "unmatched_csv_count",
                f"Unmatched CSV has {unmatched_csv_count} rows but expected "
                f"possible({possible}) + ambiguous({ambiguous}) + "
                f"unmatched({unmatched}) = {expected_unmatched}",
            ))
        else:
            results.append(CheckResult(
                Level.PASS, "unmatched_csv_count",
                f"Unmatched CSV row count ({unmatched_csv_count}) matches "
                f"expected ({expected_unmatched})",
            ))
    elif expected_unmatched > 0:
        # No unmatched CSV but there should be unmatched rows
        results.append(CheckResult(
            Level.FAIL, "unmatched_csv_count",
            f"No unmatched CSV found but report expects "
            f"{expected_unmatched} non-confident rows "
            f"(possible={possible}, ambiguous={ambiguous}, "
            f"unmatched={unmatched})",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "unmatched_csv_count",
            "No unmatched CSV needed (0 non-confident rows)",
        ))

    # Sub-check 4: Cross-check canvassing - unmatched == confident
    if canvassing_rows is not None and unmatched_rows is not None:
        diff = len(canvassing_rows) - len(unmatched_rows)
        if diff != confident:
            results.append(CheckResult(
                Level.FAIL, "canvassing_cross_check",
                f"Canvassing rows ({len(canvassing_rows)}) - "
                f"unmatched rows ({len(unmatched_rows)}) = {diff}, "
                f"but report says {confident} confident matches",
            ))
        else:
            results.append(CheckResult(
                Level.PASS, "canvassing_cross_check",
                f"Cross-check: {len(canvassing_rows)} - "
                f"{len(unmatched_rows)} = {confident} confident matches",
            ))

    return results


UNMATCHED_HELPER_COLUMNS = [
    "Match Category", "Match Score", "Best Candidate Elector No.",
    "Best Candidate Name", "Best Candidate Address",
]

VALID_MATCH_CATEGORIES = {"unmatched", "possible", "ambiguous"}


def check_unmatched_csv_valid(unmatched_rows, unmatched_headers):
    """Validate unmatched CSV structure.

    Checks:
    - Helper columns present
    - Match Category values valid
    - Original canvassing columns present (profile_name at minimum)
    """
    results = []
    if unmatched_rows is None:
        results.append(CheckResult(
            Level.PASS, "unmatched_csv_valid",
            "No unmatched CSV to validate (0 unmatched rows expected)",
        ))
        return results

    header_set = set(unmatched_headers)

    # Check helper columns
    missing_helpers = [c for c in UNMATCHED_HELPER_COLUMNS
                       if c not in header_set]
    if missing_helpers:
        results.append(CheckResult(
            Level.WARN, "unmatched_csv_valid",
            f"Unmatched CSV missing helper column(s): {missing_helpers}",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "unmatched_csv_valid",
            "All helper columns present in unmatched CSV",
        ))

    # Check Match Category values
    if "Match Category" in header_set:
        invalid_cats = []
        for i, row in enumerate(unmatched_rows):
            cat = row.get("Match Category", "").strip().lower()
            if cat and cat not in VALID_MATCH_CATEGORIES:
                invalid_cats.append(
                    f"row {i}: '{row.get('Match Category', '')}'"
                )
        if invalid_cats:
            results.append(CheckResult(
                Level.WARN, "unmatched_match_categories",
                f"{len(invalid_cats)} invalid Match Category value(s)",
                details=invalid_cats[:20],
            ))
        else:
            results.append(CheckResult(
                Level.PASS, "unmatched_match_categories",
                "All Match Category values valid",
            ))

    # Check original canvassing columns present
    if "profile_name" not in header_set:
        results.append(CheckResult(
            Level.WARN, "unmatched_csv_valid",
            "Unmatched CSV missing 'profile_name' column "
            "(original canvassing data may be absent)",
        ))

    return results


# ---------------------------------------------------------------------------
# WARN checks
# ---------------------------------------------------------------------------

def check_match_rate(report_data, output_rows, election_names, min_rate):
    """Enrichment match rate >= threshold."""
    results = []
    rate = None
    source = "data"

    if report_data:
        confident = len(report_data.get("confident_matches", []))
        unmatched = len(report_data.get("unmatched", []))
        total = confident + unmatched
        if total > 0:
            rate = confident / total
            source = "report"

    if rate is None and election_names:
        # Estimate from data: rows with any non-empty election column
        enriched = 0
        for row in output_rows:
            has_data = False
            for election in election_names:
                for suffix in ELECTION_SUFFIXES:
                    col = f"{election} {suffix}"
                    if row.get(col, "").strip():
                        has_data = True
                        break
                if has_data:
                    break
            if has_data:
                enriched += 1
        if output_rows:
            rate = enriched / len(output_rows)
            source = "data estimate"

    if rate is not None:
        if rate < min_rate:
            results.append(CheckResult(
                Level.WARN, "match_rate",
                f"Match rate {rate:.1%} is below threshold "
                f"{min_rate:.0%} (source: {source})",
            ))
        else:
            results.append(CheckResult(
                Level.PASS, "match_rate",
                f"Match rate {rate:.1%} meets threshold "
                f"{min_rate:.0%} (source: {source})",
            ))

    return results


def check_matched_but_empty(report_data, output_rows, election_names):
    """Rows matched (per report) but with all election columns blank.

    Best-effort: joins by BaseName + PostCode from report.
    Skipped if no report data.
    """
    results = []
    if not report_data or not report_data.get("confident_matches"):
        return results

    # Build lookup from output: (name, postcode) -> row indices
    output_lookup = {}
    for i, row in enumerate(output_rows):
        name = (f"{row.get('Forename', '')} "
                f"{row.get('Surname', '')}").strip()
        pc = row.get("PostCode", "").strip()
        key = (name, pc)
        if key not in output_lookup:
            output_lookup[key] = []
        output_lookup[key].append(i)

    empty_matches = []
    for match in report_data["confident_matches"]:
        base_name = match["BaseName"]
        postcode = match["PostCode"]
        key = (base_name, postcode)

        indices = output_lookup.get(key, [])
        for idx in indices:
            row = output_rows[idx]
            has_data = False
            for election in election_names:
                for suffix in ELECTION_SUFFIXES:
                    col = f"{election} {suffix}"
                    if row.get(col, "").strip():
                        has_data = True
                        break
                if has_data:
                    break
            if not has_data:
                elector = row.get("Full Elector No.", f"row {idx}")
                empty_matches.append(
                    f"{elector}: matched as '{base_name}' but all "
                    f"election columns blank"
                )

    if empty_matches:
        results.append(CheckResult(
            Level.WARN, "matched_but_empty",
            f"{len(empty_matches)} row(s) matched in report but have "
            f"no election data",
            details=empty_matches[:20],
        ))

    return results


def check_voted_party_consistency(output_rows, election_names):
    """If Voted="Y" then Party should be non-empty (same election)."""
    results = []
    inconsistent = []

    for row in output_rows:
        for election in election_names:
            voted_col = f"{election} Voted"
            party_col = f"{election} Party"
            voted = row.get(voted_col, "").strip()
            party = row.get(party_col, "").strip()

            if voted == "Y" and not party:
                elector = row.get("Full Elector No.", "?")
                inconsistent.append(
                    f'{elector}: {voted_col}="Y", {party_col}=""'
                )

    if inconsistent:
        results.append(CheckResult(
            Level.WARN, "voted_party_consistency",
            f'{len(inconsistent)} row(s) have Voted="Y" but Party '
            f"is blank",
            details=inconsistent[:20],
        ))
    else:
        has_voted_col = any(
            f"{e} Voted" in (output_rows[0] if output_rows else {})
            for e in election_names
        )
        if has_voted_col:
            results.append(CheckResult(
                Level.PASS, "voted_party_consistency",
                "All voted rows have a Party value",
            ))
        elif election_names:
            results.append(CheckResult(
                Level.PASS, "voted_party_consistency",
                "No Voted columns present to check",
            ))

    return results


def check_enrichment_had_effect(output_rows, output_headers, election_names):
    """At least one cell in election columns has a non-empty value."""
    results = []

    output_header_set = set(output_headers)
    election_cols = []
    for election in election_names:
        for suffix in ELECTION_SUFFIXES:
            col = f"{election} {suffix}"
            if col in output_header_set:
                election_cols.append(col)

    if not election_cols:
        results.append(CheckResult(
            Level.WARN, "enrichment_had_effect",
            "No election columns found in output",
        ))
        return results

    has_data = False
    for row in output_rows:
        for col in election_cols:
            if row.get(col, "").strip():
                has_data = True
                break
        if has_data:
            break

    if not has_data:
        results.append(CheckResult(
            Level.WARN, "enrichment_had_effect",
            "No non-empty values in any election column — enrichment "
            "may not have worked",
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "enrichment_had_effect",
            "Election columns contain data",
        ))
    return results


def check_elector_no_consistency(output_rows):
    """Full Elector No. matches {Prefix}-{No.}-{Suffix} for every row."""
    results = []
    inconsistent = []

    for row in output_rows:
        prefix = row.get("Elector No. Prefix", "")
        number = row.get("Elector No.", "")
        suffix = row.get("Elector No. Suffix", "")
        full = row.get("Full Elector No.", "")
        expected = f"{prefix}-{number}-{suffix}"

        if full != expected:
            inconsistent.append(f"Full='{full}' expected='{expected}'")

    if inconsistent:
        results.append(CheckResult(
            Level.WARN, "elector_no_consistency",
            f"{len(inconsistent)} row(s) have inconsistent "
            f"Full Elector No.",
            details=inconsistent[:20],
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "elector_no_consistency",
            f"All {len(output_rows)} Full Elector No. values are "
            f"consistent",
        ))
    return results


def check_party_codes(output_rows, election_names):
    """All Party values are blank or in VALID_PARTY_CODES."""
    results = []
    invalid = []

    for row in output_rows:
        for election in election_names:
            col = f"{election} Party"
            val = row.get(col, "").strip()
            if val and val not in VALID_PARTY_CODES:
                elector = row.get("Full Elector No.", "?")
                invalid.append(f"{elector}: {col}='{val}'")

    if invalid:
        results.append(CheckResult(
            Level.WARN, "party_codes",
            f"{len(invalid)} invalid party code(s)",
            details=invalid[:20],
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "party_codes",
            "All party codes valid",
        ))
    return results


def check_gvi_range(output_rows, election_names):
    """All Green Voting Intention values are 1-5 or blank."""
    results = []
    invalid = []

    for row in output_rows:
        for election in election_names:
            col = f"{election} Green Voting Intention"
            val = row.get(col, "").strip()
            if val not in VALID_GVI:
                elector = row.get("Full Elector No.", "?")
                invalid.append(f"{elector}: {col}='{val}'")

    if invalid:
        results.append(CheckResult(
            Level.WARN, "gvi_range",
            f"{len(invalid)} invalid Green Voting Intention value(s)",
            details=invalid[:20],
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "gvi_range",
            "All Green Voting Intention values in range 1-5 or blank",
        ))
    return results


def check_voted_values(output_rows, election_names):
    """All Voted values must be 'Y' or blank (TTW spec)."""
    results = []
    invalid = []

    for row in output_rows:
        for election in election_names:
            col = f"{election} Voted"
            val = row.get(col, "").strip()
            if val and val != "Y":
                elector = row.get("Full Elector No.", "?")
                invalid.append(f"{elector}: {col}='{val}'")

    if invalid:
        results.append(CheckResult(
            Level.WARN, "voted_values",
            f"{len(invalid)} Voted value(s) not in TTW format (expected 'Y' or blank)",
            details=invalid[:20],
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "voted_values",
            "All Voted values are 'Y' or blank",
        ))
    return results


def check_postal_voter_values(output_rows, election_names):
    """All Postal Voter values must be 'Y' or blank (TTW spec)."""
    results = []
    invalid = []

    for row in output_rows:
        for election in election_names:
            col = f"{election} Postal Voter"
            val = row.get(col, "").strip()
            if val and val != "Y":
                elector = row.get("Full Elector No.", "?")
                invalid.append(f"{elector}: {col}='{val}'")

    if invalid:
        results.append(CheckResult(
            Level.WARN, "postal_voter_values",
            f"{len(invalid)} Postal Voter value(s) not in TTW format (expected 'Y' or blank)",
            details=invalid[:20],
        ))
    else:
        results.append(CheckResult(
            Level.PASS, "postal_voter_values",
            "All Postal Voter values are 'Y' or blank",
        ))
    return results


def check_identical_headers(base_headers, output_headers):
    """If base and output have identical column sets, warn."""
    results = []
    if base_headers == output_headers:
        results.append(CheckResult(
            Level.WARN, "identical_headers",
            "Base and output have identical columns — enrichment may "
            "not have been run (or arguments were swapped)",
        ))
    return results


# ---------------------------------------------------------------------------
# INFO / Statistics
# ---------------------------------------------------------------------------

def compute_statistics(base_rows, base_headers, output_rows,
                       output_headers, election_names):
    """Compute INFO statistics about the enrichment."""
    results = []

    new_cols = ([h for h in output_headers if h not in set(base_headers)]
                if base_headers else [])
    results.append(CheckResult(
        Level.INFO, "counts",
        f"Output: {len(output_rows)} rows, {len(output_headers)} columns"
        + (f" ({len(new_cols)} new columns added)" if new_cols else ""),
    ))

    # Coverage per election field
    output_header_set = set(output_headers)
    if election_names and output_rows:
        coverage_details = []
        for election in election_names:
            for suffix in ELECTION_SUFFIXES:
                col = f"{election} {suffix}"
                if col not in output_header_set:
                    continue
                non_empty = sum(
                    1 for r in output_rows if r.get(col, "").strip()
                )
                pct = non_empty / len(output_rows) * 100
                coverage_details.append(
                    f"{col}: {pct:.1f}% ({non_empty}/{len(output_rows)})"
                )

        if coverage_details:
            results.append(CheckResult(
                Level.INFO, "election_coverage",
                "Coverage per election field:",
                details=coverage_details,
            ))

    # Rows with/without enrichment data
    if election_names and output_rows:
        enriched_count = 0
        for row in output_rows:
            has_data = False
            for election in election_names:
                for suffix in ELECTION_SUFFIXES:
                    col = f"{election} {suffix}"
                    if row.get(col, "").strip():
                        has_data = True
                        break
                if has_data:
                    break
            if has_data:
                enriched_count += 1

        results.append(CheckResult(
            Level.INFO, "enrichment_rows",
            f"{enriched_count}/{len(output_rows)} rows have enrichment "
            f"data, {len(output_rows) - enriched_count} without",
        ))

    return results


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(results, output_path, base_path, report_path, strict,
                   canvassing_path=None, unmatched_path=None):
    """Format validation results into a human-readable report with
    machine-readable footer."""
    lines = []
    lines.append("=" * 60)
    lines.append("Electoral Register Enrichment Validation Report")
    lines.append("=" * 60)
    lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Output file: {output_path}")
    if base_path:
        lines.append(f"Base file: {base_path}")
    if report_path:
        lines.append(f"Enrichment report: {report_path}")
    if canvassing_path:
        lines.append(f"Canvassing export: {canvassing_path}")
    if unmatched_path:
        lines.append(f"Unmatched CSV: {unmatched_path}")
    lines.append(f"Mode: {'strict' if strict else 'normal'}")
    lines.append("")

    # Group by level
    fails = [r for r in results if r.level == Level.FAIL]
    warns = [r for r in results if r.level == Level.WARN]
    passes = [r for r in results if r.level == Level.PASS]
    infos = [r for r in results if r.level == Level.INFO]

    def _format_check(check):
        out = [f"[{check.level.value}] {check.category}: {check.message}"]
        for detail in check.details:
            out.append(f"        {detail}")
        return out

    lines.append("--- FAIL Checks ---")
    if fails:
        for check in fails:
            lines.extend(_format_check(check))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("--- WARN Checks ---")
    if warns:
        for check in warns:
            lines.extend(_format_check(check))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("--- PASS Checks ---")
    if passes:
        for check in passes:
            lines.extend(_format_check(check))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("--- INFO / Statistics ---")
    if infos:
        for check in infos:
            lines.extend(_format_check(check))
    else:
        lines.append("(none)")
    lines.append("")

    # Summary
    lines.append("=" * 60)
    lines.append(
        f"SUMMARY: PASSED={len(passes)}  WARNED={len(warns)}  "
        f"FAILED={len(fails)}"
    )

    if fails:
        lines.append("RESULT: FAILED")
    elif warns and strict:
        lines.append("RESULT: FAILED (strict mode)")
    elif warns:
        lines.append("RESULT: PASSED (with warnings)")
    else:
        lines.append("RESULT: PASSED")
    lines.append("=" * 60)
    lines.append("")

    # Machine-readable footer
    lines.append("### MACHINE-READABLE SECTION ###")
    for check in results:
        if check.level != Level.INFO:
            lines.append(
                f"CHECK|Level={check.level.value}"
                f"|Category={check.category}"
                f"|Message={check.message}"
            )
    lines.append("### END MACHINE-READABLE SECTION ###")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_validation(output_path, base_path=None, report_path=None,
                   elections=None, min_match_rate=DEFAULT_MIN_MATCH_RATE,
                   strict=False, quiet=False,
                   canvassing_path=None, unmatched_path=None):
    """Run all validation checks. Returns (exit_code, report_text)."""

    # Load data
    output_rows, output_headers, base_rows, base_headers = load_csvs(
        output_path, base_path
    )

    # Parse enrichment report
    report_data = None
    if report_path:
        report_data = parse_enrichment_report(report_path)

    # Load canvassing CSV if provided
    canvassing_rows = None
    if canvassing_path:
        canvassing_rows, _, _ = read_input(canvassing_path)

    # Load unmatched CSV
    unmatched_rows = None
    unmatched_headers = None
    resolved_unmatched_path = unmatched_path
    if unmatched_path is None:
        # Auto-derive from output path
        out_p = Path(output_path)
        candidate = out_p.parent / f"{out_p.stem}.unmatched.csv"
        if candidate.exists():
            resolved_unmatched_path = str(candidate)
    if resolved_unmatched_path and Path(resolved_unmatched_path).exists():
        unmatched_rows, _, unmatched_headers = read_input(
            resolved_unmatched_path
        )

    # Discover elections
    election_names = discover_election_names(output_headers, elections)

    results = []
    row_order_ok = True

    # FAIL checks
    if base_rows is not None:
        results.extend(check_row_count(base_rows, output_rows))
        results.extend(
            check_base_headers_present(base_headers, output_headers)
        )

        order_results = check_row_order(base_rows, output_rows)
        results.extend(order_results)
        row_order_ok = all(r.level != Level.FAIL for r in order_results)

        if row_order_ok:
            results.extend(check_base_column_integrity(
                base_rows, output_rows, base_headers
            ))

    results.extend(check_duplicate_elector_numbers(output_rows))
    results.extend(check_file_format(output_path))

    # Canvassing accounting (FAIL level)
    if report_data and report_data.get("summaries", {}).get("canvassing"):
        results.extend(check_canvassing_accounting(
            report_data, canvassing_rows, unmatched_rows,
            resolved_unmatched_path,
        ))

    # WARN checks
    results.extend(check_match_rate(
        report_data, output_rows, election_names, min_match_rate
    ))
    results.extend(check_matched_but_empty(
        report_data, output_rows, election_names
    ))
    results.extend(check_voted_party_consistency(
        output_rows, election_names
    ))

    results.extend(check_enrichment_had_effect(
        output_rows, output_headers, election_names
    ))

    if base_headers is not None:
        results.extend(check_identical_headers(base_headers, output_headers))

    results.extend(check_elector_no_consistency(output_rows))
    results.extend(check_party_codes(output_rows, election_names))
    results.extend(check_gvi_range(output_rows, election_names))
    results.extend(check_voted_values(output_rows, election_names))
    results.extend(check_postal_voter_values(output_rows, election_names))

    # Unmatched CSV structure (WARN level)
    if unmatched_rows is not None or resolved_unmatched_path:
        results.extend(check_unmatched_csv_valid(
            unmatched_rows, unmatched_headers or [],
        ))

    # INFO
    results.extend(compute_statistics(
        base_rows, base_headers, output_rows, output_headers, election_names
    ))

    # Format report
    report_text = format_report(
        results, output_path, base_path, report_path, strict,
        canvassing_path, resolved_unmatched_path,
    )

    # Determine exit code
    has_fails = any(r.level == Level.FAIL for r in results)
    has_warns = any(r.level == Level.WARN for r in results)

    if has_fails:
        exit_code = 1
    elif strict and has_warns:
        exit_code = 1
    else:
        exit_code = 0

    return exit_code, report_text


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate an enriched TTW electoral register CSV."
    )
    parser.add_argument("output", help="Enriched output CSV to validate")
    parser.add_argument("--base", required=True,
                        help="Base TTW CSV (for integrity comparison)")
    parser.add_argument("--report", default=None,
                        help="Enrichment QA report file")
    parser.add_argument("--elections", nargs="*", default=None,
                        help="Election names to validate "
                        "(default: auto-discover)")
    parser.add_argument("--min-match-rate", type=float,
                        default=DEFAULT_MIN_MATCH_RATE,
                        help="Minimum acceptable match rate "
                        f"(default: {DEFAULT_MIN_MATCH_RATE})")
    parser.add_argument("--canvassing-export", default=None,
                        help="Canvassing export CSV (DS3) for accounting "
                        "cross-checks")
    parser.add_argument("--unmatched", default=None,
                        help="Unmatched CSV path (auto-derived from output "
                        "if not given)")
    parser.add_argument("--strict", action="store_true",
                        help="Promote WARNs to FAILs in exit code")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress stdout output")
    args = parser.parse_args()

    # Validate --unmatched: if explicitly provided, must exist
    if args.unmatched and not Path(args.unmatched).exists():
        print(f"ERROR: --unmatched file not found: {args.unmatched}",
              file=sys.stderr)
        sys.exit(1)

    exit_code, report_text = run_validation(
        output_path=args.output,
        base_path=args.base,
        report_path=args.report,
        elections=args.elections,
        min_match_rate=args.min_match_rate,
        strict=args.strict,
        quiet=args.quiet,
        canvassing_path=args.canvassing_export,
        unmatched_path=args.unmatched,
    )

    if not args.quiet:
        print(report_text)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
