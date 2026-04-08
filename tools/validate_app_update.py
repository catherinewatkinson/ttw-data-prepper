#!/usr/bin/env python3
"""Validate that update_app_export.py only modified expected fields.

Compares the original app-export against the updated output and verifies that
only amendable fields were changed — all other fields must be identical.

Usage:
    python3 tools/validate_app_update.py ORIGINAL.csv UPDATED.csv
    python3 tools/validate_app_update.py ORIGINAL.csv UPDATED.csv --changed-only

Exit code: 0 = passed, 1 = failed.
"""

import argparse
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ttw_common import read_input

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LE2026_PREFIX = "Brent London Borough Council election (2026-May-07)"
GE2024_PREFIX = "Brent London Borough Council election (2024-Jul-04)"

# Fields that update_app_export.py is allowed to modify
AMENDABLE_FIELDS = frozenset([
    "Date of Attainment",
    f"{GE2024_PREFIX} Voted",
    f"{LE2026_PREFIX} Most Recent Data - Date",
    f"{LE2026_PREFIX} Most Recent Data - GVI",
    f"{LE2026_PREFIX} Most Recent Data - Usual Party",
    f"{LE2026_PREFIX} Most Recent Data - Postal Voter",
    f"{LE2026_PREFIX} Previous Data 1 - Date",
    f"{LE2026_PREFIX} Previous Data 1 - GVI",
    f"{LE2026_PREFIX} Previous Data 1 - Usual Party",
    f"{LE2026_PREFIX} Previous Data 2 - Date",
    f"{LE2026_PREFIX} Previous Data 2 - GVI",
    f"{LE2026_PREFIX} Previous Data 2 - Usual Party",
    f"{LE2026_PREFIX} Previous Data 3 - Date",
    f"{LE2026_PREFIX} Previous Data 3 - GVI",
    f"{LE2026_PREFIX} Previous Data 3 - Usual Party",
    f"{LE2026_PREFIX} Previous Data 4 - Date",
    f"{LE2026_PREFIX} Previous Data 4 - GVI",
    f"{LE2026_PREFIX} Previous Data 4 - Usual Party",
    "Poster ticked",
    "Board ticked",
    "Do Not Knock ticked",
    "Date of Note 1 (most recent)",
    "Text of Note 1",
    "Date of Note 2", "Text of Note 2",
    "Date of Note 3", "Text of Note 3",
    "Date of Note 4", "Text of Note 4",
    "Date of Note 5", "Text of Note 5",
    "Date of Note 6", "Text of Note 6",
    "Date of Note 7", "Text of Note 7",
    "Date of Note 8", "Text of Note 8",
    "Date of Note 9", "Text of Note 9",
    "Date of Note 10", "Text of Note 10",
])

# Row identity column (never modified, used to match rows)
ROW_KEY = "Voter Number"


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
# Checks
# ---------------------------------------------------------------------------

def check_columns_match(orig_headers, updated_headers):
    """Verify output has same columns as input."""
    if orig_headers == updated_headers:
        return CheckResult(Level.PASS, "Columns",
                           f"All {len(orig_headers)} columns preserved in order")

    missing = [h for h in orig_headers if h not in updated_headers]
    extra = [h for h in updated_headers if h not in orig_headers]
    details = []
    if missing:
        details.append(f"Missing: {', '.join(missing[:10])}")
    if extra:
        details.append(f"Extra: {', '.join(extra[:10])}")
    if not missing and not extra:
        details.append("Columns present but in different order")

    return CheckResult(Level.FAIL, "Columns",
                       "Column mismatch between original and updated", details)


def check_row_count(orig_rows, updated_rows, changed_only):
    """Verify row counts make sense."""
    if changed_only:
        if len(updated_rows) > len(orig_rows):
            return CheckResult(Level.FAIL, "Row count",
                               f"Updated has more rows ({len(updated_rows)}) "
                               f"than original ({len(orig_rows)})")
        return CheckResult(Level.PASS, "Row count",
                           f"Updated: {len(updated_rows)} rows "
                           f"(--changed-only from {len(orig_rows)} original)")
    else:
        if len(orig_rows) != len(updated_rows):
            return CheckResult(Level.FAIL, "Row count",
                               f"Row count mismatch: original={len(orig_rows)}, "
                               f"updated={len(updated_rows)}")
        return CheckResult(Level.PASS, "Row count",
                           f"{len(orig_rows)} rows in both files")


def check_all_updated_rows_traceable(orig_rows, updated_rows):
    """Verify every updated row can be traced to an original row by Voter Number."""
    orig_keys = {row.get(ROW_KEY, "").strip() for row in orig_rows}
    untraced = []
    for i, row in enumerate(updated_rows):
        key = row.get(ROW_KEY, "").strip()
        if key not in orig_keys:
            untraced.append(f"Row {i+1}: Voter Number '{key}' not in original")

    if untraced:
        return CheckResult(Level.FAIL, "Traceability",
                           f"{len(untraced)} updated row(s) not found in original",
                           untraced[:20])
    return CheckResult(Level.PASS, "Traceability",
                       "All updated rows trace back to original")


def check_protected_fields(orig_rows, updated_rows, orig_headers, changed_only):
    """Verify non-amendable fields are identical between original and updated.
    Rows with duplicate Voter Numbers are skipped (can't reliably match)."""
    # Find duplicate Voter Numbers (can't reliably compare these)
    key_counts = Counter(row.get(ROW_KEY, "").strip() for row in orig_rows)
    duplicate_keys = {k for k, c in key_counts.items() if c > 1}

    # Build lookup by Voter Number for unique rows only
    orig_by_key = {}
    for row in orig_rows:
        key = row.get(ROW_KEY, "").strip()
        if key and key not in duplicate_keys:
            orig_by_key[key] = row

    protected_fields = [h for h in orig_headers if h not in AMENDABLE_FIELDS]
    violations = []
    skipped_dupes = 0

    for i, updated_row in enumerate(updated_rows):
        key = updated_row.get(ROW_KEY, "").strip()
        if key in duplicate_keys:
            skipped_dupes += 1
            continue
        orig_row = orig_by_key.get(key)
        if not orig_row:
            continue  # Already caught by traceability check

        for field_name in protected_fields:
            orig_val = orig_row.get(field_name, "")
            updated_val = updated_row.get(field_name, "")
            if orig_val != updated_val:
                violations.append(
                    f"Voter {key}: '{field_name}' changed "
                    f"'{orig_val[:40]}' -> '{updated_val[:40]}'")

    details = violations[:20]
    if skipped_dupes:
        details.append(f"({skipped_dupes} rows with duplicate Voter Numbers skipped — see Duplicate voters check)")

    if violations:
        return CheckResult(Level.FAIL, "Protected fields",
                           f"{len(violations)} protected field(s) were modified",
                           details)

    msg = (f"All non-amendable fields identical "
           f"({len(protected_fields)} fields checked per row)")
    if skipped_dupes:
        msg += f" — {skipped_dupes} duplicate-key rows skipped"
    return CheckResult(Level.PASS, "Protected fields", msg,
                       [f"({skipped_dupes} rows with duplicate Voter Numbers skipped)"] if skipped_dupes else [])


def check_amendable_field_values(updated_rows):
    """Verify amendable fields contain valid TTW values."""
    issues = []

    valid_gvi = {"1", "2", "3", "4", "5", "<NO RECORD>", "<NO DATA RECORDED>", ""}
    valid_party = {
        "Greens", "Conservatives", "Labour", "Liberal Democrats",
        "Reform/UKIP/Brexit", "Plaid Cymru", "Independent",
        "Residents Association", "Others",
        "<NO RECORD>", "<NO DATA RECORDED>", "",
    }
    valid_voted = {"Y", "<NO RECORD>", "<NO DATA RECORDED>", ""}
    valid_postal = {"Y", "<NO RECORD>", "<NO DATA RECORDED>", ""}
    valid_tag = {"TRUE", "FALSE", "true", "false", ""}

    for i, row in enumerate(updated_rows):
        key = row.get(ROW_KEY, "").strip() or f"row {i+1}"

        # GVI
        for gvi_col in [f"{LE2026_PREFIX} Most Recent Data - GVI"] + \
                        [f"{LE2026_PREFIX} Previous Data {n} - GVI" for n in range(1, 5)]:
            val = row.get(gvi_col, "")
            if val not in valid_gvi:
                issues.append(f"{key}: {gvi_col} = '{val}' (invalid)")

        # Party
        for party_col in [f"{LE2026_PREFIX} Most Recent Data - Usual Party"] + \
                          [f"{LE2026_PREFIX} Previous Data {n} - Usual Party" for n in range(1, 5)]:
            val = row.get(party_col, "")
            if val not in valid_party:
                issues.append(f"{key}: {party_col} = '{val}' (invalid)")

        # Voted
        val = row.get(f"{GE2024_PREFIX} Voted", "")
        if val not in valid_voted:
            issues.append(f"{key}: GE2024 Voted = '{val}' (invalid)")

        # Postal Voter
        val = row.get(f"{LE2026_PREFIX} Most Recent Data - Postal Voter", "")
        if val not in valid_postal:
            issues.append(f"{key}: LE2026 Postal Voter = '{val}' (invalid)")

        # Tags
        for tag in ["Poster ticked", "Board ticked", "Do Not Knock ticked"]:
            val = row.get(tag, "")
            if val not in valid_tag:
                issues.append(f"{key}: {tag} = '{val}' (invalid)")

    if issues:
        return CheckResult(Level.WARN, "Field values",
                           f"{len(issues)} field(s) with non-standard TTW values",
                           issues[:20])
    return CheckResult(Level.PASS, "Field values",
                       "All amendable fields contain valid TTW values")


def check_file_format(path):
    """Verify UTF-8 BOM."""
    try:
        with open(path, "rb") as f:
            bom = f.read(3)
        if bom != b"\xef\xbb\xbf":
            return CheckResult(Level.FAIL, "File format",
                               "Output CSV missing UTF-8 BOM")
    except OSError as e:
        return CheckResult(Level.FAIL, "File format", f"Cannot read file: {e}")
    return CheckResult(Level.PASS, "File format", "UTF-8 BOM present")


def check_duplicate_voter_numbers(updated_rows):
    """Verify no duplicate Voter Numbers in output."""
    counts = Counter(row.get(ROW_KEY, "").strip() for row in updated_rows)
    dupes = [(k, c) for k, c in counts.items() if c > 1 and k]
    if dupes:
        details = [f"'{k}' appears {c} times" for k, c in dupes[:20]]
        return CheckResult(Level.WARN, "Duplicate voters",
                           f"{len(dupes)} duplicate Voter Number(s) in data "
                           f"(protected field check skipped for these rows)",
                           details)
    return CheckResult(Level.PASS, "Duplicate voters",
                       "All Voter Numbers unique")


def compute_statistics(orig_rows, updated_rows, orig_headers, changed_only):
    """Summary statistics."""
    # Count how many rows actually changed
    orig_by_key = {}
    for row in orig_rows:
        key = row.get(ROW_KEY, "").strip()
        if key:
            orig_by_key[key] = row

    changed_count = 0
    amended_field_counts = Counter()

    for updated_row in updated_rows:
        key = updated_row.get(ROW_KEY, "").strip()
        orig_row = orig_by_key.get(key)
        if not orig_row:
            continue
        row_changed = False
        for field_name in AMENDABLE_FIELDS:
            if field_name in orig_row or field_name in updated_row:
                if orig_row.get(field_name, "") != updated_row.get(field_name, ""):
                    amended_field_counts[field_name] += 1
                    row_changed = True
        if row_changed:
            changed_count += 1

    lines = [
        f"Original rows: {len(orig_rows)}",
        f"Updated rows: {len(updated_rows)}",
        f"Rows with changes: {changed_count}",
    ]
    if amended_field_counts:
        lines.append("Changed fields:")
        for field_name, count in amended_field_counts.most_common(20):
            # Shorten long field names for readability
            short = field_name.replace(
                "Brent London Borough Council election ", "")
            lines.append(f"  {short}: {count}")

    return [CheckResult(Level.INFO, "Statistics", "Summary", lines)]


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(results, orig_path, updated_path, changed_only):
    lines = []
    lines.append("=" * 60)
    lines.append("App-Export Update Validation Report")
    lines.append("=" * 60)
    lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Original: {orig_path}")
    lines.append(f"Updated: {updated_path}")
    lines.append(f"Mode: {'changed-only' if changed_only else 'full'}")
    lines.append("")

    fails = [r for r in results if r.level == Level.FAIL]
    warns = [r for r in results if r.level == Level.WARN]
    passes = [r for r in results if r.level == Level.PASS]
    infos = [r for r in results if r.level == Level.INFO]

    def _fmt(check):
        out = [f"[{check.level.value}] {check.category}: {check.message}"]
        for d in check.details:
            out.append(f"        {d}")
        return out

    lines.append("--- FAIL Checks ---")
    if fails:
        for c in fails:
            lines.extend(_fmt(c))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("--- WARN Checks ---")
    if warns:
        for c in warns:
            lines.extend(_fmt(c))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("--- PASS Checks ---")
    if passes:
        for c in passes:
            lines.extend(_fmt(c))
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("--- INFO / Statistics ---")
    if infos:
        for c in infos:
            lines.extend(_fmt(c))
    lines.append("")

    has_fails = bool(fails)
    if has_fails:
        lines.append("VERDICT: FAIL")
    elif warns:
        lines.append("VERDICT: PASS (with warnings)")
    else:
        lines.append("VERDICT: PASS")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_validation(orig_path, updated_path, changed_only=False):
    orig_rows, _, orig_headers = read_input(orig_path)
    updated_rows, _, updated_headers = read_input(updated_path)

    results = []

    # FAIL checks
    results.append(check_columns_match(orig_headers, updated_headers))
    results.append(check_row_count(orig_rows, updated_rows, changed_only))
    results.append(check_all_updated_rows_traceable(orig_rows, updated_rows))
    results.append(check_protected_fields(
        orig_rows, updated_rows, orig_headers, changed_only))
    results.append(check_file_format(updated_path))
    results.append(check_duplicate_voter_numbers(updated_rows))

    # WARN checks
    results.append(check_amendable_field_values(updated_rows))

    # INFO
    results.extend(compute_statistics(
        orig_rows, updated_rows, orig_headers, changed_only))

    report_text = format_report(results, orig_path, updated_path, changed_only)

    has_fails = any(r.level == Level.FAIL for r in results)
    return 1 if has_fails else 0, report_text


def main():
    parser = argparse.ArgumentParser(
        description="Validate app-export update — check only expected fields changed.")
    parser.add_argument("original", help="Original app-export CSV (before update)")
    parser.add_argument("updated", help="Updated output CSV (after update)")
    parser.add_argument("--changed-only", action="store_true",
                        help="Updated file contains only changed rows")
    parser.add_argument("--quiet", action="store_true",
                        help="Only show WARN/FAIL")
    args = parser.parse_args()

    exit_code, report_text = run_validation(
        args.original, args.updated, changed_only=args.changed_only)

    if not args.quiet or exit_code != 0:
        print(report_text)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
