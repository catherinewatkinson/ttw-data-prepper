#!/usr/bin/env python3
"""Post-check validation for membership vs register cross-check output.

Verifies that the membership cross-check produced correct, uncorrupted output
using genuinely independent checks (no matching functions imported).

Usage:
    python3 tools/validate_membership_check.py MEMBERSHIP.csv REGISTER.csv OUTPUT.csv \\
        [--report REPORT.txt] [--strict] [--quiet]

Exit code: 0 = passed, 1 = failed.
"""

import argparse
import csv
import re
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

VALID_MATCH_STATUSES = {"unmatched", "possible", "ambiguous", "out_of_area", "no_postcode"}

# Machine-readable line types that correspond 1:1 to output rows
# NO_POSTCODE is excluded — it overlaps with matched/unmatched/possible/ambiguous
OUTPUT_LINE_TYPES = {"UNMATCHED", "POSSIBLE", "AMBIGUOUS", "OUT_OF_AREA"}


# ---------------------------------------------------------------------------
# Data structures (same pattern as validate_enrichment.py)
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
# Report parsing
# ---------------------------------------------------------------------------

def parse_membership_report(report_path):
    """Parse the membership check QA report.

    Returns dict with machine_lines (list of parsed dicts), summary (dict of
    counts), and warnings (list). Returns None if report cannot be parsed.
    """
    try:
        text = Path(report_path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None

    # Parse machine-readable section
    machine_lines = []
    in_section = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "### MACHINE-READABLE SECTION ###":
            in_section = True
            continue
        if stripped == "### END MACHINE-READABLE SECTION ###":
            break
        if in_section and stripped:
            parts = stripped.split("|")
            if not parts:
                continue
            line_type = parts[0]
            fields = {}
            for part in parts[1:]:
                if "=" in part:
                    key, _, value = part.partition("=")
                    fields[key] = value
            machine_lines.append({"type": line_type, "fields": fields})

    if not machine_lines and "### MACHINE-READABLE SECTION ###" not in text:
        return None

    # Parse summary section
    summary = {}
    for line in text.splitlines():
        stripped = line.strip()
        for key in ["Total members", "Total register entries",
                     "Matched (in register)", "Possible matches",
                     "Ambiguous", "Unmatched (not in register)",
                     "Out of area", "No postcode"]:
            if stripped.startswith(f"{key}:"):
                try:
                    summary[key] = int(stripped.split(":")[-1].strip())
                except ValueError:
                    pass

    # Count warnings (lines containing "Skipped row")
    skipped_count = 0
    for line in text.splitlines():
        if "Skipped row" in line:
            skipped_count += 1

    return {
        "machine_lines": machine_lines,
        "summary": summary,
        "skipped_count": skipped_count,
        "raw_text": text,
    }


# ---------------------------------------------------------------------------
# FAIL checks
# ---------------------------------------------------------------------------

def check_output_structure(output_headers, membership_headers):
    """Verify output has all membership columns + Match_Status + Best_Candidate."""
    missing = []
    for col in membership_headers:
        if col not in output_headers:
            missing.append(col)
    for col in ["Match_Status", "Best_Candidate"]:
        if col not in output_headers:
            missing.append(col)

    if missing:
        return CheckResult(
            Level.FAIL, "Output structure",
            f"Output CSV missing {len(missing)} expected column(s)",
            [f"Missing: {col}" for col in missing[:20]])
    return CheckResult(Level.PASS, "Output structure",
                       "All expected columns present")


def check_match_status_values(output_rows):
    """Verify all Match_Status values are valid (no 'matched' or blank in output)."""
    invalid = []
    for i, row in enumerate(output_rows):
        status = row.get("Match_Status", "")
        if status not in VALID_MATCH_STATUSES:
            invalid.append(f"Row {i+1}: Match_Status='{status}'")

    if invalid:
        return CheckResult(
            Level.FAIL, "Match status values",
            f"{len(invalid)} row(s) with invalid Match_Status",
            invalid[:20])
    return CheckResult(Level.PASS, "Match status values",
                       "All Match_Status values valid")


def check_matched_not_in_output(report_data, output_rows):
    """Verify no MATCHED member appears in the output CSV."""
    if not report_data:
        return CheckResult(Level.WARN, "Matched-not-in-output",
                           "Cannot check — report not parseable")

    # Build set of (name, postcode) from MATCHED report lines
    matched_keys = set()
    for entry in report_data["machine_lines"]:
        if entry["type"] == "MATCHED":
            name = entry["fields"].get("Member", "").strip()
            pc = entry["fields"].get("PostCode", "").strip()
            matched_keys.add((name.lower(), pc.lower()))

    if not matched_keys:
        return CheckResult(Level.PASS, "Matched-not-in-output",
                           "No matched entries to check (all members in output)")

    # Check output rows
    found_in_output = []
    for i, row in enumerate(output_rows):
        first = row.get("first_name", "").strip()
        last = row.get("last_name", "").strip()
        name = f"{first} {last}".strip().lower()
        pc = row.get("zip_code", "").strip().lower()
        if (name, pc) in matched_keys:
            found_in_output.append(f"Row {i+1}: {first} {last} ({pc})")

    if found_in_output:
        return CheckResult(
            Level.FAIL, "Matched-not-in-output",
            f"{len(found_in_output)} matched member(s) found in output CSV",
            found_in_output[:20])
    return CheckResult(Level.PASS, "Matched-not-in-output",
                       f"None of {len(matched_keys)} matched members appear in output")


def check_output_traces_to_input(output_rows, membership_rows, membership_headers):
    """Verify every output row traces back to an input membership row."""
    # Build fingerprints of membership rows (all fields except Match_Status/Best_Candidate)
    mem_fingerprints = set()
    for row in membership_rows:
        fp = tuple(row.get(h, "") for h in membership_headers)
        mem_fingerprints.add(fp)

    untraced = []
    for i, row in enumerate(output_rows):
        fp = tuple(row.get(h, "") for h in membership_headers)
        if fp not in mem_fingerprints:
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
            untraced.append(f"Row {i+1}: {name} — not found in membership input")

    if untraced:
        return CheckResult(
            Level.FAIL, "Output traceability",
            f"{len(untraced)} output row(s) cannot be traced to membership input",
            untraced[:20])
    return CheckResult(Level.PASS, "Output traceability",
                       "All output rows trace back to membership input")


def check_accounting(report_data, output_rows, membership_rows, strict=False):
    """Verify core invariant: output_rows + matched == total_members - skipped."""
    if not report_data:
        return CheckResult(Level.WARN, "Accounting",
                           "Cannot check — report not parseable")

    summary = report_data["summary"]
    matched = summary.get("Matched (in register)", 0)
    skipped = report_data["skipped_count"]
    total_members = len(membership_rows)
    output_count = len(output_rows)

    # In strict mode, possible matches are excluded from output
    possible_excluded = 0
    if strict:
        for entry in report_data["machine_lines"]:
            if entry["type"] == "POSSIBLE":
                possible_excluded += 1

    expected_output = total_members - matched - skipped - possible_excluded
    actual_output = output_count

    issues = []
    if actual_output != expected_output:
        issues.append(
            f"Output rows ({actual_output}) + matched ({matched}) + skipped ({skipped})"
            f"{f' + possible_excluded ({possible_excluded})' if strict else ''}"
            f" != total members ({total_members})")

    # Also verify report's total_members matches actual membership CSV
    report_total = summary.get("Total members", -1)
    if report_total != total_members:
        issues.append(
            f"Report says {report_total} total members, "
            f"but membership CSV has {total_members} rows")

    if issues:
        return CheckResult(Level.FAIL, "Accounting",
                           "Accounting mismatch detected", issues)
    return CheckResult(Level.PASS, "Accounting",
                       f"Accounting correct: {output_count} output + {matched} matched"
                       f" + {skipped} skipped = {total_members} total")


def check_status_count_reconciliation(report_data, output_rows):
    """Verify output row counts by status match machine-readable line counts."""
    if not report_data:
        return CheckResult(Level.WARN, "Status reconciliation",
                           "Cannot check — report not parseable")

    # Count output rows by status
    output_counts = Counter(row.get("Match_Status", "") for row in output_rows)

    # Count machine-readable lines by type (excluding NO_POSTCODE which overlaps)
    report_counts = Counter()
    for entry in report_data["machine_lines"]:
        if entry["type"] in OUTPUT_LINE_TYPES:
            report_counts[entry["type"]] += 1

    # Map between output status names and report line types
    status_to_type = {
        "unmatched": "UNMATCHED",
        "possible": "POSSIBLE",
        "ambiguous": "AMBIGUOUS",
        "out_of_area": "OUT_OF_AREA",
    }

    mismatches = []
    for status, line_type in status_to_type.items():
        csv_count = output_counts.get(status, 0)
        report_count = report_counts.get(line_type, 0)
        if csv_count != report_count:
            mismatches.append(
                f"{status}: {csv_count} in CSV vs {report_count} in report")

    if mismatches:
        return CheckResult(
            Level.FAIL, "Status reconciliation",
            f"{len(mismatches)} status count mismatch(es) between CSV and report",
            mismatches)
    return CheckResult(Level.PASS, "Status reconciliation",
                       "Output status counts match report")


def check_field_preservation(output_rows, membership_rows, membership_headers):
    """Verify membership field values are unchanged in output rows."""
    # Build lookup: fingerprint -> membership row
    mem_by_fp = {}
    for row in membership_rows:
        fp = tuple(row.get(h, "") for h in membership_headers)
        mem_by_fp[fp] = row

    corrupted = []
    for i, out_row in enumerate(output_rows):
        fp = tuple(out_row.get(h, "") for h in membership_headers)
        if fp in mem_by_fp:
            continue  # Perfect match — fields preserved
        # Try to find by name+email (looser match for error reporting)
        name = f"{out_row.get('first_name', '')} {out_row.get('last_name', '')}".strip()
        corrupted.append(f"Row {i+1}: {name} — membership fields altered")

    if corrupted:
        return CheckResult(
            Level.FAIL, "Field preservation",
            f"{len(corrupted)} output row(s) have altered membership fields",
            corrupted[:20])
    return CheckResult(Level.PASS, "Field preservation",
                       "All membership fields preserved in output")


def check_file_format(output_path):
    """Verify output CSV is UTF-8 BOM encoded."""
    try:
        with open(output_path, "rb") as f:
            bom = f.read(3)
        if bom != b"\xef\xbb\xbf":
            return CheckResult(Level.FAIL, "File format",
                               "Output CSV missing UTF-8 BOM")
    except OSError as e:
        return CheckResult(Level.FAIL, "File format",
                           f"Cannot read output file: {e}")
    return CheckResult(Level.PASS, "File format", "UTF-8 BOM present")


def check_register_row_count(report_data, register_rows):
    """Verify report's register count matches actual register CSV."""
    if not report_data:
        return CheckResult(Level.WARN, "Register row count",
                           "Cannot check — report not parseable")

    report_count = report_data["summary"].get("Total register entries", -1)
    actual_count = len(register_rows)

    if report_count != actual_count:
        return CheckResult(
            Level.FAIL, "Register row count",
            f"Report says {report_count} register entries, "
            f"but register CSV has {actual_count} rows")
    return CheckResult(Level.PASS, "Register row count",
                       f"Register row count matches: {actual_count}")


# ---------------------------------------------------------------------------
# WARN checks
# ---------------------------------------------------------------------------

def check_match_rate(report_data, membership_rows):
    """Warn if match rate is unusually low."""
    if not report_data:
        return CheckResult(Level.WARN, "Match rate",
                           "Cannot check — report not parseable")

    matched = report_data["summary"].get("Matched (in register)", 0)
    total = len(membership_rows)
    if total == 0:
        return CheckResult(Level.PASS, "Match rate", "No members to check")

    rate = matched / total
    if rate < 0.5:
        return CheckResult(
            Level.WARN, "Match rate",
            f"Only {rate:.0%} of members matched — "
            f"check correct register file was used",
            [f"{matched}/{total} matched"])
    return CheckResult(Level.PASS, "Match rate",
                       f"{rate:.0%} match rate ({matched}/{total})")


def check_out_of_area_rate(report_data, membership_rows):
    """Warn if many members are out of area."""
    if not report_data:
        return CheckResult(Level.WARN, "Out-of-area rate",
                           "Cannot check — report not parseable")

    oa = report_data["summary"].get("Out of area", 0)
    total = len(membership_rows)
    if total == 0:
        return CheckResult(Level.PASS, "Out-of-area rate", "No members")

    rate = oa / total
    if rate > 0.5:
        return CheckResult(
            Level.WARN, "Out-of-area rate",
            f"{rate:.0%} of members are out of area — "
            f"check postcode formats match between files",
            [f"{oa}/{total} out of area"])
    return CheckResult(Level.PASS, "Out-of-area rate",
                       f"{rate:.0%} out of area ({oa}/{total})")


def check_no_postcode_rate(report_data, membership_rows):
    """Warn if many members have no postcode."""
    if not report_data:
        return CheckResult(Level.WARN, "No-postcode rate",
                           "Cannot check — report not parseable")

    np = report_data["summary"].get("No postcode", 0)
    total = len(membership_rows)
    if total == 0:
        return CheckResult(Level.PASS, "No-postcode rate", "No members")

    rate = np / total
    if rate > 0.3:
        return CheckResult(
            Level.WARN, "No-postcode rate",
            f"{rate:.0%} of members have no postcode — "
            f"membership data quality issue",
            [f"{np}/{total} without postcode"])
    return CheckResult(Level.PASS, "No-postcode rate",
                       f"{rate:.0%} without postcode ({np}/{total})")


def check_matched_name_sanity(report_data):
    """Crude independent check: matched surnames should be plausibly similar.

    For short surnames (<=3 chars): require exact case-insensitive match.
    For longer surnames: require a common substring of length 3+.
    This does NOT use any fuzzy matching functions — just string operations.
    """
    if not report_data:
        return CheckResult(Level.WARN, "Name sanity",
                           "Cannot check — report not parseable")

    suspicious = []
    for entry in report_data["machine_lines"]:
        if entry["type"] != "MATCHED":
            continue
        member_name = entry["fields"].get("Member", "")
        register_name = entry["fields"].get("Register", "")

        # Extract last word as surname
        member_surname = member_name.split()[-1] if member_name.split() else ""
        register_surname = register_name.split()[-1] if register_name.split() else ""

        if not member_surname or not register_surname:
            continue

        m_lower = member_surname.lower()
        r_lower = register_surname.lower()

        if len(m_lower) <= 3 or len(r_lower) <= 3:
            # Short surname: require exact match
            if m_lower != r_lower:
                suspicious.append(
                    f"'{member_name}' matched to '{register_name}' "
                    f"— short surnames differ: '{member_surname}' vs '{register_surname}'")
        else:
            # Longer surname: require common substring of length 3+
            has_common = False
            for i in range(len(m_lower) - 2):
                if m_lower[i:i+3] in r_lower:
                    has_common = True
                    break
            if not has_common:
                suspicious.append(
                    f"'{member_name}' matched to '{register_name}' "
                    f"— surnames share no common 3-char substring: "
                    f"'{member_surname}' vs '{register_surname}'")

    if suspicious:
        return CheckResult(
            Level.WARN, "Name sanity",
            f"{len(suspicious)} matched pair(s) with suspicious surname mismatch",
            suspicious[:20])
    return CheckResult(Level.PASS, "Name sanity",
                       "All matched surname pairs pass sanity check")


def check_report_consistency(report_data):
    """Verify report is internally consistent."""
    if not report_data:
        return CheckResult(Level.FAIL, "Report consistency",
                           "Report file missing or not parseable")

    issues = []

    # Check machine-readable section exists
    if not report_data["machine_lines"] and not report_data["summary"]:
        issues.append("No machine-readable data or summary found")

    # Check summary fields present
    required_summary = ["Total members", "Matched (in register)"]
    for key in required_summary:
        if key not in report_data["summary"]:
            issues.append(f"Missing summary field: '{key}'")

    if issues:
        return CheckResult(Level.WARN, "Report consistency",
                           "Report has consistency issues", issues)
    return CheckResult(Level.PASS, "Report consistency",
                       "Report is internally consistent")


# ---------------------------------------------------------------------------
# INFO checks
# ---------------------------------------------------------------------------

def compute_statistics(output_rows, membership_rows, register_rows, report_data):
    """Compute and return summary statistics."""
    results = []
    total = len(membership_rows)
    reg_total = len(register_rows)
    out_total = len(output_rows)

    status_counts = Counter(row.get("Match_Status", "") for row in output_rows)

    matched = 0
    if report_data:
        matched = report_data["summary"].get("Matched (in register)", 0)

    rate = (matched / total * 100) if total else 0

    lines = [
        f"Membership rows: {total}",
        f"Register rows: {reg_total}",
        f"Matched (registered): {matched} ({rate:.1f}%)",
        f"Output rows: {out_total}",
    ]
    for status in sorted(status_counts.keys()):
        lines.append(f"  {status}: {status_counts[status]}")

    results.append(CheckResult(Level.INFO, "Statistics",
                               "Summary", lines))
    return results


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(results, membership_path, register_path, output_path,
                  report_path, strict):
    """Format validation results into human-readable report."""
    lines = []
    lines.append("=" * 60)
    lines.append("Membership Cross-Check Validation Report")
    lines.append("=" * 60)
    lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Membership file: {membership_path}")
    lines.append(f"Register file: {register_path}")
    lines.append(f"Output file: {output_path}")
    if report_path:
        lines.append(f"Check report: {report_path}")
    lines.append(f"Mode: {'strict' if strict else 'normal'}")
    lines.append("")

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

    # Verdict
    has_fails = bool(fails)
    if has_fails:
        lines.append("VERDICT: FAIL")
    elif warns:
        lines.append("VERDICT: PASS (with warnings)")
    else:
        lines.append("VERDICT: PASS")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main validation runner
# ---------------------------------------------------------------------------

def run_validation(membership_path, register_path, output_path,
                   report_path=None, strict=False, quiet=False):
    """Run all validation checks. Returns (exit_code, report_text)."""

    # Load CSVs
    membership_rows, _, membership_headers = read_input(membership_path)
    register_rows, _, _ = read_input(register_path)
    output_rows, _, output_headers = read_input(output_path)

    # Parse report
    if not report_path:
        report_path = output_path + ".report.txt"
    report_data = None
    if Path(report_path).exists():
        report_data = parse_membership_report(report_path)

    results = []

    # FAIL checks
    results.append(check_output_structure(output_headers, membership_headers))
    results.append(check_match_status_values(output_rows))
    results.append(check_matched_not_in_output(report_data, output_rows))
    results.append(check_output_traces_to_input(
        output_rows, membership_rows, membership_headers))
    results.append(check_accounting(
        report_data, output_rows, membership_rows, strict=strict))
    results.append(check_status_count_reconciliation(report_data, output_rows))
    results.append(check_field_preservation(
        output_rows, membership_rows, membership_headers))
    results.append(check_file_format(output_path))
    results.append(check_register_row_count(report_data, register_rows))

    # WARN checks
    results.append(check_match_rate(report_data, membership_rows))
    results.append(check_out_of_area_rate(report_data, membership_rows))
    results.append(check_no_postcode_rate(report_data, membership_rows))
    results.append(check_matched_name_sanity(report_data))
    results.append(check_report_consistency(report_data))

    # INFO
    results.extend(compute_statistics(
        output_rows, membership_rows, register_rows, report_data))

    # Format
    report_text = format_report(
        results, membership_path, register_path, output_path,
        report_path, strict)

    # Exit code
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
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Validate membership cross-check output.")
    parser.add_argument("membership", help="Original membership list CSV")
    parser.add_argument("register", help="Original register CSV")
    parser.add_argument("output", help="Cross-check output CSV to validate")
    parser.add_argument("--report", default=None,
                        help="Cross-check QA report (default: OUTPUT.report.txt)")
    parser.add_argument("--strict", action="store_true",
                        help="Check was run with --strict (adjusts accounting)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress stdout (only show WARN/FAIL)")
    args = parser.parse_args()

    exit_code, report_text = run_validation(
        membership_path=args.membership,
        register_path=args.register,
        output_path=args.output,
        report_path=args.report,
        strict=args.strict,
        quiet=args.quiet,
    )

    if not args.quiet or exit_code != 0:
        print(report_text)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
