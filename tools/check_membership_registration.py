#!/usr/bin/env python3
"""Cross-check Green Party membership list against raw electoral register.

Finds members who do NOT appear in the register (i.e. not registered to vote
locally). Outputs a CSV of unmatched members for voter registration drives.

Usage:
    python3 tools/check_membership_registration.py MEMBERSHIP.csv REGISTER.csv OUTPUT.csv
    python3 tools/check_membership_registration.py MEMBERSHIP.csv REGISTER.csv OUTPUT.csv --report report.txt
    python3 tools/check_membership_registration.py MEMBERSHIP.csv REGISTER.csv OUTPUT.csv --strict
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Add tools/ dir so we can import sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from ttw_common import read_input, normalize_postcode, UK_POSTCODE_RE
from enrich_register import (
    _surname_forename_similarity,
    _normalize_address,
    _address_similarity,
    _name_similarity,
    _extract_postcode,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_MEMBERSHIP_FIELDS = ["first_name", "last_name"]

# Multi-variant field lookups for register columns (council vs TTW format)
_SURNAME_KEYS = ["ElectorSurname", "Surname", "Last Name"]
_FORENAME_KEYS = ["ElectorForename", "Forename", "First Name"]
_ADDRESS_KEYS = [
    ("RegisteredAddress1", "RegisteredAddress2", "RegisteredAddress3",
     "RegisteredAddress4", "RegisteredAddress5", "RegisteredAddress6"),
    ("Address1", "Address2", "Address3", "Address4", "Address5", "Address6"),
]
_POSTCODE_KEYS = ["PostCode", "Postcode", "Post Code", "POSTCODE"]

# Matching thresholds
DEFAULT_THRESHOLD = 0.8
NO_POSTCODE_THRESHOLD = 0.95
POSSIBLE_THRESHOLD = 0.6
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
    """Build concatenated address string from register row, trying both column naming conventions."""
    for addr_keys in _ADDRESS_KEYS:
        parts = [row.get(k, "").strip() for k in addr_keys]
        combined = " ".join(p for p in parts if p)
        if combined:
            return combined
    return ""


def _get_register_postcode(row):
    """Extract and normalize postcode from register row."""
    for key in _POSTCODE_KEYS:
        val = row.get(key, "").strip()
        if val:
            pc, _ = normalize_postcode(val)
            return pc or ""
    return ""


def _get_member_postcode(row):
    """Extract postcode from membership row: try zip_code first, then search can2_user_address."""
    return _extract_postcode(row, field_order=("zip_code", "can2_user_address"))


# ---------------------------------------------------------------------------
# QA Report
# ---------------------------------------------------------------------------

class MembershipCheckReport:
    """Collects report entries during membership cross-check."""

    def __init__(self):
        self.membership_file = ""
        self.register_file = ""
        self.output_file = ""
        self.total_members = 0
        self.total_register = 0
        self.matched = 0
        self.unmatched = 0
        self.out_of_area = 0
        self.no_postcode = 0
        self.ambiguous = 0
        self.possible = 0

        self.matched_details = []      # [(member_name, register_name, postcode, score)]
        self.possible_details = []     # [(member_name, postcode, score, register_name)]
        self.ambiguous_details = []    # [(member_name, postcode, [(candidate, score)])]
        self.unmatched_details = []    # [(member_name, postcode)]
        self.out_of_area_details = []  # [(member_name, postcode, fallback_match_or_None)]
        self.no_postcode_details = []  # [(member_name,)]
        self.warnings = []

    def write(self, path):
        """Write human-readable report with machine-readable footer."""
        lines = []
        lines.append("=" * 60)
        lines.append("Membership vs Register Cross-Check Report")
        lines.append("=" * 60)
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Membership file: {self.membership_file}")
        lines.append(f"Register file: {self.register_file}")
        lines.append(f"Output file: {self.output_file}")
        lines.append("")

        # --- Summary ---
        lines.append("--- Summary ---")
        lines.append(f"Total members: {self.total_members}")
        lines.append(f"Total register entries: {self.total_register}")
        lines.append(f"Matched (in register): {self.matched}")
        lines.append(f"Possible matches (included in output): {self.possible}")
        lines.append(f"Ambiguous (included in output): {self.ambiguous}")
        lines.append(f"Unmatched (not in register): {self.unmatched}")
        lines.append(f"Out of area: {self.out_of_area}")
        lines.append(f"No postcode: {self.no_postcode}")
        lines.append("")

        # --- Matched ---
        if self.matched_details:
            lines.append("--- Confident Matches (excluded from output) ---")
            for member_name, reg_name, pc, score in self.matched_details:
                lines.append(f"  {member_name} <-> {reg_name} ({pc}, score={score:.2f})")
            lines.append("")

        # --- Possible ---
        if self.possible_details:
            lines.append("--- Possible Matches (included in output for review) ---")
            for member_name, pc, score, reg_name in self.possible_details:
                lines.append(f"  {member_name} ~ {reg_name} ({pc}, score={score:.2f})")
            lines.append("")

        # --- Ambiguous ---
        if self.ambiguous_details:
            lines.append("--- Ambiguous Matches (included in output for review) ---")
            for member_name, pc, candidates in self.ambiguous_details:
                cands_str = ", ".join(f"{n} ({s:.2f})" for n, s in candidates)
                lines.append(f"  {member_name} ({pc}): {cands_str}")
            lines.append("")

        # --- Unmatched ---
        if self.unmatched_details:
            lines.append("--- Unmatched (not found in register) ---")
            for member_name, pc in self.unmatched_details:
                lines.append(f"  {member_name} ({pc or 'no postcode'})")
            lines.append("")

        # --- Out of area ---
        if self.out_of_area_details:
            lines.append("--- Out of Area (postcode not in register) ---")
            for member_name, pc, fallback in self.out_of_area_details:
                if fallback:
                    lines.append(f"  {member_name} ({pc}) -- possible name match: {fallback}")
                else:
                    lines.append(f"  {member_name} ({pc})")
            lines.append("")

        # --- No postcode ---
        if self.no_postcode_details:
            lines.append("--- No Postcode ---")
            for (member_name,) in self.no_postcode_details:
                lines.append(f"  {member_name}")
            lines.append("")

        # --- Warnings ---
        if self.warnings:
            lines.append("--- Warnings ---")
            for w in self.warnings:
                lines.append(f"  {w}")
            lines.append("")

        # Note about false negatives
        lines.append("--- Note ---")
        lines.append("Members listed as unmatched may be registered under a different name")
        lines.append("variant (nickname, married name, etc.). Review possible matches carefully.")
        lines.append("")

        # --- Machine-readable ---
        lines.append("### MACHINE-READABLE SECTION ###")
        for member_name, reg_name, pc, score in self.matched_details:
            lines.append(f"MATCHED|Member={member_name}|Register={reg_name}|PostCode={pc}|Score={score:.2f}")
        for member_name, pc, score, reg_name in self.possible_details:
            lines.append(f"POSSIBLE|Member={member_name}|Candidate={reg_name}|PostCode={pc}|Score={score:.2f}")
        for member_name, pc, candidates in self.ambiguous_details:
            cands = ";".join(f"{n}:{s:.2f}" for n, s in candidates)
            lines.append(f"AMBIGUOUS|Member={member_name}|PostCode={pc}|Candidates={cands}")
        for member_name, pc in self.unmatched_details:
            lines.append(f"UNMATCHED|Member={member_name}|PostCode={pc}")
        for member_name, pc, fallback in self.out_of_area_details:
            fb = f"|Fallback={fallback}" if fallback else ""
            lines.append(f"OUT_OF_AREA|Member={member_name}|PostCode={pc}{fb}")
        for (member_name,) in self.no_postcode_details:
            lines.append(f"NO_POSTCODE|Member={member_name}")
        lines.append("### END MACHINE-READABLE SECTION ###")

        Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Core matching
# ---------------------------------------------------------------------------

def match_members_to_register(member_rows, register_rows, threshold, report):
    """Match membership rows against register. Returns list of output rows
    (unmatched/possible/ambiguous/out_of_area/no_postcode members)."""

    # Build postcode index from register
    pc_index = defaultdict(list)  # postcode -> [(idx, surname, forename, addr_str)]
    for i, row in enumerate(register_rows):
        pc = _get_register_postcode(row)
        surname = _get_field(row, _SURNAME_KEYS)
        forename = _get_field(row, _FORENAME_KEYS)
        addr_str = _get_register_address(row)
        if pc:
            pc_index[pc].append((i, surname, forename, addr_str))

    # All-rows list for no-postcode / out-of-area fallback
    all_register = [
        (i,
         _get_field(row, _SURNAME_KEYS),
         _get_field(row, _FORENAME_KEYS),
         _get_register_address(row))
        for i, row in enumerate(register_rows)
    ]

    report.total_members = len(member_rows)
    report.total_register = len(register_rows)

    output_rows = []

    for member_row in member_rows:
        first = member_row.get("first_name", "").strip()
        last = member_row.get("last_name", "").strip()
        member_name = f"{first} {last}".strip() or "(unknown)"
        member_addr = member_row.get("can2_user_address", "").strip()
        member_pc = _get_member_postcode(member_row)

        # Determine candidates and effective threshold
        if member_pc:
            candidates = pc_index.get(member_pc, None)
            if candidates is not None:
                effective_threshold = threshold
            else:
                # Postcode valid but not in register — out of area
                # Do a high-threshold name-only scan to catch stale postcodes
                fallback_match = _find_name_fallback(
                    last, first, all_register, NO_POSTCODE_THRESHOLD)
                report.out_of_area += 1
                report.out_of_area_details.append(
                    (member_name, member_pc, fallback_match))
                out_row = dict(member_row)
                out_row["Match_Status"] = "out_of_area"
                out_row["Best_Candidate"] = fallback_match or ""
                output_rows.append(out_row)
                continue
        else:
            if not first and not last:
                # No name, no postcode — skip with warning
                report.warnings.append(f"Skipped row with no name and no postcode")
                continue
            # No postcode — scan full register at elevated threshold
            candidates = all_register
            effective_threshold = NO_POSTCODE_THRESHOLD
            report.no_postcode += 1
            report.no_postcode_details.append((member_name,))

        if not candidates:
            report.unmatched += 1
            report.unmatched_details.append((member_name, member_pc))
            out_row = dict(member_row)
            out_row["Match_Status"] = "unmatched"
            out_row["Best_Candidate"] = ""
            output_rows.append(out_row)
            continue

        # Score all candidates
        scored = []
        for reg_idx, reg_surname, reg_forename, reg_addr in candidates:
            score = _surname_forename_similarity(
                last, first, reg_surname, reg_forename)
            scored.append((score, reg_idx, reg_surname, reg_forename, reg_addr))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_idx, best_surname, best_forename, best_addr = scored[0]
        best_name = f"{best_forename} {best_surname}".strip()

        # Ambiguity check
        if len(scored) > 1 and best_score < 1.0:
            second_score = scored[1][0]
            if (best_score >= effective_threshold
                    and (best_score - second_score) < AMBIGUITY_MARGIN):
                # Try address tiebreaker
                if member_addr:
                    addr_scores = [
                        (s[0] + 0.001 * _address_similarity(member_addr, s[4]), s)
                        for s in scored[:2]
                    ]
                    addr_scores.sort(key=lambda x: x[0], reverse=True)
                    if addr_scores[0][0] - addr_scores[1][0] >= AMBIGUITY_MARGIN:
                        # Address broke the tie
                        best = addr_scores[0][1]
                        best_score, best_idx, best_surname, best_forename, best_addr = best
                        best_name = f"{best_forename} {best_surname}".strip()
                        # Fall through to confident match check below
                    else:
                        # Still ambiguous
                        cands = [(f"{s[3]} {s[2]}".strip(), s[0]) for s in scored[:2]]
                        report.ambiguous += 1
                        report.ambiguous_details.append((member_name, member_pc, cands))
                        out_row = dict(member_row)
                        out_row["Match_Status"] = "ambiguous"
                        out_row["Best_Candidate"] = f"{cands[0][0]} ({cands[0][1]:.2f})"
                        output_rows.append(out_row)
                        continue
                else:
                    cands = [(f"{s[3]} {s[2]}".strip(), s[0]) for s in scored[:2]]
                    report.ambiguous += 1
                    report.ambiguous_details.append((member_name, member_pc, cands))
                    out_row = dict(member_row)
                    out_row["Match_Status"] = "ambiguous"
                    out_row["Best_Candidate"] = f"{cands[0][0]} ({cands[0][1]:.2f})"
                    output_rows.append(out_row)
                    continue

        if best_score >= effective_threshold:
            # Confident match — member IS registered
            report.matched += 1
            report.matched_details.append(
                (member_name, best_name, member_pc, best_score))
            # Do NOT add to output — they're registered
        elif best_score >= POSSIBLE_THRESHOLD:
            # Possible match — include in output by default for review
            report.possible += 1
            report.possible_details.append(
                (member_name, member_pc, best_score, best_name))
            out_row = dict(member_row)
            out_row["Match_Status"] = "possible"
            out_row["Best_Candidate"] = f"{best_name} ({best_score:.2f})"
            output_rows.append(out_row)
        else:
            # No match found
            if not member_pc:
                # Already counted as no_postcode above; just add to output
                out_row = dict(member_row)
                out_row["Match_Status"] = "no_postcode"
                out_row["Best_Candidate"] = f"{best_name} ({best_score:.2f})" if best_name else ""
                output_rows.append(out_row)
            else:
                report.unmatched += 1
                report.unmatched_details.append((member_name, member_pc))
                out_row = dict(member_row)
                out_row["Match_Status"] = "unmatched"
                out_row["Best_Candidate"] = f"{best_name} ({best_score:.2f})" if best_name else ""
                output_rows.append(out_row)

    return output_rows


def _find_name_fallback(surname, forename, all_register, threshold):
    """Scan full register for a high-confidence name match (for out-of-area fallback).
    Returns display name of best match if above threshold, else None."""
    best_score = 0
    best_name = None
    for _, reg_surname, reg_forename, _ in all_register:
        score = _surname_forename_similarity(surname, forename, reg_surname, reg_forename)
        if score > best_score:
            best_score = score
            best_name = f"{reg_forename} {reg_surname}".strip()
    if best_score >= threshold:
        return f"{best_name} ({best_score:.2f})"
    return None


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def validate_membership_csv(headers):
    """Check membership CSV has required fields. Returns list of errors."""
    errors = []
    for field in REQUIRED_MEMBERSHIP_FIELDS:
        if field not in headers:
            errors.append(f"Membership CSV missing required column: '{field}'")
    return errors


def validate_register_csv(headers):
    """Check register CSV has name and postcode fields. Returns list of errors."""
    has_surname = any(k in headers for k in _SURNAME_KEYS)
    has_forename = any(k in headers for k in _FORENAME_KEYS)
    has_postcode = any(k in headers for k in _POSTCODE_KEYS)
    errors = []
    if not has_surname:
        errors.append(f"Register CSV missing surname column (expected one of: {', '.join(_SURNAME_KEYS)})")
    if not has_forename:
        errors.append(f"Register CSV missing forename column (expected one of: {', '.join(_FORENAME_KEYS)})")
    if not has_postcode:
        errors.append(f"Register CSV missing postcode column (expected one of: {', '.join(_POSTCODE_KEYS)})")
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cross-check membership list against electoral register. "
                    "Outputs members not found in the register.")
    parser.add_argument("membership", help="Membership list CSV")
    parser.add_argument("register", help="Raw electoral register CSV (council format)")
    parser.add_argument("output", help="Output CSV of unmatched members")
    parser.add_argument("--report", default=None,
                        help="QA report path (default: OUTPUT.report.txt)")
    parser.add_argument("--match-threshold", type=float, default=DEFAULT_THRESHOLD,
                        help=f"Fuzzy match threshold (default: {DEFAULT_THRESHOLD})")
    parser.add_argument("--strict", action="store_true",
                        help="Exclude possible matches from output (only confident unmatched)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress stdout output")
    args = parser.parse_args()

    report_path = args.report or (args.output + ".report.txt")

    # Overwrite protection
    for input_path in [args.membership, args.register]:
        if os.path.abspath(args.output) == os.path.abspath(input_path):
            print(f"ERROR: Output path '{args.output}' would overwrite input '{input_path}'.",
                  file=sys.stderr)
            sys.exit(1)

    # Read inputs
    member_rows, _, member_headers = read_input(args.membership)
    register_rows, _, register_headers = read_input(args.register)

    # Validate
    errors = validate_membership_csv(member_headers)
    errors.extend(validate_register_csv(register_headers))
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if not member_rows:
        print("ERROR: Membership CSV is empty (no data rows).", file=sys.stderr)
        sys.exit(1)

    if not register_rows:
        print("ERROR: Register CSV is empty (no data rows).", file=sys.stderr)
        sys.exit(1)

    # Run matching
    report = MembershipCheckReport()
    report.membership_file = args.membership
    report.register_file = args.register
    report.output_file = args.output

    output_rows = match_members_to_register(
        member_rows, register_rows, args.match_threshold, report)

    # Apply --strict: remove possible matches from output
    if args.strict:
        output_rows = [r for r in output_rows if r.get("Match_Status") != "possible"]

    # Write output CSV
    output_headers = list(member_headers) + ["Match_Status", "Best_Candidate"]
    with open(args.output, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_headers,
                                lineterminator="\r\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    # Write report
    report.write(report_path)

    # Summary
    if not args.quiet:
        total_output = len(output_rows)
        print(f"Membership cross-check complete.")
        print(f"  Members: {report.total_members}")
        print(f"  Register entries: {report.total_register}")
        print(f"  Matched (registered): {report.matched}")
        print(f"  Output rows (not registered / review): {total_output}")
        if report.out_of_area:
            print(f"  Out of area: {report.out_of_area}")
        if report.no_postcode:
            print(f"  No postcode: {report.no_postcode}")
        print(f"  Report: {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
