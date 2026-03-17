#!/usr/bin/env python3
"""Enrich a cleaned TTW electoral register CSV with canvassing and register data.

Usage:
    python3 tools/enrich_register.py BASE_TTW.csv OUTPUT.csv \\
        --enriched-register SPREADSHEET2.csv \\
        --canvassing-export SPREADSHEET1.csv \\
        --canvassing-register NEW_CANVASSING.csv \\
        --historic-elections GE2024 \\
        --future-elections 2026 \\
        [--strip-extra] [--report PATH] [--match-threshold 0.8] [--dry-run] [--quiet]

Sources:
    --enriched-register   Register-format CSV with historic election data
    --canvassing-export   TTW canvassing export CSV (profile_name, address fields)
    --canvassing-register Register-format CSV with future election canvassing data
                          (Party, 1-5, Comments). Requires --future-elections.
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Import from shared utilities module
sys.path.insert(0, str(Path(__file__).parent))
from ttw_common import (read_input, write_output, normalize_postcode,
                        UK_POSTCODE_RE, VALID_PARTY_CODES,
                        map_party_name as _map_party_name_common)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Extra columns from enriched register (non-TTW)
EXTRA_COLS_REGISTER = [
    "Email Address", "Phone number", "Comments", "Issues",
    "DNK", "New", "1st round",
]

# Extra columns from canvassing export (non-TTW)
EXTRA_COLS_CANVASSING = ["visit_issues", "visit_notes"]

# Extra columns from canvassing register (non-TTW)
EXTRA_COLS_CANVASSING_REGISTER = ["Comments"]

# Core identity/address fields in enriched register that must NOT be modified
# during duplicate merge — only enrichment data columns should be merged.
_ER_CORE_FIELDS = frozenset([
    # Identity fields
    "PDCode", "RollNo", "Forename", "Surname", "PostCode",
    "ElectorSurname", "ElectorForename", "Full Name",
    "Middle Names", "First Name", "Last Name",
    "SubHouse", "ElectorID", "UPRN",
    # Address fields
    "RegisteredAddress1", "RegisteredAddress2", "RegisteredAddress3",
    "RegisteredAddress4", "RegisteredAddress5", "RegisteredAddress6",
    "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
    "Post Code", "Postcode", "POSTCODE",
    # Electoral structure / registration metadata
    "FranchiseMarker", "DateOfAttainment", "Date of Attainment",
    "Euro", "Parl", "Ward",
    "MethodOfVerification",
])


# ---------------------------------------------------------------------------
# QA Report for enrichment
# ---------------------------------------------------------------------------

class EnrichQAReport:
    """Collects report entries during enrichment."""

    def __init__(self):
        self.base_file = ""
        self.output_file = ""
        self.enriched_register_file = ""
        self.canvassing_export_file = ""
        self.base_rows = 0
        self.output_rows = 0

        # Enriched register matching
        self.er_total = 0
        self.er_matched = 0
        self.er_unmatched = []      # [(postcode, er_display_name)]
        self.er_possible = []       # [(name, postcode, score, candidate_name)]
        self.er_ambiguous = []      # [(name, postcode, [(candidate_name, score)])]
        self.er_confident_matches = [] # [(er_name, base_name, postcode, score)]
        self.er_duplicate_keys = [] # [(name, postcode, count)]
        self.er_merge_clashes = []  # [(name, postcode, field, kept_val, discarded_val)]
        self.er_merge_count = 0     # number of ER rows merged (not discarded)

        # Canvassing register matching
        self.canvassing_register_file = ""
        self.cr_total = 0
        self.cr_matched = 0
        self.cr_unmatched = []       # [(postcode, name)]
        self.cr_confident_matches = [] # [(cr_name, base_name, postcode, score)]
        self.cr_possible = []        # [(name, postcode, score, candidate_name)]
        self.cr_ambiguous = []       # [(name, postcode, [(candidate_name, score)])]

        # Column mapping and overwrite tracking
        self.column_mapping = []           # [(source_col, target_col)]
        self.new_columns_created = []      # [col_name]
        self.existing_columns_updated = [] # [col_name]
        self.overwrite_details = []        # [(row_key, field, old, new)]
        self.preserved_count = 0           # fields where blank incoming preserved non-empty existing

        # Canvassing export matching
        self.ce_total = 0
        self.ce_confident = 0
        self.ce_possible = []       # [(profile_name, addr, score, candidate_name)]
        self.ce_ambiguous = []      # [(profile_name, addr, [(name, score)])]
        self.ce_unmatched = []      # [(profile_name, addr, best_score)]
        self.ce_duplicate_visits = [] # [(base_key, count)]
        self.ce_unmatched_rows = []  # [dict] full rows for unmatched CSV export
        self.ce_headers = []         # original DS3 column order
        self.ce_has_dnk = False      # True if DS3 CSV has a DNK column
        self.unmatched_csv_path = ""  # set by main() if written

        # Data
        self.conflicts = []         # [(row_key, field, er_val, ce_val, resolved)]
        self.unrecognized_parties = [] # [(source, value)]
        self.warnings = []          # [str]
        self.questions_data = {}    # field_name -> bool (has data)

    def write(self, path):
        """Write human-readable report with machine-readable footer."""
        lines = []
        lines.append("=" * 60)
        lines.append("Electoral Register Enrichment QA Report")
        lines.append("=" * 60)
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Base file: {self.base_file}")
        lines.append(f"Output file: {self.output_file}")
        if self.enriched_register_file:
            lines.append(f"Enriched register: {self.enriched_register_file}")
        if self.canvassing_export_file:
            lines.append(f"Canvassing export: {self.canvassing_export_file}")
        if self.canvassing_register_file:
            lines.append(f"Canvassing register: {self.canvassing_register_file}")
        lines.append("")

        # --- Summary ---
        lines.append("--- Summary ---")
        lines.append(f"Base rows: {self.base_rows}")
        lines.append(f"Output rows: {self.output_rows}")
        if self.enriched_register_file:
            pct = (self.er_matched / self.er_total * 100) if self.er_total else 0
            lines.append(f"Enriched register: {self.er_matched}/{self.er_total} matched ({pct:.1f}%)")
        if self.canvassing_export_file:
            lines.append(f"Canvassing export: {self.ce_confident} confident, "
                         f"{len(self.ce_possible)} possible, "
                         f"{len(self.ce_ambiguous)} ambiguous, "
                         f"{len(self.ce_unmatched)} unmatched "
                         f"(of {self.ce_total} total)")
        if self.canvassing_register_file:
            cr_pct = (self.cr_matched / self.cr_total * 100) if self.cr_total else 0
            lines.append(f"Canvassing register: {self.cr_matched}/{self.cr_total} matched ({cr_pct:.1f}%)")
        lines.append("")

        # --- Enriched Register Matching ---
        if self.enriched_register_file:
            lines.append("--- Enriched Register Matching ---")
            lines.append(f"Total rows: {self.er_total}")
            lines.append(f"Confident matches: {self.er_matched}")
            lines.append(f"Unmatched: {len(self.er_unmatched)}")
            lines.append(f"Possible matches: {len(self.er_possible)}")
            lines.append(f"Ambiguous matches: {len(self.er_ambiguous)}")
            lines.append(f"Duplicate ER rows (same base): {len(self.er_duplicate_keys)}")
            if self.er_unmatched:
                lines.append("  Unmatched rows:")
                for postcode, name in self.er_unmatched:
                    lines.append(f"    {name} ({postcode})")
            if self.er_possible:
                lines.append("  Possible matches (for human review):")
                for name, postcode, score, candidate in self.er_possible:
                    lines.append(f"    \"{name}\" ({postcode}) -> \"{candidate}\" (score={score:.3f})")
            if self.er_ambiguous:
                lines.append("  Ambiguous matches:")
                for name, postcode, candidates in self.er_ambiguous:
                    cand_str = ", ".join(f"\"{n}\" ({s:.3f})" for n, s in candidates)
                    lines.append(f"    \"{name}\" ({postcode}): {cand_str}")
            if self.er_duplicate_keys:
                lines.append(f"  Duplicate ER rows (merged): {len(self.er_duplicate_keys)}")
                if self.er_merge_count:
                    lines.append(f"  Gap fills from duplicates: {self.er_merge_count}")
            if self.er_merge_clashes:
                # Only list duplicates that had clashes — these need manual review
                clash_names = set((n, p) for n, p, _, _, _ in self.er_merge_clashes)
                lines.append(f"  Merge clashes (need review): {len(self.er_merge_clashes)}")
                for name, postcode, field, kept, discarded in self.er_merge_clashes:
                    lines.append(f"    {name} ({postcode}): {field} kept=\"{kept}\" discarded=\"{discarded}\"")
            lines.append("")

        # --- Canvassing Export Matching ---
        if self.canvassing_export_file:
            lines.append("--- Canvassing Export Matching ---")
            lines.append(f"Total rows: {self.ce_total}")
            lines.append(f"Confident matches: {self.ce_confident}")
            if self.ce_possible:
                lines.append(f"Possible matches (for human review): {len(self.ce_possible)}")
                for profile_name, addr, score, candidate in self.ce_possible:
                    lines.append(f"    \"{profile_name}\" ({addr}) -> \"{candidate}\" (score={score:.3f})")
            if self.ce_ambiguous:
                lines.append(f"Ambiguous matches: {len(self.ce_ambiguous)}")
                for profile_name, addr, candidates in self.ce_ambiguous:
                    cand_str = ", ".join(f"\"{n}\" ({s:.3f})" for n, s in candidates)
                    lines.append(f"    \"{profile_name}\" ({addr}): {cand_str}")
            if self.ce_unmatched:
                lines.append(f"Unmatched: {len(self.ce_unmatched)}")
                for profile_name, addr, best_score in self.ce_unmatched:
                    if best_score is not None:
                        lines.append(f"    \"{profile_name}\" ({addr}) best_score={best_score:.3f}")
                    else:
                        lines.append(f"    \"{profile_name}\" ({addr}) no candidates")
            if self.ce_duplicate_visits:
                lines.append(f"Duplicate canvassing visits: {len(self.ce_duplicate_visits)}")
                for key, count in self.ce_duplicate_visits:
                    lines.append(f"    Base row {key}: {count} visits (last used)")
            if self.unmatched_csv_path:
                lines.append(f"Unmatched rows exported to: {self.unmatched_csv_path}")
            lines.append("")

        # --- Canvassing Register Matching ---
        if self.canvassing_register_file:
            lines.append("--- Canvassing Register Matching ---")
            lines.append(f"Total rows: {self.cr_total}")
            lines.append(f"Confident matches: {self.cr_matched}")
            lines.append(f"Unmatched: {len(self.cr_unmatched)}")
            lines.append(f"Possible matches: {len(self.cr_possible)}")
            lines.append(f"Ambiguous matches: {len(self.cr_ambiguous)}")
            if self.cr_unmatched:
                lines.append("  Unmatched rows:")
                for postcode, name in self.cr_unmatched:
                    lines.append(f"    {name} ({postcode})")
            if self.cr_possible:
                lines.append("  Possible matches (for human review):")
                for name, postcode, score, candidate in self.cr_possible:
                    lines.append(f"    \"{name}\" ({postcode}) -> \"{candidate}\" (score={score:.3f})")
            if self.cr_ambiguous:
                lines.append("  Ambiguous matches:")
                for name, postcode, candidates in self.cr_ambiguous:
                    cand_str = ", ".join(f"\"{n}\" ({s:.3f})" for n, s in candidates)
                    lines.append(f"    \"{name}\" ({postcode}): {cand_str}")
            lines.append("")

        # --- Data Conflicts ---
        if self.conflicts:
            lines.append("--- Data Conflicts ---")
            for row_key, field, er_val, ce_val, resolved in self.conflicts:
                lines.append(f"  Row {row_key}: {field} -- "
                             f"enriched register=\"{er_val}\" vs canvassing=\"{ce_val}\" "
                             f"-> resolved=\"{resolved}\"")
            lines.append("")

        # --- Unrecognized Party Values ---
        if self.unrecognized_parties:
            lines.append("--- Unrecognized Party Values ---")
            for source, value in self.unrecognized_parties:
                lines.append(f"  [{source}] \"{value}\" -- kept as-is")
            lines.append("")

        # --- Warnings ---
        if self.warnings:
            lines.append("--- Warnings ---")
            for w in self.warnings:
                lines.append(f"  {w}")
            lines.append("")

        # --- Questions to Resolve ---
        questions = []
        if self.questions_data.get("1-5"):
            questions.append("Q1: Is `1-5` = Green Voting Intention? "
                             "-> likely TTW field: <election> Green Voting Intention")
        if self.questions_data.get("PostalVoter"):
            questions.append("Q2: Do `PostalVoter?` and `P/PB` overlap? "
                             "-> likely TTW field: <election> Postal Voter")
        if self.questions_data.get("DNK"):
            questions.append("Q3: Is `DNK` = Do Not Knock? -> no TTW equivalent")
        if self.questions_data.get("New"):
            questions.append("Q4: What does `New` mean? -> no TTW equivalent")
        if self.questions_data.get("1st round"):
            questions.append("Q5: What does `1st round` mean? -> no TTW equivalent")
        if self.conflicts:
            questions.append("Q6: Conflict resolution priority correct? "
                             "(enriched register wins over canvassing export)")
        if questions:
            lines.append("--- Questions to Resolve ---")
            for q in questions:
                lines.append(f"  {q}")
            lines.append("")

        # --- Column Mapping ---
        if self.column_mapping:
            lines.append("--- Column Mapping ---")
            for source, target in self.column_mapping:
                lines.append(f"  {source} -> {target}")
            lines.append("")

        # --- New Columns Created ---
        if self.new_columns_created:
            lines.append("--- New Columns Created ---")
            for col in self.new_columns_created:
                lines.append(f"  {col}")
            lines.append("")

        # --- Existing Columns Updated ---
        if self.existing_columns_updated:
            lines.append("--- Existing Columns Updated ---")
            for col in self.existing_columns_updated:
                lines.append(f"  {col}")
            lines.append("")

        # --- Overwrite Details ---
        if self.overwrite_details or self.preserved_count:
            lines.append("--- Overwrite Details ---")
            if self.overwrite_details:
                for row_key, field, old, new in self.overwrite_details:
                    lines.append(f"  Row {row_key}: {field} \"{old}\" -> \"{new}\"")
            if self.preserved_count:
                lines.append(f"  Preserved {self.preserved_count} existing value(s) where incoming was blank")
            lines.append("")

        # --- Machine-readable footer ---
        lines.append("### MACHINE-READABLE SECTION ###")
        for row_key, field, er_val, ce_val, resolved in self.conflicts:
            lines.append(f"CONFLICT|Row={row_key}|Field={field}"
                         f"|EnrichedRegister={er_val}|Canvassing={ce_val}"
                         f"|Resolved={resolved}")
        for w in self.warnings:
            lines.append(f"WARNING|{w}")
        if self.enriched_register_file:
            for postcode, name in self.er_unmatched:
                lines.append(f"MATCH|Source=enriched_register|Status=unmatched"
                             f"|PostCode={postcode}|Name={name}")
            for er_name, base_name, postcode, score in self.er_confident_matches:
                lines.append(f"MATCH|Source=enriched_register|Status=confident"
                             f"|ERName={er_name}|BaseName={base_name}"
                             f"|PostCode={postcode}|Score={score:.3f}")
        if self.canvassing_export_file:
            for profile_name, addr, score, candidate in self.ce_possible:
                lines.append(f"MATCH|Source=canvassing|Status=possible"
                             f"|Name={profile_name}|Score={score:.3f}"
                             f"|Candidate={candidate}")
        if self.canvassing_register_file:
            for postcode, name in self.cr_unmatched:
                lines.append(f"MATCH|Source=canvassing_register|Status=unmatched"
                             f"|PostCode={postcode}|Name={name}")
            for cr_name, base_name, postcode, score in self.cr_confident_matches:
                lines.append(f"MATCH|Source=canvassing_register|Status=confident"
                             f"|CRName={cr_name}|BaseName={base_name}"
                             f"|PostCode={postcode}|Score={score:.3f}")
        for row_key, field, old, new in self.overwrite_details:
            lines.append(f"OVERWRITE|Row={row_key}|Field={field}|Old={old}|New={new}")
        for name, postcode, field, kept, discarded in self.er_merge_clashes:
            lines.append(f"MERGE_CLASH|Name={name}|PostCode={postcode}"
                         f"|Field={field}|Kept={kept}|Discarded={discarded}")
        if self.canvassing_export_file:
            lines.append(f"SUMMARY|Source=canvassing|Total={self.ce_total}"
                         f"|Confident={self.ce_confident}"
                         f"|Possible={len(self.ce_possible)}"
                         f"|Ambiguous={len(self.ce_ambiguous)}"
                         f"|Unmatched={len(self.ce_unmatched)}")
        if self.enriched_register_file:
            lines.append(f"SUMMARY|Source=enriched_register|Total={self.er_total}"
                         f"|Matched={self.er_matched}"
                         f"|Unmatched={len(self.er_unmatched)}"
                         f"|Possible={len(self.er_possible)}"
                         f"|Ambiguous={len(self.er_ambiguous)}")
        if self.canvassing_register_file:
            lines.append(f"SUMMARY|Source=canvassing_register|Total={self.cr_total}"
                         f"|Matched={self.cr_matched}"
                         f"|Unmatched={len(self.cr_unmatched)}"
                         f"|Possible={len(self.cr_possible)}"
                         f"|Ambiguous={len(self.cr_ambiguous)}")
        lines.append("### END MACHINE-READABLE SECTION ###")

        Path(path).write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Party name mapping
# ---------------------------------------------------------------------------

def map_party_name(value, report, source="unknown"):
    """Map a party name to TTW code. Returns mapped value.

    Wraps the common map_party_name() and routes warnings to the enrichment report.
    """
    mapped, warning = _map_party_name_common(value)
    if warning:
        raw = value.strip() if value else ""
        report.unrecognized_parties.append((source, raw))
    return mapped


# ---------------------------------------------------------------------------
# Fuzzy matching utilities (stdlib only)
# ---------------------------------------------------------------------------

def _bigrams(s):
    """Return set of character bigrams from a string."""
    s = s.lower().strip()
    if len(s) < 2:
        return set()
    return {s[i:i+2] for i in range(len(s) - 1)}


def _dice_coefficient(a, b):
    """Bigram Dice coefficient between two strings."""
    if not a or not b:
        return 0.0
    ba = _bigrams(a)
    bb = _bigrams(b)
    if not ba or not bb:
        return 0.0
    overlap = len(ba & bb)
    return 2.0 * overlap / (len(ba) + len(bb))


def _levenshtein(a, b):
    """Levenshtein edit distance between two strings."""
    if len(a) < len(b):
        return _levenshtein(b, a)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            cost = 0 if ca == cb else 1
            curr.append(min(curr[j] + 1, prev[j + 1] + 1, prev[j] + cost))
        prev = curr
    return prev[-1]


def _name_similarity(name_a, name_b):
    """Compare two full names. Handles short names specially."""
    a = (name_a or "").strip()
    b = (name_b or "").strip()
    if not a or not b:
        return 0.0

    # Short name handling: if either name < 4 chars total
    if len(a) < 4 or len(b) < 4:
        if a.lower() == b.lower():
            return 1.0
        if _levenshtein(a.lower(), b.lower()) <= 1:
            return 0.9
        return 0.0

    return _dice_coefficient(a, b)


def _surname_forename_similarity(surname_a, forename_a, surname_b, forename_b):
    """Score = 0.6 * surname_similarity + 0.4 * forename_similarity.

    Prioritises surname (more discriminating across families). For same-family
    disambiguation, the 40% forename weight provides sufficient margin.
    """
    return (0.6 * _name_similarity(surname_a, surname_b)
            + 0.4 * _name_similarity(forename_a, forename_b))


def _normalize_address(addr):
    """Normalize address for comparison: strip brackets, commas, ampersands, leading zeros, sort tokens."""
    s = addr or ""
    s = s.replace("[", "").replace("]", "")
    s = s.replace(",", "")
    s = s.replace("&", "and")
    s = re.sub(r'\b0+(\d)', r'\1', s)  # "Flat 01" -> "Flat 1"
    s = re.sub(r'\s+', ' ', s).strip()
    s = " ".join(sorted(s.split()))    # "1 Willesden House" -> "1 House Willesden"
    return s


def _address_similarity(addr_a, addr_b):
    """Compare two address strings using Dice coefficient."""
    return _dice_coefficient(_normalize_address(addr_a), _normalize_address(addr_b))


def _extract_postcode(row_dict, field_order=("address 4", "address 3", "address 2", "address 1")):
    """Extract and normalize postcode from canvassing address fields.
    Scans fields in order (later address fields first). Returns normalized postcode or ""."""
    for field in field_order:
        val = row_dict.get(field, "").strip()
        if not val:
            continue
        # Try the whole field as a postcode
        normalized, _ = normalize_postcode(val)
        if normalized and UK_POSTCODE_RE.match(normalized):
            return normalized
        # Try to find a postcode within the field
        match = re.search(r"[A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2}", val.upper())
        if match:
            candidate = match.group()
            normalized, _ = normalize_postcode(candidate)
            if normalized and UK_POSTCODE_RE.match(normalized):
                return normalized
    return ""


# ---------------------------------------------------------------------------
# Flexible postal voter column lookup
# ---------------------------------------------------------------------------

_POSTAL_VOTER_KEYS = ["PostalVoter?", "PostalVoter", "Postal Voter",
                      "postalvoter?", "postalvoter", "postal voter",
                      "POSTALVOTER?", "POSTALVOTER", "POSTAL VOTER",
                      "Postal voter", "postal Voter"]

def _get_postal_voter(row):
    """Get postal voter value from a row, accepting multiple column name variants."""
    for key in _POSTAL_VOTER_KEYS:
        val = row.get(key, "").strip()
        if val:
            return val
    return ""


# ---------------------------------------------------------------------------
# Overwrite-safe field setter
# ---------------------------------------------------------------------------

def _set_field(row, field, new_value, row_key, report):
    """Set field with overwrite protection.

    - Non-empty incoming that differs from existing: overwrite, log to report.
    - Empty incoming: skip (preserve existing).
    - Same value: no-op.
    """
    existing = row.get(field, "")
    if not new_value:
        # Empty incoming: preserve existing
        if existing:
            report.preserved_count += 1
        return
    if existing and existing != new_value:
        # Overwrite: log the change
        report.overwrite_details.append((row_key, field, existing, new_value))
    row[field] = new_value


# ---------------------------------------------------------------------------
# Fuzzy matching (enriched register)
# ---------------------------------------------------------------------------

def _merge_er_rows(primary, secondary, display_name, postcode, report):
    """Fill gaps in primary from secondary. Log clashes.

    Only merges enrichment data columns — core identity/address fields
    (RollNo, Address1-6, SubHouse, ElectorID, UPRN, etc.) are skipped.
    """
    for key in secondary:
        if key in _ER_CORE_FIELDS:
            continue  # Never merge core electoral data
        sec_val = secondary[key].strip() if secondary[key] else ""
        if not sec_val:
            continue  # Nothing to merge
        pri_val = primary.get(key, "").strip() if primary.get(key) else ""
        if not pri_val:
            # Gap: fill from secondary
            primary[key] = secondary[key]
            report.er_merge_count += 1
        elif pri_val != sec_val:
            # Clash: keep primary, log for manual review
            report.er_merge_clashes.append(
                (display_name, postcode, key, pri_val, sec_val))


def match_enriched_register(base_rows, er_rows, threshold, report):
    """Match enriched register rows to base by fuzzy name+postcode.
    Returns dict: base_index -> er_row."""
    AMBIGUITY_MARGIN = 0.15
    POSSIBLE_THRESHOLD = 0.6
    NO_POSTCODE_THRESHOLD = 0.95

    # Build postcode index from base rows
    pc_index = defaultdict(list)  # postcode -> [(index, surname, forename, addr_str)]
    for i, row in enumerate(base_rows):
        pc = row.get("PostCode", "").strip()
        surname = row.get("Surname", "").strip()
        forename = row.get("Forename", "").strip()
        addr_str = f"{row.get('Address1', '')} {row.get('Address2', '')}".strip()
        if pc:
            pc_index[pc].append((i, surname, forename, addr_str))

    # All-rows list for no-postcode fallback
    all_base = [(i,
                 row.get("Surname", "").strip(),
                 row.get("Forename", "").strip(),
                 f"{row.get('Address1', '')} {row.get('Address2', '')}".strip())
                for i, row in enumerate(base_rows)]

    # Track which base rows have been matched (for duplicate detection)
    base_claimed = {}  # base_index -> (er_idx, er_name)

    matched = {}
    report.er_total = len(er_rows)

    for er_idx, er_row in enumerate(er_rows):
        # Extract name from ER row (try both column naming conventions)
        er_surname = er_row.get("Surname", "").strip()
        if not er_surname:
            er_surname = er_row.get("Last Name", "").strip()
        if not er_surname:
            er_surname = er_row.get("ElectorSurname", "").strip()
        er_forename = er_row.get("Forename", "").strip()
        if not er_forename:
            er_forename = er_row.get("First Name", "").strip()
        if not er_forename:
            er_forename = er_row.get("ElectorForename", "").strip()
        er_display_name = f"{er_forename} {er_surname}".strip() or "(unknown)"

        # Extract and normalize postcode
        er_postcode_raw = er_row.get("PostCode", "")
        if not er_postcode_raw:
            er_postcode_raw = er_row.get("Postcode", "")
        if not er_postcode_raw:
            er_postcode_raw = er_row.get("Post Code", "")
        if not er_postcode_raw:
            er_postcode_raw = er_row.get("POSTCODE", "")
        er_postcode = er_postcode_raw.strip().upper()
        # Normalize spacing
        pc_norm, _ = normalize_postcode(er_postcode)
        if pc_norm:
            er_postcode = pc_norm

        # Determine candidates
        if er_postcode:
            candidates = pc_index.get(er_postcode, [])
            effective_threshold = threshold
            if not candidates:
                # Postcode exists but no base rows at that postcode: fallback
                candidates = all_base
                effective_threshold = NO_POSTCODE_THRESHOLD
        else:
            candidates = all_base
            effective_threshold = NO_POSTCODE_THRESHOLD

        if not candidates:
            report.er_unmatched.append((er_postcode, er_display_name))
            continue

        # Score all candidates
        scored = []
        for base_idx, base_surname, base_forename, base_addr in candidates:
            score = _surname_forename_similarity(
                er_surname, er_forename, base_surname, base_forename)
            scored.append((score, base_idx, f"{base_forename} {base_surname}".strip()))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_idx, best_name = scored[0]

        # Check disambiguation — perfect matches (score 1.0) are always accepted
        if len(scored) > 1 and best_score < 1.0:
            second_score = scored[1][0]
            if best_score >= effective_threshold and (best_score - second_score) < AMBIGUITY_MARGIN:
                cands = [(scored[0][2], scored[0][0]), (scored[1][2], scored[1][0])]
                report.er_ambiguous.append((er_display_name, er_postcode, cands))
                continue

        if best_score >= effective_threshold:
            # Confident match — check for duplicate base claims
            if best_idx in base_claimed:
                prev_er_idx, prev_name = base_claimed[best_idx]
                # Count how many ER rows matched this base
                existing_count = 1
                for name_pc_count in report.er_duplicate_keys:
                    if name_pc_count[0] == prev_name:
                        existing_count = name_pc_count[2]
                        report.er_duplicate_keys.remove(name_pc_count)
                        break
                report.er_duplicate_keys.append(
                    (er_display_name, er_postcode, existing_count + 1))
                # Merge: fill gaps in existing match from this duplicate
                clashes_before = len(report.er_merge_clashes)
                _merge_er_rows(matched[best_idx], er_row, er_display_name, er_postcode, report)
                # Only warn if clashes were found (different non-empty values)
                if len(report.er_merge_clashes) > clashes_before:
                    report.warnings.append(
                        f"Enriched register: duplicate match \"{er_display_name}\" "
                        f"({er_postcode}) -> base \"{best_name}\" "
                        f"({existing_count + 1} occurrences, merged with clashes)")
                # Don't increment er_matched — already counted
                continue
            base_claimed[best_idx] = (er_idx, er_display_name)
            matched[best_idx] = er_row
            report.er_matched += 1
            report.er_confident_matches.append(
                (er_display_name, best_name, er_postcode, best_score))
        elif best_score >= POSSIBLE_THRESHOLD:
            report.er_possible.append(
                (er_display_name, er_postcode, best_score, best_name))
        else:
            report.er_unmatched.append((er_postcode, er_display_name))

    return matched


# ---------------------------------------------------------------------------
# Fuzzy matching (canvassing export)
# ---------------------------------------------------------------------------

def _append_unmatched_export(report, ce_row, category, score=None,
                             base_row=None, best_name=None,
                             second_base_row=None, second_name=None,
                             second_score=None):
    """Build and append a row to report.ce_unmatched_rows for CSV export."""
    export = {"Match Category": category}
    export["Match Score"] = f"{score:.4f}" if score is not None else ""
    if base_row:
        export["Best Candidate Elector No."] = base_row.get("Full Elector No.", "")
        export["Best Candidate Name"] = best_name or ""
        export["Best Candidate Address"] = (
            f"{base_row.get('Address1', '')} {base_row.get('Address2', '')}".strip())
        export["Best Candidate PostCode"] = base_row.get("PostCode", "")
    else:
        export["Best Candidate Elector No."] = ""
        export["Best Candidate Name"] = ""
        export["Best Candidate Address"] = ""
        export["Best Candidate PostCode"] = ""
    if second_base_row:
        export["2nd Candidate Elector No."] = second_base_row.get("Full Elector No.", "")
        export["2nd Candidate Name"] = second_name or ""
        export["2nd Candidate Score"] = f"{second_score:.4f}" if second_score is not None else ""
    else:
        export["2nd Candidate Elector No."] = ""
        export["2nd Candidate Name"] = ""
        export["2nd Candidate Score"] = ""
    export.update(ce_row)
    report.ce_unmatched_rows.append(export)


_UNMATCHED_HELPER_COLS = [
    "Match Category", "Match Score",
    "Best Candidate Elector No.", "Best Candidate Name",
    "Best Candidate Address", "Best Candidate PostCode",
    "2nd Candidate Elector No.", "2nd Candidate Name", "2nd Candidate Score",
]


def _write_unmatched_csv(unmatched_rows, output_path, ce_headers):
    """Write unmatched/possible/ambiguous canvassing rows to CSV."""
    headers = _UNMATCHED_HELPER_COLS + list(ce_headers)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n",
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(unmatched_rows)


def match_canvassing_export(base_rows, ce_rows, threshold, report):
    """Match canvassing rows to base using fuzzy matching.
    Returns dict: base_index -> ce_row."""
    AMBIGUITY_MARGIN = 0.15
    POSSIBLE_THRESHOLD = 0.6
    NO_POSTCODE_THRESHOLD = 0.95

    # Build postcode index for base rows
    pc_index = defaultdict(list)  # postcode -> [(index, full_name, addr_str)]
    for i, row in enumerate(base_rows):
        pc = row.get("PostCode", "").strip()
        full_name = f"{row.get('Forename', '')} {row.get('Surname', '')}".strip()
        addr_str = f"{row.get('Address1', '')} {row.get('Address2', '')}".strip()
        if pc:
            pc_index[pc].append((i, full_name, addr_str))

    # Also build an all-rows list for no-postcode fallback
    all_base = [(i,
                 f"{row.get('Forename', '')} {row.get('Surname', '')}".strip(),
                 f"{row.get('Address1', '')} {row.get('Address2', '')}".strip())
                for i, row in enumerate(base_rows)]

    # Track which base rows have been matched (for duplicate detection)
    base_match_count = defaultdict(list)  # base_index -> [ce_row_index]

    matched = {}
    report.ce_total = len(ce_rows)

    for ce_idx, ce_row in enumerate(ce_rows):
        profile_name = ce_row.get("profile_name", "").strip()
        ce_addr = f"{ce_row.get('address 1', '')} {ce_row.get('address 2', '')}".strip()
        ce_postcode = _extract_postcode(ce_row)

        if ce_postcode:
            candidates = pc_index.get(ce_postcode, [])
            effective_threshold = threshold
        else:
            # No postcode: search all, use higher threshold
            candidates = all_base
            effective_threshold = NO_POSTCODE_THRESHOLD
            if not candidates:
                report.ce_unmatched.append((profile_name, ce_addr, None))
                _append_unmatched_export(report, ce_row, "unmatched")
                continue

        if not candidates:
            report.ce_unmatched.append((profile_name, ce_addr, None))
            _append_unmatched_export(report, ce_row, "unmatched")
            continue

        # Score all candidates
        scored = []
        for base_idx, base_name, base_addr in candidates:
            name_sim = _name_similarity(profile_name, base_name)
            addr_sim = _address_similarity(ce_addr, base_addr)
            score = 0.5 * name_sim + 0.5 * addr_sim
            scored.append((score, base_idx, base_name))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_idx, best_name = scored[0]

        # Check disambiguation — perfect matches (score 1.0) are always accepted
        if len(scored) > 1 and best_score < 1.0:
            second_score = scored[1][0]
            if best_score >= effective_threshold and (best_score - second_score) < AMBIGUITY_MARGIN:
                # Ambiguous
                cands = [(scored[0][2], scored[0][0]), (scored[1][2], scored[1][0])]
                report.ce_ambiguous.append((profile_name, ce_addr, cands))
                base_row_1 = base_rows[scored[0][1]]
                base_row_2 = base_rows[scored[1][1]]
                _append_unmatched_export(
                    report, ce_row, "ambiguous",
                    score=scored[0][0], base_row=base_row_1,
                    best_name=scored[0][2],
                    second_base_row=base_row_2,
                    second_name=scored[1][2],
                    second_score=scored[1][0])
                continue

        if best_score >= effective_threshold:
            # Confident match
            base_match_count[best_idx].append(ce_idx)
            matched[best_idx] = ce_row
            report.ce_confident += 1
        elif best_score >= POSSIBLE_THRESHOLD:
            # Possible match — report only
            report.ce_possible.append((profile_name, ce_addr, best_score, best_name))
            _append_unmatched_export(
                report, ce_row, "possible",
                score=best_score, base_row=base_rows[best_idx],
                best_name=best_name)
        else:
            report.ce_unmatched.append((profile_name, ce_addr, best_score if scored else None))
            _append_unmatched_export(
                report, ce_row, "unmatched",
                score=best_score if scored else None,
                base_row=base_rows[best_idx] if scored else None,
                best_name=best_name if scored else None)

    # Check for duplicate canvassing visits (multiple ce rows matching same base)
    for base_idx, ce_indices in base_match_count.items():
        if len(ce_indices) > 1:
            # Take the last one (most recent visit)
            last_ce_idx = ce_indices[-1]
            matched[base_idx] = ce_rows[last_ce_idx]
            base_name = f"{base_rows[base_idx].get('Forename', '')} {base_rows[base_idx].get('Surname', '')}".strip()
            report.ce_duplicate_visits.append((base_name, len(ce_indices)))
            report.warnings.append(
                f"Canvassing: {len(ce_indices)} visits matched base row \"{base_name}\", last used")

    return matched


# ---------------------------------------------------------------------------
# Election column generation
# ---------------------------------------------------------------------------

def generate_election_columns(row, base_idx, er_match, ce_match,
                              historic_elections, future_elections, report):
    """Generate election columns for a single row."""
    prefix = row.get("Elector No. Prefix", "")
    number = row.get("Elector No.", "")
    row_key = f"{prefix}-{number}"

    for election in historic_elections:
        voted_key = f"{election} Voted"
        party_key = f"{election} Party"
        gvi_key = f"{election} Green Voting Intention"

        er_voted = ""
        er_party = ""
        ce_party = ""

        if er_match:
            # GE24 column -> voted
            ge24_val = er_match.get("GE24", "").strip()
            if ge24_val and ge24_val.upper() not in ("N", "NO"):
                er_voted = "Y"
            # Party from enriched register
            er_party_raw = er_match.get("Party", "").strip()
            er_party = map_party_name(er_party_raw, report, "enriched_register")

        if ce_match:
            ce_party_raw = ce_match.get("visit_previously_voted_for", "").strip()
            ce_party = map_party_name(ce_party_raw, report, "canvassing")

        # Resolve party: enriched register wins
        party = er_party
        if er_party and ce_party and er_party != ce_party:
            report.conflicts.append((row_key, party_key, er_party, ce_party, er_party))
            party = er_party
        elif not er_party and ce_party:
            party = ce_party

        _set_field(row, voted_key, er_voted, row_key, report)
        _set_field(row, party_key, party, row_key, report)
        # GVI for historic elections is left empty — inferring voting intention
        # from party would be an assumption, not actual canvassing data
        _set_field(row, gvi_key, "", row_key, report)

    for election in future_elections:
        postal_key = f"{election} Postal Voter"
        party_key = f"{election} Party"
        gvi_key = f"{election} Green Voting Intention"

        postal = ""
        if er_match:
            pv = _get_postal_voter(er_match)
            if not pv:
                pv = er_match.get("P/PB", "").strip()
            # Treat explicit "N"/"No" as blank (no postal vote)
            if pv and pv.upper() not in ("N", "NO"):
                postal = "Y"
                report.questions_data["PostalVoter"] = True

        if not postal and ce_match:
            visit_postal = ce_match.get("visit_postal_vote", "").strip()
            if visit_postal and visit_postal.upper() not in ("FALSE", "N", "NO", ""):
                postal = "Y"

        _set_field(row, postal_key, postal, row_key, report)
        _set_field(row, party_key, "", row_key, report)
        _set_field(row, gvi_key, "", row_key, report)


# ---------------------------------------------------------------------------
# Extra columns
# ---------------------------------------------------------------------------

def add_extra_columns(row, er_match, ce_match, report):
    """Add non-TTW extra columns from both sources."""
    prefix = row.get("Elector No. Prefix", "")
    number = row.get("Elector No.", "")
    row_key = f"{prefix}-{number}"

    if er_match:
        for col in EXTRA_COLS_REGISTER:
            val = er_match.get(col, "").strip()
            _set_field(row, col, val, row_key, report)
            # Track questions
            if col == "DNK" and val:
                report.questions_data["DNK"] = True
            if col == "New" and val:
                report.questions_data["New"] = True
            if col == "1st round" and val:
                report.questions_data["1st round"] = True
            if col == "1-5" and er_match.get("1-5", "").strip():
                report.questions_data["1-5"] = True
    else:
        for col in EXTRA_COLS_REGISTER:
            if col not in row:
                row[col] = ""

    if ce_match:
        for col in EXTRA_COLS_CANVASSING:
            val = ce_match.get(col, "").strip()
            _set_field(row, col, val, row_key, report)
        # Optional DNK from canvassing export (only if column exists in CE CSV)
        if report.ce_has_dnk:
            dnk_val = ce_match.get("DNK", "").strip()
            _set_field(row, "DNK", dnk_val, row_key, report)
            if dnk_val:
                report.questions_data["DNK"] = True
    else:
        for col in EXTRA_COLS_CANVASSING:
            if col not in row:
                row[col] = ""
        if report.ce_has_dnk and "DNK" not in row:
            row["DNK"] = ""


# ---------------------------------------------------------------------------
# Output header construction
# ---------------------------------------------------------------------------

def build_enrichment_headers(base_headers, historic_elections, future_elections,
                             has_er, has_ce, strip_extra, report=None, has_cr=False,
                             ce_has_dnk=False):
    """Build output header list preserving base order, appending election + extra cols.

    Deduplicates: skips columns that already exist in base_headers.
    """
    headers = list(base_headers)
    base_set = set(base_headers)

    def _add_col(col):
        if col in base_set:
            if report:
                report.existing_columns_updated.append(col)
            return  # Skip duplicate
        headers.append(col)
        base_set.add(col)
        if report:
            report.new_columns_created.append(col)

    # Election columns
    for election in historic_elections:
        _add_col(f"{election} Green Voting Intention")
        _add_col(f"{election} Party")
        _add_col(f"{election} Voted")

    for election in future_elections:
        _add_col(f"{election} Green Voting Intention")
        _add_col(f"{election} Party")
        _add_col(f"{election} Postal Voter")

    # Extra columns (unless stripped)
    if not strip_extra:
        if has_er:
            for col in EXTRA_COLS_REGISTER:
                _add_col(col)
        if has_ce:
            for col in EXTRA_COLS_CANVASSING:
                _add_col(col)
        if has_cr:
            for col in EXTRA_COLS_CANVASSING_REGISTER:
                _add_col(col)
        # DNK from CE (conditional — only when CE CSV has the column)
        if ce_has_dnk and not has_er:  # ER already adds DNK via EXTRA_COLS_REGISTER
            _add_col("DNK")

    return headers


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def validate_base_is_ttw(headers):
    """Check that the base file is in TTW format (not council format)."""
    required = {"Elector No. Prefix", "Elector No.", "Forename", "Surname"}
    header_set = set(headers)
    missing = required - header_set
    if missing:
        # Check if it looks like council format
        council_indicators = {"PDCode", "RollNo", "ElectorForename", "ElectorSurname"}
        if header_set & council_indicators:
            print("ERROR: Base file appears to be in council format, not TTW format.",
                  file=sys.stderr)
            print("Run clean_register.py first to convert to TTW format.", file=sys.stderr)
        else:
            print(f"ERROR: Base file missing required TTW columns: {missing}",
                  file=sys.stderr)
        sys.exit(1)


def validate_enriched_register(headers):
    """Check enriched register has required columns.

    Requires PostCode (case-insensitive) plus at least one forename column
    and at least one surname column.
    """
    header_set = set(headers)
    header_lower = {h.lower(): h for h in headers}

    # Check PostCode (case-insensitive variants)
    has_postcode = any(h.lower().replace(" ", "") == "postcode" for h in headers)
    if not has_postcode:
        print("ERROR: Enriched register missing required column: PostCode",
              file=sys.stderr)
        print(f"Found columns: {headers}", file=sys.stderr)
        sys.exit(1)

    # Check name columns (accept council-format ElectorForename/ElectorSurname too)
    has_forename = bool(header_set & {"Forename", "First Name", "ElectorForename"})
    has_surname = bool(header_set & {"Surname", "Last Name", "ElectorSurname"})
    missing = []
    if not has_forename:
        missing.append("Forename, First Name, or ElectorForename")
    if not has_surname:
        missing.append("Surname, Last Name, or ElectorSurname")
    if missing:
        print(f"ERROR: Enriched register missing required columns: {missing}",
              file=sys.stderr)
        print(f"Found columns: {headers}", file=sys.stderr)
        sys.exit(1)


def validate_canvassing_export(headers):
    """Check canvassing export has required columns."""
    required = {"profile_name", "address 1"}
    header_set = set(headers)
    missing = required - header_set
    if missing:
        print(f"ERROR: Canvassing export missing required columns: {missing}",
              file=sys.stderr)
        print(f"Found columns: {headers}", file=sys.stderr)
        sys.exit(1)


def validate_canvassing_register(headers):
    """Check canvassing register has required columns.

    Accepts ElectorSurname/ElectorForename as valid name columns
    (broader than validate_enriched_register).
    """
    # Check PostCode (case-insensitive variants)
    has_postcode = any(h.lower().replace(" ", "") == "postcode" for h in headers)
    if not has_postcode:
        print("ERROR: Canvassing register missing required column: PostCode",
              file=sys.stderr)
        sys.exit(1)

    # Check name columns (broader set than validate_enriched_register)
    header_set = set(headers)
    has_forename = bool(header_set & {"Forename", "First Name", "ElectorForename"})
    has_surname = bool(header_set & {"Surname", "Last Name", "ElectorSurname"})
    missing = []
    if not has_forename:
        missing.append("Forename, First Name, or ElectorForename")
    if not has_surname:
        missing.append("Surname, Last Name, or ElectorSurname")
    if missing:
        print(f"ERROR: Canvassing register missing required columns: {missing}",
              file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Canvassing register column generation
# ---------------------------------------------------------------------------

def generate_canvassing_register_columns(row, cr_match, future_elections, report):
    """Map canvassing register data to future election columns."""
    row_key = f"{row.get('Elector No. Prefix', '')}-{row.get('Elector No.', '')}"

    if not cr_match:
        return

    for election in future_elections:
        # Party → {election} Party (mapped to TTW code)
        party_raw = cr_match.get("Party", "").strip()
        party = map_party_name(party_raw, report, "canvassing_register")
        _set_field(row, f"{election} Party", party, row_key, report)

        # 1-5 → {election} Green Voting Intention
        gvi_raw = cr_match.get("1-5", "").strip()
        if gvi_raw in {"1", "2", "3", "4", "5"}:
            _set_field(row, f"{election} Green Voting Intention", gvi_raw,
                       row_key, report)
        elif gvi_raw:
            report.warnings.append(
                f"Canvassing register row {row_key}: invalid 1-5 value "
                f"'{gvi_raw}', skipped")

    # Comments → Comments extra column
    comments = cr_match.get("Comments", "").strip()
    _set_field(row, "Comments", comments, row_key, report)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Enrich a cleaned TTW electoral register with canvassing and register data."
    )
    parser.add_argument("base", help="Base TTW CSV (output of clean_register.py)")
    parser.add_argument("output", help="Output enriched CSV")
    parser.add_argument("--enriched-register", default=None,
                        help="Enriched register CSV (Spreadsheet 2)")
    parser.add_argument("--canvassing-export", default=None,
                        help="Canvassing export CSV (Spreadsheet 1)")
    parser.add_argument("--canvassing-register", default=None,
                        help="Canvassing register CSV (register-format with "
                        "future election canvassing data: Party, 1-5, Comments)")
    parser.add_argument("--historic-elections", nargs="*", default=[],
                        help="Historic election names (e.g. GE2024)")
    parser.add_argument("--future-elections", nargs="*", default=[],
                        help="Future election names (e.g. 2026)")
    parser.add_argument("--strip-extra", action="store_true",
                        help="Remove non-TTW columns for upload-ready output")
    parser.add_argument("--report", default=None,
                        help="QA report path (default: OUTPUT.report.txt)")
    parser.add_argument("--match-threshold", type=float, default=0.8,
                        help="Fuzzy match confidence threshold (default: 0.8)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate only the QA report without writing output CSV")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress stdout progress messages")
    args = parser.parse_args()

    # Validate: at least one source required
    if not args.enriched_register and not args.canvassing_export and not args.canvassing_register:
        print("ERROR: At least one of --enriched-register, --canvassing-export, "
              "or --canvassing-register is required.", file=sys.stderr)
        sys.exit(1)

    # --canvassing-register requires --future-elections
    if args.canvassing_register and not args.future_elections:
        print("ERROR: --future-elections is required when using --canvassing-register "
              "(canvassing data maps to future election columns).", file=sys.stderr)
        sys.exit(1)

    # Overwrite protection
    base_path = Path(args.base).resolve()
    output_path = Path(args.output).resolve()
    if base_path == output_path:
        print("ERROR: Output path must be different from base path.", file=sys.stderr)
        sys.exit(1)

    report = EnrichQAReport()
    report.base_file = args.base
    report.output_file = args.output
    report_path = args.report or f"{args.output}.report.txt"

    # --- Read base ---
    if not args.quiet:
        print(f"Reading base: {args.base}...")
    base_rows, base_enc, base_headers = read_input(args.base)
    validate_base_is_ttw(base_headers)
    report.base_rows = len(base_rows)

    # --- Read enriched register ---
    er_rows = None
    er_match_map = {}
    if args.enriched_register:
        report.enriched_register_file = args.enriched_register
        if not args.quiet:
            print(f"Reading enriched register: {args.enriched_register}...")
        er_rows, _, er_headers = read_input(args.enriched_register)
        validate_enriched_register(er_headers)
        er_match_map = match_enriched_register(base_rows, er_rows,
                                                args.match_threshold, report)

    # --- Read canvassing export ---
    ce_rows = None
    ce_match_map = {}
    if args.canvassing_export:
        report.canvassing_export_file = args.canvassing_export
        if not args.quiet:
            print(f"Reading canvassing export: {args.canvassing_export}...")
        ce_rows, _, ce_headers = read_input(args.canvassing_export)
        validate_canvassing_export(ce_headers)
        report.ce_headers = ce_headers
        report.ce_has_dnk = "DNK" in ce_headers
        ce_match_map = match_canvassing_export(
            base_rows, ce_rows, args.match_threshold, report)

    # --- Read canvassing register ---
    cr_match_map = {}
    if args.canvassing_register:
        report.canvassing_register_file = args.canvassing_register
        if not args.quiet:
            print(f"Reading canvassing register: {args.canvassing_register}...")
        cr_rows, _, cr_headers = read_input(args.canvassing_register)
        validate_canvassing_register(cr_headers)
        # Use temporary report to avoid conflating ER and CR stats
        temp_report = EnrichQAReport()
        cr_match_map = match_enriched_register(
            base_rows, cr_rows, args.match_threshold, temp_report)
        # Copy stats from temp report to main report cr_* fields
        report.cr_total = temp_report.er_total
        report.cr_matched = temp_report.er_matched
        report.cr_unmatched = temp_report.er_unmatched
        report.cr_confident_matches = temp_report.er_confident_matches
        report.cr_possible = temp_report.er_possible
        report.cr_ambiguous = temp_report.er_ambiguous
        # Merge warnings and unrecognized parties
        report.warnings.extend(temp_report.warnings)
        report.unrecognized_parties.extend(temp_report.unrecognized_parties)

    # --- Enrich each base row ---
    if not args.quiet:
        print("Enriching rows...")
    output_rows = []
    for i, row in enumerate(base_rows):
        enriched = dict(row)  # Copy base row (read-only principle)
        er_match = er_match_map.get(i)
        ce_match = ce_match_map.get(i)

        generate_election_columns(enriched, i, er_match, ce_match,
                                  args.historic_elections, args.future_elections,
                                  report)
        add_extra_columns(enriched, er_match, ce_match, report)

        # Apply canvassing register data (after other enrichment, so CR overwrites)
        cr_match = cr_match_map.get(i)
        generate_canvassing_register_columns(
            enriched, cr_match, args.future_elections, report)

        output_rows.append(enriched)

    # --- Row count assertion ---
    assert len(output_rows) == len(base_rows), \
        f"Row count mismatch: {len(output_rows)} output vs {len(base_rows)} base"
    report.output_rows = len(output_rows)

    # --- Build headers ---
    output_headers = build_enrichment_headers(
        base_headers,
        args.historic_elections, args.future_elections,
        has_er=bool(args.enriched_register),
        has_ce=bool(args.canvassing_export),
        strip_extra=args.strip_extra,
        report=report,
        has_cr=bool(args.canvassing_register),
        ce_has_dnk=report.ce_has_dnk,
    )

    # --- Write output (unless dry-run) ---
    if not args.dry_run:
        if not args.quiet:
            print(f"Writing output: {args.output}...")
        write_output(output_rows, output_headers, args.output)

        # Write unmatched canvassing rows CSV if any
        if report.ce_unmatched_rows:
            output_p = Path(args.output)
            unmatched_path = str(output_p.parent / f"{output_p.stem}.unmatched.csv")
            _write_unmatched_csv(report.ce_unmatched_rows, unmatched_path,
                                 report.ce_headers)
            report.unmatched_csv_path = unmatched_path
            if not args.quiet:
                print(f"Unmatched canvassing rows: {unmatched_path}")

    # --- Write report ---
    report.write(report_path)

    # --- Console summary ---
    if not args.quiet:
        print(f"\nBase rows:   {report.base_rows}")
        print(f"Output rows: {report.output_rows}")
        if args.enriched_register:
            print(f"Enriched register: {report.er_matched}/{report.er_total} matched")
        if args.canvassing_export:
            print(f"Canvassing: {report.ce_confident} confident, "
                  f"{len(report.ce_possible)} possible, "
                  f"{len(report.ce_ambiguous)} ambiguous, "
                  f"{len(report.ce_unmatched)} unmatched")
        if args.canvassing_register:
            print(f"Canvassing register: {report.cr_matched}/{report.cr_total} matched")
        if args.dry_run:
            print("(dry-run: no output CSV written)")
        print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
