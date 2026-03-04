#!/usr/bin/env python3
"""Enrich a cleaned TTW electoral register CSV with canvassing and register data.

Usage:
    python3 tools/enrich_register.py BASE_TTW.csv OUTPUT.csv \\
        --enriched-register SPREADSHEET2.csv \\
        --canvassing-export SPREADSHEET1.csv \\
        --historic-elections GE2024 \\
        --future-elections 2026 \\
        [--strip-extra] [--report PATH] [--match-threshold 0.8] [--dry-run] [--quiet]
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Import from sibling module
sys.path.insert(0, str(Path(__file__).parent))
from clean_register import (read_input, write_output, normalize_postcode,
                            UK_POSTCODE_RE, VALID_PARTY_CODES)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Party name -> TTW code mapping (case-insensitive, underscores -> spaces)
PARTY_NAME_MAP = {
    "green party": "G",
    "green": "G",
    "greens": "G",
    "labour": "Lab",
    "conservatives": "Con",
    "conservative": "Con",
    "tory": "Con",
    "tories": "Con",
    "liberal democrats": "LD",
    "liberal democrat": "LD",
    "lib dem": "LD",
    "lib dems": "LD",
    "reform": "REF",
    "reform uk": "REF",
    "reform party": "REF",
    "independent": "Ind",
}

# Values that map to blank (not a party)
PARTY_BLANK_VALUES = {
    "did not vote", "none", "refused to say", "won't say",
    "dont know", "don't know", "no answer",
}

# GVI derivation from party code
PARTY_TO_GVI = {"G": "1", "Con": "2", "Lab": "3", "LD": "4"}

# Extra columns from enriched register (non-TTW)
EXTRA_COLS_REGISTER = [
    "Email Address", "Phone number", "Comments", "Issues",
    "DNK", "New", "1st round",
]

# Extra columns from canvassing export (non-TTW)
EXTRA_COLS_CANVASSING = ["visit_issues", "visit_notes"]


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
        self.er_unmatched = []      # [(pdcode, rollno, name)]
        self.er_duplicate_keys = [] # [(pdcode, rollno, count)]

        # Canvassing export matching
        self.ce_total = 0
        self.ce_confident = 0
        self.ce_possible = []       # [(profile_name, addr, score, candidate_name)]
        self.ce_ambiguous = []      # [(profile_name, addr, [(name, score)])]
        self.ce_unmatched = []      # [(profile_name, addr, best_score)]
        self.ce_duplicate_visits = [] # [(base_key, count)]

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
        lines.append("")

        # --- Enriched Register Matching ---
        if self.enriched_register_file:
            lines.append("--- Enriched Register Matching ---")
            lines.append(f"Total rows: {self.er_total}")
            lines.append(f"Matched: {self.er_matched}")
            lines.append(f"Unmatched: {len(self.er_unmatched)}")
            lines.append(f"Duplicate keys: {len(self.er_duplicate_keys)}")
            if self.er_unmatched:
                lines.append("  Unmatched rows:")
                for pdcode, rollno, name in self.er_unmatched:
                    lines.append(f"    {pdcode}-{rollno} ({name})")
            if self.er_duplicate_keys:
                lines.append("  Duplicate keys:")
                for pdcode, rollno, count in self.er_duplicate_keys:
                    lines.append(f"    {pdcode}-{rollno}: {count} occurrences")
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

        # --- Machine-readable footer ---
        lines.append("### MACHINE-READABLE SECTION ###")
        for row_key, field, er_val, ce_val, resolved in self.conflicts:
            lines.append(f"CONFLICT|Row={row_key}|Field={field}"
                         f"|EnrichedRegister={er_val}|Canvassing={ce_val}"
                         f"|Resolved={resolved}")
        for w in self.warnings:
            lines.append(f"WARNING|{w}")
        if self.enriched_register_file:
            for pdcode, rollno, name in self.er_unmatched:
                lines.append(f"MATCH|Source=enriched_register|Status=unmatched"
                             f"|Key={pdcode}-{rollno}|Name={name}")
        if self.canvassing_export_file:
            for profile_name, addr, score, candidate in self.ce_possible:
                lines.append(f"MATCH|Source=canvassing|Status=possible"
                             f"|Name={profile_name}|Score={score:.3f}"
                             f"|Candidate={candidate}")
        lines.append("### END MACHINE-READABLE SECTION ###")

        Path(path).write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Party name mapping
# ---------------------------------------------------------------------------

def map_party_name(value, report, source="unknown"):
    """Map a party name to TTW code. Returns mapped value."""
    if not value or not value.strip():
        return ""

    raw = value.strip()

    # Already a valid TTW code? Passthrough.
    if raw in VALID_PARTY_CODES:
        return raw

    normalized = raw.replace("_", " ").lower().strip()

    if normalized in PARTY_BLANK_VALUES:
        return ""

    if normalized in PARTY_NAME_MAP:
        return PARTY_NAME_MAP[normalized]

    # Unrecognized — keep as-is, warn
    report.unrecognized_parties.append((source, raw))
    return raw


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


def _address_similarity(addr_a, addr_b):
    """Compare two address strings using Dice coefficient."""
    return _dice_coefficient(addr_a or "", addr_b or "")


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
# Exact matching (enriched register)
# ---------------------------------------------------------------------------

def match_enriched_register(base_rows, er_rows, report):
    """Match enriched register rows to base by PDCode+RollNo.
    Returns dict: base_index -> er_row."""
    # Build base lookup: (prefix, number) -> index
    base_lookup = {}
    for i, row in enumerate(base_rows):
        key = (row.get("Elector No. Prefix", "").strip(),
               row.get("Elector No.", "").strip())
        base_lookup[key] = i

    # Build ER lookup, handling duplicates
    er_by_key = {}
    er_key_counts = defaultdict(int)
    for row in er_rows:
        pdcode = row.get("PDCode", "").strip()
        rollno = row.get("RollNo", "").strip()
        key = (pdcode, rollno)
        er_key_counts[key] += 1
        if key not in er_by_key:
            er_by_key[key] = row

    # Log duplicates
    for key, count in er_key_counts.items():
        if count > 1:
            report.er_duplicate_keys.append((key[0], key[1], count))
            report.warnings.append(
                f"Enriched register: duplicate key {key[0]}-{key[1]} ({count} occurrences, first used)")

    # Match
    matched = {}
    for key, er_row in er_by_key.items():
        if key in base_lookup:
            matched[base_lookup[key]] = er_row
            report.er_matched += 1
        else:
            name = f"{er_row.get('Forename', '')} {er_row.get('Surname', '')}".strip()
            if not name:
                name = f"{er_row.get('First Name', '')} {er_row.get('Last Name', '')}".strip()
            report.er_unmatched.append((key[0], key[1], name or "(unknown)"))

    report.er_total = len(er_by_key)
    return matched


# ---------------------------------------------------------------------------
# Fuzzy matching (canvassing export)
# ---------------------------------------------------------------------------

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
                continue

        if not candidates:
            report.ce_unmatched.append((profile_name, ce_addr, None))
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

        # Check disambiguation
        if len(scored) > 1:
            second_score = scored[1][0]
            if best_score >= effective_threshold and (best_score - second_score) < AMBIGUITY_MARGIN:
                # Ambiguous
                cands = [(scored[0][2], scored[0][0]), (scored[1][2], scored[1][0])]
                report.ce_ambiguous.append((profile_name, ce_addr, cands))
                continue

        if best_score >= effective_threshold:
            # Confident match
            base_match_count[best_idx].append(ce_idx)
            matched[best_idx] = ce_row
            report.ce_confident += 1
        elif best_score >= POSSIBLE_THRESHOLD:
            # Possible match — report only
            report.ce_possible.append((profile_name, ce_addr, best_score, best_name))
        else:
            report.ce_unmatched.append((profile_name, ce_addr, best_score if scored else None))

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
            if ge24_val:
                er_voted = "v"
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

        row[voted_key] = er_voted
        row[party_key] = party
        # Derive GVI from party code
        if party in PARTY_TO_GVI:
            row[gvi_key] = PARTY_TO_GVI[party]
        elif party and party not in ("", "Ind", "REF"):
            row[gvi_key] = "5"  # Other
        else:
            row[gvi_key] = ""

    for election in future_elections:
        postal_key = f"{election} Postal Voter"
        party_key = f"{election} Party"
        gvi_key = f"{election} Green Voting Intention"

        postal = ""
        if er_match:
            pv = er_match.get("PostalVoter?", "").strip()
            if not pv:
                pv = er_match.get("P/PB", "").strip()
            if pv:
                postal = "v"
                report.questions_data["PostalVoter"] = True

        if not postal and ce_match:
            visit_postal = ce_match.get("visit_postal_vote", "").strip()
            if visit_postal and visit_postal.upper() not in ("FALSE", ""):
                postal = "v"

        row[postal_key] = postal
        row[party_key] = ""  # Uncertain
        row[gvi_key] = ""    # Uncertain


# ---------------------------------------------------------------------------
# Extra columns
# ---------------------------------------------------------------------------

def add_extra_columns(row, er_match, ce_match, report):
    """Add non-TTW extra columns from both sources."""
    if er_match:
        for col in EXTRA_COLS_REGISTER:
            val = er_match.get(col, "").strip()
            row[col] = val
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
            row[col] = ""

    if ce_match:
        for col in EXTRA_COLS_CANVASSING:
            row[col] = ce_match.get(col, "").strip()
    else:
        for col in EXTRA_COLS_CANVASSING:
            row[col] = ""


# ---------------------------------------------------------------------------
# Output header construction
# ---------------------------------------------------------------------------

def build_enrichment_headers(base_headers, historic_elections, future_elections,
                             has_er, has_ce, strip_extra):
    """Build output header list preserving base order, appending election + extra cols."""
    headers = list(base_headers)

    # Election columns
    for election in historic_elections:
        headers.append(f"{election} Green Voting Intention")
        headers.append(f"{election} Party")
        headers.append(f"{election} Voted")

    for election in future_elections:
        headers.append(f"{election} Green Voting Intention")
        headers.append(f"{election} Party")
        headers.append(f"{election} Postal Voter")

    # Extra columns (unless stripped)
    if not strip_extra:
        if has_er:
            headers.extend(EXTRA_COLS_REGISTER)
        if has_ce:
            headers.extend(EXTRA_COLS_CANVASSING)

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
    """Check enriched register has required columns."""
    required = {"PDCode", "RollNo"}
    header_set = set(headers)
    missing = required - header_set
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
    if not args.enriched_register and not args.canvassing_export:
        print("ERROR: At least one of --enriched-register or --canvassing-export is required.",
              file=sys.stderr)
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
        er_match_map = match_enriched_register(base_rows, er_rows, report)

    # --- Read canvassing export ---
    ce_rows = None
    ce_match_map = {}
    if args.canvassing_export:
        report.canvassing_export_file = args.canvassing_export
        if not args.quiet:
            print(f"Reading canvassing export: {args.canvassing_export}...")
        ce_rows, _, ce_headers = read_input(args.canvassing_export)
        validate_canvassing_export(ce_headers)
        ce_match_map = match_canvassing_export(
            base_rows, ce_rows, args.match_threshold, report)

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
    )

    # --- Write output (unless dry-run) ---
    if not args.dry_run:
        if not args.quiet:
            print(f"Writing output: {args.output}...")
        write_output(output_rows, output_headers, args.output)

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
        if args.dry_run:
            print("(dry-run: no output CSV written)")
        print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
