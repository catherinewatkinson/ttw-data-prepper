#!/usr/bin/env python3
"""Transform council electoral register CSV to TTW Digital upload format.

Usage:
    python3 tools/clean_register.py INPUT OUTPUT [OPTIONS]

Examples:
    # Register only (default)
    python3 tools/clean_register.py council_data.csv cleaned.csv

    # Register + election data
    python3 tools/clean_register.py council_data.csv cleaned.csv \\
        --mode register+elections \\
        --elections 2022 2026 \\
        --election-types historic future

    # Combined council+enrichment data (single file with GE24/Party/1-5 columns)
    # GE24 is historic (-> Voted), Party/1-5 are current (-> future election)
    # Suffix is auto-detected: decimal RollNos are normalized to sequential suffixes
    python3 tools/clean_register.py combined.csv output.csv \\
        --mode register+elections \\
        --elections GE2024 LE2026 \\
        --election-types historic future \\
        --enriched-columns

    # Upload-ready (strip extra columns like Email, Phone, DNK, etc.)
    python3 tools/clean_register.py combined.csv upload.csv \\
        --mode register+elections \\
        --elections GE2024 LE2026 \\
        --election-types historic future \\
        --enriched-columns \\
        --strip-extra
"""

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

# Import shared utilities
sys.path.insert(0, str(Path(__file__).parent))
from ttw_common import (read_input, write_output, normalize_postcode,
                        UK_POSTCODE_RE, VALID_PARTY_CODES,
                        PARTY_NAME_MAP, PARTY_BLANK_VALUES, map_party_name)
from enrich_register import _surname_forename_similarity, _address_similarity

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# TTW output column order — matches the supplied TTW test data exactly
# Note: Surname before Forename (matches test data, not template)
TTW_REGISTER_HEADERS = [
    "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
    "Surname", "Forename", "Middle Names",
    "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
    "PostCode", "UPRN",
]

# Date of Attainment is placed after Middle Names when present in input data
# It is NOT in the TTW test data, so it is only included if input has date values
DOA_INSERT_POSITION = 7  # After "Middle Names", before "Address1"

# Optional columns that can be removed with --strip-empty
OPTIONAL_COLUMNS = {
    "Date of Attainment", "Middle Names",
    "Address3", "Address4", "Address5", "Address6", "UPRN",
}

# Required columns in council input
REQUIRED_COUNCIL_COLUMNS = [
    "PDCode", "RollNo", "ElectorForename", "ElectorSurname",
    "RegisteredAddress1", "PostCode",
]

# Council → TTW field mapping
FIELD_MAP = {
    "PDCode": "Elector No. Prefix",
    "RollNo": "Elector No.",
    "ElectorForename": "Forename",
    "ElectorMiddleName": "Middle Names",
    "ElectorSurname": "Surname",
    "DateOfAttainment": "Date of Attainment",
    "RegisteredAddress1": "Address1",
    "RegisteredAddress2": "Address2",
    "RegisteredAddress3": "Address3",
    "RegisteredAddress4": "Address4",
    "RegisteredAddress5": "Address5",
    "RegisteredAddress6": "Address6",
    "PostCode": "PostCode",
    "UPRN": "UPRN",
}
# Reverse lookup: TTW output name -> council source name
_FIELD_MAP_REVERSE = {v: k for k, v in FIELD_MAP.items()}


def _norm_col(name):
    """Normalize column name for alias matching: lowercase, strip spaces/underscores/hyphens/dots."""
    return re.sub(r'[\s_\-\.]+', '', name).lower()


# Column aliases: normalized_alias -> canonical council column name
# Maps TTW output names and common variants to the council column names
# expected by FIELD_MAP and REQUIRED_COUNCIL_COLUMNS.
COLUMN_ALIASES = {}
_alias_entries = {
    "PDCode": ["pdcode", "pd", "pollingdistrict", "electornopre", "electornoprefix"],
    "RollNo": ["rollno", "rollnumber", "electorno", "electornumber"],
    "ElectorForename": ["electorforename", "forename", "firstname", "givenname"],
    "ElectorMiddleName": ["electormiddlename", "middlenames", "middlename"],
    "ElectorSurname": ["electorsurname", "surname", "lastname", "familyname"],
    "DateOfAttainment": ["dateofattainment", "dateattained", "doa",
                         "attainmentdate"],
    "RegisteredAddress1": ["registeredaddress1", "address1", "regaddress1"],
    "RegisteredAddress2": ["registeredaddress2", "address2", "regaddress2"],
    "RegisteredAddress3": ["registeredaddress3", "address3", "regaddress3"],
    "RegisteredAddress4": ["registeredaddress4", "address4", "regaddress4"],
    "RegisteredAddress5": ["registeredaddress5", "address5", "regaddress5"],
    "RegisteredAddress6": ["registeredaddress6", "address6", "regaddress6"],
    "PostCode": ["postcode", "zipcode"],
    "UPRN": ["uprn"],
    "Suffix": ["suffix", "electorsuffix", "electornosuffix"],
    "ElectorTitle": ["electortitle", "title"],
    "ElectorID": ["electorid"],
    # Council "SubHouse" carries the flat designator; "House" carries the
    # building name/number. Aliases deliberately exclude "housename" and
    # "house_name" to avoid colliding with the TTW reference's "House Name"
    # column (which normalises to "housename"). The TTW reference is read
    # directly via build_padding_reference and never goes through map_row
    # aliasing, so we don't need that alias here.
    "SubHouse": ["subhouse", "sub_house", "sub house"],
    "House": ["house"],
}
for canonical, aliases in _alias_entries.items():
    for alias in aliases:
        COLUMN_ALIASES[alias] = canonical


def resolve_aliases(headers, quiet=False):
    """Resolve column name aliases in-place. Returns (renamed_headers, alias_log).

    Only renames a column if its canonical target is not already present.
    This ensures council-format names take precedence (e.g. if both
    RegisteredAddress1 and Address1 exist, only RegisteredAddress1 is used).
    """
    alias_log = []  # [(original_name, canonical_name)]
    skipped_aliases = []  # [(original_name, canonical_name, reason)]
    canonical_present = set(headers)  # Track what's already in headers
    new_headers = []

    for h in headers:
        norm = _norm_col(h)
        canonical = COLUMN_ALIASES.get(norm)
        if canonical and canonical != h and canonical not in canonical_present:
            alias_log.append((h, canonical))
            canonical_present.add(canonical)
            new_headers.append(canonical)
        elif canonical and canonical != h and canonical in canonical_present:
            skipped_aliases.append((h, canonical))
            new_headers.append(h)
        else:
            new_headers.append(h)

    if alias_log and not quiet:
        print(f"NOTE: Resolved {len(alias_log)} column alias(es):", file=sys.stderr)
        for orig, canon in alias_log:
            print(f"  '{orig}' -> '{canon}'", file=sys.stderr)
    if skipped_aliases and not quiet:
        for orig, canon in skipped_aliases:
            print(f"NOTE: Column '{orig}' also maps to '{canon}' but "
                  f"'{canon}' already present — kept as '{orig}'", file=sys.stderr)

    return new_headers, alias_log


# Council-only columns (preserved by default, stripped with --strip-extra)
# Columns always preserved in output regardless of --strip-extra
# (e.g. ChangeTypeID from electoral updates data)
ALWAYS_PRESERVE_COLUMNS = ["ChangeTypeID"]

COUNCIL_ONLY_COLUMNS = [
    "ElectorTitle", "IERStatus", "FranchiseMarker",
    "Euro", "Parl", "County", "Ward",
    "SubHouse", "House",
    "MethodOfVerification", "ElectorID",
]

# Enrichment columns recognized but not mapped (preserved unless --strip-extra)
ENRICHMENT_DISCARD_COLUMNS = ["Full Name", "Full name"]

# Enrichment extra columns preserved in output (unless --strip-extra)
ENRICHMENT_EXTRA_COLUMNS = [
    "Email Address", "Phone number", "Comments", "Issues",
    "P/PB", "DNK", "New", "1st round",
    "Identifier", "Address Identifier",
]

# Enrichment source columns (mapped to TTW election columns)
ENRICHMENT_SOURCE_COLUMNS = ["GE24", "Party", "1-5", "PostalVoter?",
                             "PostalVoter", "Postal Voter", "Postal voter"]

# TTW headers that indicate a file-swap
TTW_INDICATOR_HEADERS = {"Elector No. Prefix", "Full Elector No.", "Elector No. Suffix"}

# Date formats to try (ordered by priority)
KNOWN_DATE_FORMATS_DMY = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y"]
KNOWN_DATE_FORMATS_MDY = ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]

# Road suffixes for address heuristics (lowercase for comparison)
ROAD_SUFFIXES = {"road", "rd", "street", "st", "lane", "ln", "avenue", "ave",
    "drive", "dr", "close", "cl", "court", "ct", "crescent", "cres", "cr",
    "way", "place", "pl", "terrace", "gardens", "grove", "hill",
    "park", "rise", "row", "square", "walk", "mews", "passage",
    "parade", "broadway", "highway", "embankment", "boulevard",
    "vale", "chase", "green", "common", "path", "mount", "villas"}

# Building-name suffixes that also appear in ROAD_SUFFIXES (used by Fix 4b)
# These words commonly name buildings (e.g. "Sheil Court", "Oak Terrace") but
# Fix 4 skips them because they look like roads. Fix 4b reorders when Address2
# confirms a road is already present.
BUILDING_NAME_SUFFIXES = {"court", "place", "gardens", "terrace", "grove",
    "square", "parade", "mews", "villas", "green", "chase", "rise", "row"}

# Regex for unit prefixes (Flat, Unit, Apt, Room, Studio)
UNIT_PREFIXES_RE = re.compile(r"^(flat|unit|apt|room|studio)\s+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class QAReport:
    """Collects report entries during processing."""

    def __init__(self):
        self.input_file = ""
        self.output_file = ""
        self.mode = ""
        self.suffix_mode = "auto"
        self.input_encoding = ""
        self.input_columns = []
        self.output_columns = []
        self.discarded_columns = []
        self.removed_optional = []
        self.total_input = 0
        self.total_output = 0
        self.deletions = []       # [(pdcode, rollno, name, reason)]
        self.warnings = []        # [(row, field, value, issue)]
        self.fixes = []           # [(row, field, old_value, new_value, issue)]
        self.info = []            # [str]
        self.critical_warnings = []  # [str] — shown at top of report
        self.unrecognized_columns = []  # [col_name]
        self.alias_log = []  # [(original_name, canonical_name)]
        self.strip_extra = False

    def write(self, path):
        """Write human-readable report with machine-readable footer."""
        lines = []
        lines.append("=" * 50)
        lines.append("Electoral Register Conversion QA Report")
        lines.append("=" * 50)
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Input file: {self.input_file}")
        lines.append(f"Output file: {self.output_file}")
        lines.append(f"Mode: {self.mode}")
        lines.append(f"Suffix mode: {self.suffix_mode}")
        lines.append("")

        # Critical warnings first (e.g. A/D collisions)
        if self.critical_warnings:
            lines.append("!" * 50)
            lines.append("CRITICAL — MANUAL REVIEW REQUIRED")
            lines.append("!" * 50)
            for cw in self.critical_warnings:
                lines.append(f"  {cw}")
            lines.append("")

        lines.append("--- Summary ---")
        lines.append(f"Total input rows: {self.total_input}")
        lines.append(f"Rows deleted (no address): {len(self.deletions)}")
        lines.append(f"Rows in output: {self.total_output}")
        lines.append(f"Fixes applied: {len(self.fixes)}")
        lines.append(f"Warnings: {len(self.warnings)}")
        lines.append("")

        lines.append("--- Encoding ---")
        lines.append(f"Input encoding detected: {self.input_encoding}")
        lines.append("Output encoding: UTF-8-BOM")
        lines.append("")

        lines.append("--- Column Mapping ---")
        lines.append(f"Input columns: {', '.join(self.input_columns)}")
        lines.append("")

        # Per-field origin table
        # Invert FIELD_MAP: TTW field -> council field
        ttw_to_council = {v: k for k, v in FIELD_MAP.items()}

        # Transformation annotations for fields that get extra processing
        transform_notes = {
            "Surname": "(+ name case normalization)",
            "Forename": "(+ name case normalization)",
            "Date of Attainment": "(+ date normalization to DD/MM/YYYY)",
            "Address1": "(+ address reformatting)",
            "Address2": "(+ address reformatting)",
            "PostCode": "(+ spacing/case normalization)",
        }

        col_width = max(
            (len(f) for f in self.output_columns),
            default=25,
        )
        lines.append(f"  {'Output Field':<{col_width}}    <- Source")
        lines.append(f"  {'-' * col_width}    ------")

        for field in self.output_columns:
            if field == "Elector No. Suffix":
                source = "Auto-detected: decimal RollNo -> sequential suffix, else '0'"
            elif field == "Full Elector No.":
                source = "Computed: {Elector No. Prefix}-{Elector No.}-{Elector No. Suffix}"
            elif field in ttw_to_council:
                source = ttw_to_council[field]
                annotation = transform_notes.get(field)
                if annotation:
                    source = f"{source} {annotation}"
            else:
                # Election columns or other unmapped fields
                source = field
            lines.append(f"  {field:<{col_width}}    <- {source}")

        lines.append("")
        lines.append(f"Output columns: {', '.join(self.output_columns)}")
        if self.discarded_columns:
            lines.append(f"Discarded columns: {', '.join(self.discarded_columns)}")
        if self.removed_optional:
            lines.append(f"Removed empty optional columns: {', '.join(self.removed_optional)}")
        lines.append("")

        if self.deletions:
            lines.append("--- Deleted Records ---")
            for pdcode, rollno, name, reason in self.deletions:
                lines.append(f"  {pdcode}-{rollno} ({name}): {reason}")
            lines.append("")

        if self.fixes:
            lines.append("--- Fixes Applied ---")
            for row, field, old_value, new_value, issue in self.fixes:
                lines.append(f"  Row {row}: {field} '{old_value}' -> '{new_value}' -- {issue}")
            lines.append("")

        if self.warnings:
            lines.append("--- Warnings ---")
            for row, field, value, issue in self.warnings:
                display_val = f"'{value}'" if value else "(empty)"
                lines.append(f"  Row {row}: {field} = {display_val} -- {issue}")
            lines.append("")

        if self.unrecognized_columns:
            label = "stripped" if self.strip_extra else "preserved"
            lines.append(f"--- Unrecognized Input Columns ({label}) ---")
            for col in self.unrecognized_columns:
                lines.append(f"  {col}")
            lines.append("")

        if self.alias_log:
            lines.append("--- Column Aliases Resolved ---")
            for orig, canon in self.alias_log:
                lines.append(f"  '{orig}' -> '{canon}'")
            lines.append("")

        if self.info:
            lines.append("--- Info ---")
            for msg in self.info:
                lines.append(f"  {msg}")
            lines.append("")

        # Machine-readable section
        # Known limitation: if old/new values contain '|', parsing will break.
        # Electoral register data will not contain '|', so this is acceptable.
        lines.append("### MACHINE-READABLE SECTION ###")
        for pdcode, rollno, name, reason in self.deletions:
            lines.append(f"DELETED|PDCode={pdcode}|RollNo={rollno}|Reason={reason}")
        for row, field, old_value, new_value, issue in self.fixes:
            lines.append(f"FIX|Row={row}|Field={field}|Old={old_value}|New={new_value}|Issue={issue}")
        for row, field, value, issue in self.warnings:
            lines.append(f"WARNING|Row={row}|Field={field}|Value={value}|Issue={issue}")
        lines.append("### END MACHINE-READABLE SECTION ###")

        Path(path).write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def validate_input(headers, rows, report, max_rows):
    """Validate input headers and row count. Returns True if OK."""

    # File-swap detection
    header_set = set(headers)
    if header_set & TTW_INDICATOR_HEADERS:
        print("ERROR: Input file appears to be in TTW format already, not council format.",
              file=sys.stderr)
        print("Did you accidentally swap the input and output files?", file=sys.stderr)
        print(f"Headers found: {headers}", file=sys.stderr)
        sys.exit(1)

    # Required columns
    missing = [c for c in REQUIRED_COUNCIL_COLUMNS if c not in header_set]
    if missing:
        print(f"ERROR: Missing required columns: {missing}", file=sys.stderr)
        print(f"Found columns: {headers}", file=sys.stderr)
        sys.exit(1)

    # Empty file
    if not rows:
        print("ERROR: Input file has zero data rows.", file=sys.stderr)
        sys.exit(1)

    # Row count warning
    if len(rows) > max_rows:
        report.info.append(
            f"WARNING: File contains {len(rows)} rows, exceeding the TTW limit of "
            f"{max_rows}. The upload will need to be split into multiple files."
        )

    return True


# ---------------------------------------------------------------------------
# Field mapping
# ---------------------------------------------------------------------------

def map_row(council_row, row_num=None, report=None):
    """Map council-format fields to TTW-format fields.

    After the standard FIELD_MAP copy, if the council row uses dedicated
    SubHouse / House columns (Brent's preferred place for flat designator
    and building name), fold them into Address1/Address2 and shift any
    RegisteredAddress1-RegisteredAddress4 content into Address3-Address6.
    Composition runs *after* the FIELD_MAP loop so its writes are final —
    otherwise the loop would overwrite our SubHouse value with whatever
    (often empty) string sits in RegisteredAddress1.
    """
    ttw_row = {}
    for council_col, ttw_col in FIELD_MAP.items():
        val = council_row.get(council_col) or ""
        ttw_row[ttw_col] = val.strip()

    sub_house = (council_row.get("SubHouse") or "").strip()
    house = (council_row.get("House") or "").strip()

    if not (sub_house or house):
        return ttw_row

    ra1 = (council_row.get("RegisteredAddress1") or "").strip()
    ra2 = (council_row.get("RegisteredAddress2") or "").strip()
    ra3 = (council_row.get("RegisteredAddress3") or "").strip()
    ra4 = (council_row.get("RegisteredAddress4") or "").strip()
    ra5 = (council_row.get("RegisteredAddress5") or "").strip()
    ra6 = (council_row.get("RegisteredAddress6") or "").strip()

    if sub_house and house:
        # Structured flat data — both populated.
        # Some council data redundantly stores the building name in
        # RegisteredAddress1 (and/or the combined "SubHouse House" string).
        # When that happens, drop the dup so we don't end up with the
        # building name twice in Address2 + Address3.
        def _norm(s):
            return " ".join(s.split()).lower()
        combined = f"{sub_house} {house}".strip()
        if _norm(ra1) in {_norm(house), _norm(combined)}:
            # RA1 is a duplicate — collapse it out of the shift.
            # Address3..6 take RA2..RA5; RA6 still falls off the end.
            shifted = [ra2, ra3, ra4, ra5]
            dropped_high = ra6
        else:
            shifted = [ra1, ra2, ra3, ra4]
            dropped_high = "; ".join(p for p in (ra5, ra6) if p)
        ttw_row["Address1"] = sub_house
        ttw_row["Address2"] = house
        for offset, val in enumerate(shifted[:4], start=3):
            ttw_row[f"Address{offset}"] = val
        if dropped_high and report is not None and row_num is not None:
            report.warnings.append((row_num, "Address", "",
                f"Address overflow dropped after SubHouse/House shift: "
                f"{dropped_high}"))
    elif sub_house and not house:
        # Flat designator alone — keep RA1 as Address2 (likely the building).
        ttw_row["Address1"] = sub_house
        ttw_row["Address2"] = ra1
        ttw_row["Address3"] = ra2
        ttw_row["Address4"] = ra3
        ttw_row["Address5"] = ra4
        ttw_row["Address6"] = ra5
        if ra6 and report is not None and row_num is not None:
            report.warnings.append((row_num, "Address", "",
                f"RA6 dropped after SubHouse shift: '{ra6}'"))
    elif house and not sub_house:
        # Building/house number without flat — house goes in Address1.
        ttw_row["Address1"] = house
        ttw_row["Address2"] = ra1
        ttw_row["Address3"] = ra2
        ttw_row["Address4"] = ra3
        ttw_row["Address5"] = ra4
        ttw_row["Address6"] = ra5
        if ra6 and report is not None and row_num is not None:
            report.warnings.append((row_num, "Address", "",
                f"RA6 dropped after House shift: '{ra6}'"))

    return ttw_row


# ---------------------------------------------------------------------------
# Suffix computation
# ---------------------------------------------------------------------------

def _strip_decimal_elector_no(rows, report):
    """Strip decimal RollNo to integer part on every row.

    Returns dict[row_index, frac_value] for rows whose original Elector No.
    contained a decimal. Membership in the dict indicates the row was
    decimal-derived; downstream suffix logic uses the fractional value as
    a tie-break sort key.
    """
    roll_no_frac = {}
    for i, row in enumerate(rows):
        en = row.get("Elector No.", "")
        if "." in en:
            int_part, frac_str = en.split(".", 1)
            try:
                frac = float("0." + frac_str)
            except ValueError:
                frac = 0.0
            roll_no_frac[i] = frac
            row["Elector No."] = int_part
            if report:
                report.fixes.append((i + 2, "Elector No.", en, int_part,
                                     "decimal RollNo normalised to integer"))
    return roll_no_frac


def _match_ad_to_reference(rows, reference_entries, report):
    """Match A/D rows to reference entries by (prefix, number) + name/address scoring.

    Assumes _strip_decimal_elector_no has already run, so Elector No. is decimal-free.
    Assigns Elector No. Suffix directly. Orphans (no reference candidate) get a
    per-row sentinel f"ORPHAN-{row_num}" plus a critical warning.
    Returns set of row indices that were processed."""
    ad_indices = set()

    for i, row in enumerate(rows):
        change_type = row.get("ChangeTypeID", "").strip().upper()
        if change_type not in ("A", "D"):
            continue
        ad_indices.add(i)

        prefix = row.get("Elector No. Prefix", "").strip()
        number = row.get("Elector No.", "").strip()

        key = (prefix, number)
        candidates = reference_entries.get(key, [])

        update_name = f"{row.get('Forename', '')} {row.get('Surname', '')}".strip()
        update_addr = " ".join(
            row.get(f"Address{j}", "").strip() for j in range(1, 5)
        ).strip()

        if not candidates:
            # Orphan — distinct, non-numeric sentinel so TTW will reject the
            # row at upload and the user is forced to reconcile manually.
            sentinel = f"ORPHAN-{i + 2}"
            row["Elector No. Suffix"] = sentinel
            if report:
                report.critical_warnings.append(
                    f"Orphan {change_type}: row {i + 2}, no reference entry for "
                    f"{prefix}-{number} ({update_name}). "
                    f"Suffix set to '{sentinel}' — manual check required "
                    f"before TTW upload.")
            continue

        if len(candidates) == 1:
            # Single match — use directly
            row["Elector No. Suffix"] = candidates[0]["suffix"]
            if report:
                report.fixes.append(
                    (i + 2, "Elector No. Suffix", "", candidates[0]["suffix"],
                     f"matched to reference ({change_type})"))
            continue

        # Multiple candidates — score by name + address similarity
        scored = []
        for entry in candidates:
            name_sim = _surname_forename_similarity(
                row.get("Surname", ""), row.get("Forename", ""),
                entry["surname"], entry["forename"])
            addr_sim = _address_similarity(update_addr, entry["addr"])
            score = 0.5 * name_sim + 0.5 * addr_sim
            scored.append((score, entry))
        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_entry = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else -1.0

        row["Elector No. Suffix"] = best_entry["suffix"]
        ref_name = f"{best_entry['forename']} {best_entry['surname']}".strip()

        if report:
            report.fixes.append(
                (i + 2, "Elector No. Suffix", "", best_entry["suffix"],
                 f"matched to reference ({change_type}, score={best_score:.2f}, "
                 f"ref='{ref_name}')"))
            if best_score < 0.6 or (best_score - second_score) < 0.15:
                report.warnings.append(
                    (i + 2, "Elector No.", "",
                     f"Low confidence match ({change_type}): "
                     f"'{update_name}' -> '{ref_name}' "
                     f"(score={best_score:.2f}, margin={best_score - second_score:.2f}, "
                     f"suffix={best_entry['suffix']})"))

    return ad_indices


def compute_suffixes(rows, council_rows=None, report=None,
                     reference_suffixes=None, reference_entries=None,
                     roll_no_frac=None):
    """Compute Elector No. Suffix for each row.

    Assumes _strip_decimal_elector_no has already run, so Elector No. is
    decimal-free. `roll_no_frac` is dict[row_index, frac_value] for rows
    whose original RollNo was decimal — used as a tie-break sort key.

    If ChangeTypeID is present and reference data available:
    - Phase A: A/D rows matched to reference (suffix from reference, or
               ORPHAN-{row_num} sentinel if no candidate).
    - Phase B: N rows grouped by (prefix, number); assign smallest free
               integer suffix avoiding reference suffixes and A/D suffixes
               in the same group.

    Otherwise (no ChangeTypeID):
    - Auto-detects method: decimal RollNos (sequential suffix per group),
      council Suffix column, or default "0".

    reference_suffixes: dict[(prefix, number)] -> set(suffix_strings) for N-row logic
    reference_entries: dict[(prefix, number)] -> list of {suffix, surname, forename, addr} for A/D matching
    """
    roll_no_frac = roll_no_frac or {}
    has_change_type = any(row.get("ChangeTypeID", "").strip() for row in rows)

    if has_change_type and reference_entries is None:
        print("ERROR: ChangeTypeID column detected in input but --full-register not provided.\n"
              "Electoral updates require a reference register to match A/D rows correctly.\n"
              "Download the full register from TTW (Data Export) and pass it with:\n"
              "  --full-register ttw_app_export.csv",
              file=sys.stderr)
        sys.exit(1)

    if has_change_type and reference_entries is not None:
        # --- Phase A: A/D rows — match to reference ---
        ad_indices = _match_ad_to_reference(rows, reference_entries, report)

        # Group A/D suffixes by (prefix, number) so Phase B can avoid them.
        ad_suffixes_by_key = defaultdict(set)
        for i in ad_indices:
            prefix = rows[i].get("Elector No. Prefix", "").strip()
            number = rows[i].get("Elector No.", "").strip()
            suffix = rows[i].get("Elector No. Suffix", "").strip()
            if prefix and number and suffix:
                ad_suffixes_by_key[(prefix, number)].add(suffix)

        # --- Phase B: N rows — group by (prefix, number) and assign next-free ---
        n_indices = set(range(len(rows))) - ad_indices

        # Group N rows by (prefix, number); preserve frac for sort tiebreak.
        n_groups = defaultdict(list)
        for i in n_indices:
            prefix = rows[i].get("Elector No. Prefix", "").strip()
            number = rows[i].get("Elector No.", "").strip()
            frac = roll_no_frac.get(i, 0.0)
            n_groups[(prefix, number)].append((i, frac))

        for key, members in n_groups.items():
            taken = set()
            if reference_suffixes:
                taken |= reference_suffixes.get(key, set())
            taken |= ad_suffixes_by_key.get(key, set())

            # Determinism: input row index primary, fractional value secondary.
            members.sort(key=lambda m: (m[0], m[1]))

            next_s = 0
            for idx, _ in members:
                while str(next_s) in taken:
                    next_s += 1
                new_suffix = str(next_s)
                rows[idx]["Elector No. Suffix"] = new_suffix
                taken.add(new_suffix)
                if report and new_suffix != "0":
                    report.fixes.append((idx + 2, "Elector No. Suffix", "",
                        new_suffix, "suffix assigned (avoids reference + A/D)"))
                next_s += 1

        _build_full_elector_no(rows)
        # Defensive: dedup should be a no-op now. If a duplicate slips through
        # it indicates a bug in Phase B's `taken` construction.
        _dedup_full_elector_no(rows, report, skip_indices=ad_indices)
    else:
        # --- No ChangeTypeID: existing behaviour, decimals already stripped ---
        has_decimals = bool(roll_no_frac)
        has_suffix_col = (council_rows is not None
                          and any((cr.get("Suffix") or "").strip()
                                  for cr in council_rows))

        if has_decimals:
            _normalize_suffixes(rows, council_rows, report, reference_suffixes,
                                roll_no_frac=roll_no_frac)
        elif has_suffix_col:
            for row, council_row in zip(rows, council_rows):
                suffix = (council_row.get("Suffix") or "").strip()
                row["Elector No. Suffix"] = suffix if suffix else ""
        else:
            for row in rows:
                row["Elector No. Suffix"] = "0"

        _build_full_elector_no(rows)
        _dedup_full_elector_no(rows, report)
        if reference_suffixes:
            _check_reference_clashes(rows, reference_suffixes, report)


def _build_full_elector_no(rows):
    """Build Full Elector No. from prefix, number, and suffix."""
    for row in rows:
        prefix = row.get("Elector No. Prefix", "")
        number = row.get("Elector No.", "")
        suffix = row.get("Elector No. Suffix", "")
        if suffix:
            row["Full Elector No."] = f"{prefix}-{number}-{suffix}"
        else:
            row["Full Elector No."] = f"{prefix}-{number}"


def _dedup_full_elector_no(rows, report, skip_indices=None):
    """Resolve duplicate Full Elector No. by reassigning suffixes sequentially.
    Rows in skip_indices (e.g. A/D rows) are never reassigned."""
    skip = skip_indices or set()
    fen_groups = defaultdict(list)
    for i, row in enumerate(rows):
        fen = row.get("Full Elector No.", "")
        fen_groups[fen].append(i)

    for fen, indices in fen_groups.items():
        if len(indices) < 2:
            continue
        # Only reassign non-skipped rows in the collision group
        reassignable = [idx for idx in indices if idx not in skip]
        skipped_in_group = [idx for idx in indices if idx in skip]
        if not reassignable:
            # All colliding rows are A/D — warn about unresolvable collision
            if len(skipped_in_group) > 1 and report:
                details = []
                for idx in skipped_in_group:
                    r = rows[idx]
                    ct = r.get("ChangeTypeID", "").strip()
                    name = f"{r.get('Forename', '')} {r.get('Surname', '')}".strip()
                    addr = f"{r.get('Address1', '')} {r.get('Address2', '')}".strip()
                    details.append(f"Row {idx + 2}: {ct} '{name}' at '{addr}'")
                detail_str = "; ".join(details)
                report.critical_warnings.append(
                    f"COLLISION: Multiple A/D rows share Full Elector No. '{fen}' — "
                    f"cannot auto-resolve. Manual review needed. {detail_str}")
            continue
        # Find taken suffixes from skipped rows in this group
        taken = {rows[idx].get("Elector No. Suffix", "") for idx in indices if idx in skip}
        next_s = 0
        for idx in reassignable:
            while str(next_s) in taken:
                next_s += 1
            row = rows[idx]
            new_suffix = str(next_s)
            old_suffix = row.get("Elector No. Suffix", "")
            if new_suffix != old_suffix:
                row["Elector No. Suffix"] = new_suffix
                if report:
                    report.fixes.append((idx + 2, "Elector No. Suffix",
                        old_suffix, new_suffix,
                        f"auto-assigned to resolve duplicate {fen}"))
            taken.add(new_suffix)
            next_s += 1
        # Rebuild Full Elector No. for reassigned rows
        for idx in reassignable:
            row = rows[idx]
            prefix = row.get("Elector No. Prefix", "")
            number = row.get("Elector No.", "")
            suffix = row.get("Elector No. Suffix", "")
            if suffix:
                row["Full Elector No."] = f"{prefix}-{number}-{suffix}"
            else:
                row["Full Elector No."] = f"{prefix}-{number}"


def _normalize_suffixes(rows, council_rows, report, reference_suffixes=None,
                        row_filter=None, roll_no_frac=None):
    """Assign sequential suffixes to groups containing decimal-derived RollNos.

    Used in non-update mode (no ChangeTypeID). Assumes _strip_decimal_elector_no
    has already run, so Elector No. is decimal-free. `roll_no_frac` indicates
    which rows were originally decimal and provides their fractional values
    for ordering.

    Only groups containing at least one decimal-derived row are renumbered.
    Groups with only integer-RollNo rows keep their existing Suffix column
    values (or "0" if no Suffix column).

    If reference_suffixes is provided, suffix assignment skips values already
    present in the reference to avoid clashes.
    """
    roll_no_frac = roll_no_frac or {}
    has_suffix_col = (council_rows is not None
                      and any((cr.get("Suffix") or "").strip()
                              for cr in council_rows))

    # Group by (prefix, integer_rollno) — Elector No. is already integer.
    groups = defaultdict(list)
    for i, row in enumerate(rows):
        if row_filter is not None and i not in row_filter:
            continue
        prefix = row.get("Elector No. Prefix", "")
        int_part = row.get("Elector No.", "")
        frac_val = roll_no_frac.get(i, 0.0)
        is_decimal = i in roll_no_frac
        groups[(prefix, int_part)].append((i, frac_val, is_decimal))

    for (prefix, int_part), members in groups.items():
        has_decimal_member = any(m[2] for m in members)

        if not has_decimal_member:
            # Preserve existing Suffix column values, or assign "0"
            for idx, _, _ in members:
                if has_suffix_col:
                    suffix = (council_rows[idx].get("Suffix") or "").strip()
                    rows[idx]["Elector No. Suffix"] = suffix if suffix else ""
                else:
                    rows[idx]["Elector No. Suffix"] = "0"
            continue

        taken_suffixes = set()
        if reference_suffixes:
            taken_suffixes = reference_suffixes.get((prefix, int_part), set())

        # Sort by fractional value ascending, row index as tiebreak.
        # Preserves original behaviour: integer-RollNo rows (frac=0.0) sort
        # before decimal siblings, so they take suffix 0 by convention.
        members.sort(key=lambda m: (m[1], m[0]))

        next_suffix = 0
        for idx, _, _ in members:
            while str(next_suffix) in taken_suffixes:
                next_suffix += 1
            new_suffix = str(next_suffix)
            rows[idx]["Elector No. Suffix"] = new_suffix
            if report and new_suffix != "0":
                report.fixes.append((idx + 2, "Elector No. Suffix", "",
                    new_suffix, "suffix normalized (fractional -> sequential)"))
            next_suffix += 1


def _check_reference_clashes(rows, reference_suffixes, report):
    """Resolve clashes between output suffixes and the reference register.

    Used in non-update mode (no ChangeTypeID) when a reference register is
    supplied for padding-width purposes. If an N-equivalent row's suffix
    already exists in the reference for the same (prefix, number), reassigns
    to the next available integer suffix that's not in the reference and not
    already used by another update row in the same group.

    In update mode, Phase B's `taken` set construction handles this directly,
    so this function is only called from the else-branch.
    """
    clashes_fixed = 0
    for i, row in enumerate(rows):
        change_type = row.get("ChangeTypeID", "").strip().upper()
        if change_type in ("A", "D"):
            continue
        prefix = row.get("Elector No. Prefix", "").strip()
        number = row.get("Elector No.", "").strip()
        suffix = row.get("Elector No. Suffix", "").strip()
        key = (prefix, number)

        if key not in reference_suffixes:
            continue

        taken = reference_suffixes[key]
        if suffix in taken:
            next_s = 0
            all_taken = taken | {r.get("Elector No. Suffix", "").strip()
                                 for r in rows
                                 if r.get("Elector No. Prefix", "").strip() == prefix
                                 and r.get("Elector No.", "").strip() == number}
            while str(next_s) in all_taken:
                next_s += 1
            new_suffix = str(next_s)
            old_suffix = suffix
            row["Elector No. Suffix"] = new_suffix
            row["Full Elector No."] = f"{prefix}-{number}-{new_suffix}"
            clashes_fixed += 1
            if report:
                report.fixes.append((i + 2, "Elector No. Suffix", old_suffix,
                    new_suffix, "suffix reassigned to avoid clash with reference register"))

    if clashes_fixed and report:
        report.warnings.append(
            (0, "Elector No.", "",
             f"{clashes_fixed} suffix(es) reassigned to avoid clashes with reference register"))


# ---------------------------------------------------------------------------
# Date normalization
# ---------------------------------------------------------------------------

def normalize_date(value, date_format_hint="DMY"):
    """Normalize a date string to DD/MM/YYYY or blank.
    Returns (normalized_value, warning_message_or_None).
    """
    if not value or not value.strip():
        return "", None

    value = value.strip()
    formats = KNOWN_DATE_FORMATS_DMY if date_format_hint == "DMY" else KNOWN_DATE_FORMATS_MDY

    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            # Validate reasonable year range
            if parsed.year < 1900 or parsed.year > 2100:
                return "", f"unreasonable year {parsed.year}, cleared to blank (original: '{value}')"
            return parsed.strftime("%d/%m/%Y"), None
        except ValueError:
            continue

    return "", f"unparseable date, cleared to blank (original: '{value}')"


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

def _needs_case_fix(value):
    """Return True only if ALL alpha chars are uppercase or ALL lowercase.

    Mixed case (e.g. "McDonald", "O'Brien-Smythe") is left alone.
    """
    if not value:
        return False
    alpha = [c for c in value if c.isalpha()]
    if not alpha:
        return False
    if all(c.isupper() for c in alpha):
        return True
    if all(c.islower() for c in alpha):
        return True
    return False


def _smart_title_case(name):
    """Title-case a name with special handling for hyphens, apostrophes, Mc/Mac.

    Known limitation: Non-Scottish names like "MACKEREL" would become "MacKerel".
    Since all changes are logged as FIX entries and the /review-report skill
    highlights name changes for human review, this is acceptable.
    """
    def _capitalize_part(part):
        if not part:
            return part
        # Mc prefix (len>2): "MCDONALD" -> "McDonald"
        if len(part) > 2 and part[:2].lower() == "mc":
            return "Mc" + part[2:].capitalize()
        # Mac prefix (remainder 4+ chars): "MACDONALD" -> "MacDonald"
        if len(part) > 3 and part[:3].lower() == "mac" and len(part[3:]) >= 4:
            return "Mac" + part[3:].capitalize()
        return part.capitalize()

    # Split on hyphens, capitalize each part
    hyphen_parts = name.split("-")
    result_parts = []
    for hp in hyphen_parts:
        # Handle apostrophes: "O'BRIEN" -> "O'Brien"
        if "'" in hp:
            apo_parts = hp.split("'")
            apo_result = "'".join(_capitalize_part(ap) for ap in apo_parts)
            result_parts.append(apo_result)
        else:
            result_parts.append(_capitalize_part(hp))
    return "-".join(result_parts)


def normalize_names(row, row_num, report):
    """Normalize name casing for Forename, Surname, Middle Names.

    Only touches ALL CAPS or all lowercase names. Mixed case is left alone.
    Does NOT touch address fields.
    """
    for field in ("Forename", "Surname", "Middle Names"):
        value = row.get(field, "")
        if not value or not _needs_case_fix(value):
            continue
        # For multi-word names (e.g. middle names "JEAN PIERRE"), split on spaces
        words = value.split()
        new_words = [_smart_title_case(w) for w in words]
        new_value = " ".join(new_words)
        if new_value != value:
            report.fixes.append((row_num, field, value, new_value, "name case normalized"))
            row[field] = new_value


# ---------------------------------------------------------------------------
# Address reformatting
# ---------------------------------------------------------------------------

_DIRECTIONAL_SUFFIXES = {"north", "south", "east", "west"}

def _looks_like_road(text):
    """Return True if text ends with a road suffix (or road suffix + direction)."""
    words = text.lower().split()
    if not words:
        return False
    if words[-1] in ROAD_SUFFIXES:
        return True
    # Handle trailing directional: "Park Avenue North" -> check "Avenue"
    if len(words) >= 2 and words[-1] in _DIRECTIONAL_SUFFIXES and words[-2] in ROAD_SUFFIXES:
        return True
    return False


def reformat_addresses(row, row_num, report):
    """Auto-fix detectable address formatting issues, flag ambiguous patterns.

    Fix order: 1 (gap) -> 1b (dual-number bracket) -> 1c (ampersand) -> 2 (flat comma) -> 2b (comma-free flat+road) -> 3 (number before flat) -> 4 (number before building) -> 4b (ambiguous building+road suffix) -> 4c (single-word building)
    Each fix refreshes local vars after modifying row.
    """
    # --- Fix 1: Address gap (Address2 empty, Address3+ has data) ---
    addr_fields = ["Address1", "Address2", "Address3", "Address4", "Address5", "Address6"]
    values = [row.get(f, "") for f in addr_fields]
    gap_found = False
    for i in range(len(values) - 1):
        if not values[i] and any(values[j] for j in range(i + 1, len(values))):
            gap_found = True
            break
    if gap_found:
        # Shift all non-empty values up
        non_empty = [v for v in values if v]
        new_values = non_empty + [""] * (len(values) - len(non_empty))
        for i, f in enumerate(addr_fields):
            if values[i] != new_values[i]:
                report.fixes.append((row_num, f, values[i], new_values[i], "address gap shifted up"))
            row[f] = new_values[i]

    # Refresh locals after fix 1
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 1b: Auto-bracket dual-number addresses ---
    # Skip if Address1 already starts with bracket notation
    if addr1 and not addr1.startswith("["):
        # Case A: "N, N road" in Address1 (comma-separated dual numbers)
        dual_comma = re.match(r"^(\d+)\s*,\s*(\d+.*)$", addr1)
        if dual_comma:
            old_addr1 = addr1
            new_addr1 = f"[{dual_comma.group(1)}], {dual_comma.group(2)}"
            row["Address1"] = new_addr1
            report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
                "dual-number auto-bracketed"))
        else:
            # Case B: "N N something" (space-separated, 2nd token starts with digit, >=3 tokens)
            tokens = addr1.split()
            if (len(tokens) >= 3
                    and re.match(r"^\d+$", tokens[0])
                    and tokens[1][0].isdigit()):
                old_addr1 = addr1
                new_addr1 = f"[{tokens[0]}], {' '.join(tokens[1:])}"
                row["Address1"] = new_addr1
                report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
                    "dual-number auto-bracketed"))
            else:
                # Case C: Address1 = bare number, Address2 starts with digit
                if re.match(r"^\d+$", addr1) and addr2 and addr2[0].isdigit():
                    old_addr1 = addr1
                    new_addr1 = f"[{addr1}]"
                    row["Address1"] = new_addr1
                    report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
                        "dual-number auto-bracketed"))

    # Refresh locals after fix 1b
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 1c: Replace '&' with 'and' in address fields ---
    for addr_field in ["Address1", "Address2", "Address3", "Address4", "Address5", "Address6"]:
        val = row.get(addr_field, "")
        if "&" in val:
            old_val = val
            new_val = val.replace("&", "and")
            row[addr_field] = new_val
            report.fixes.append((row_num, addr_field, old_val, new_val,
                "ampersand replaced with 'and'"))

    # Refresh locals after fix 1c
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 2: Flat comma split (Address1 matches "Flat/Unit/Apt X, rest" AND Address2 empty) ---
    flat_comma_match = re.match(r"^((?:Flat|Unit|Apt|Room|Studio)\s+\S+)\s*,\s*(.+)$", addr1, re.IGNORECASE)
    if flat_comma_match:
        if not addr2:
            # Auto-split
            old_addr1 = addr1
            new_addr1 = flat_comma_match.group(1)
            new_addr2 = flat_comma_match.group(2)
            row["Address1"] = new_addr1
            row["Address2"] = new_addr2
            report.fixes.append((row_num, "Address1", old_addr1, new_addr1, "flat comma split"))
            report.fixes.append((row_num, "Address2", "", new_addr2, "flat comma split"))
        # else: Address2 occupied — leave unchanged, no split, no warning (already valid per UG C3 slide 11)

    # Refresh locals after fix 2
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 2b: Comma-free flat+road (Address1 = "Flat X <road-address>", Address2 empty) ---
    # Council data may have "Flat 3 30 Chamberlayne Road" all in Address1 with no comma.
    # Split only when flat ID is standard (numeric/single-letter) and remainder looks like a road.
    # Tighter regex prevents mis-splitting multi-word flat IDs like "Flat Ground Floor".
    comma_free_flat_road = re.match(
        r"^((?:Flat|Unit|Apt|Room|Studio)\s+(?:\d+[A-Za-z]?|[A-Za-z]))\s+(.+)$", addr1, re.IGNORECASE)
    if comma_free_flat_road and not addr2:
        flat_part = comma_free_flat_road.group(1)
        road_part = comma_free_flat_road.group(2)
        if _looks_like_road(road_part):
            old_addr1 = addr1
            row["Address1"] = flat_part
            row["Address2"] = road_part
            report.fixes.append((row_num, "Address1", old_addr1, flat_part, "comma-free flat+road split"))
            report.fixes.append((row_num, "Address2", "", road_part, "comma-free flat+road split"))

    # Refresh locals after fix 2b
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 3: Number before Flat (Address1 starts with \d+ Flat/Unit/Apt) ---
    # UG C3 slide 12: "56 Flat 1 | Coleman Road" is explicitly INVALID regardless of Address2.
    num_before_flat = re.match(r"^(\d+[A-Za-z]?)\s+((?:Flat|Unit|Apt|Room|Studio)\s+.*)$", addr1, re.IGNORECASE)
    if num_before_flat:
        old_addr1 = addr1
        building_num = num_before_flat.group(1)
        flat_part = num_before_flat.group(2)
        if not addr2:
            # Address2 empty: Flat part -> Address1, building number -> Address2
            row["Address1"] = flat_part
            row["Address2"] = building_num
            report.fixes.append((row_num, "Address1", old_addr1, flat_part, "number before flat reordered"))
            report.fixes.append((row_num, "Address2", "", building_num, "number before flat reordered"))
        elif not re.match(r"^\d+[A-Za-z]?\s", addr2):
            # Address2 has no leading house number: safe to prepend building number
            old_addr2 = addr2
            new_addr2 = f"{building_num} {addr2}"
            row["Address1"] = flat_part
            row["Address2"] = new_addr2
            report.fixes.append((row_num, "Address1", old_addr1, flat_part, "number before flat reordered"))
            report.fixes.append((row_num, "Address2", old_addr2, new_addr2, "number before flat reordered"))
        else:
            # Address2 already has a house number — ambiguous, flag for manual review
            report.warnings.append((row_num, "Address1", addr1,
                "NEEDS MANUAL FIX: Number before flat designation, but Address2 already has a house number"))

    # Refresh locals after fix 3
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 4: Number before building name ---
    # Pattern: "N BuildingName" where remainder is >=2 words, all alpha, no road suffix, no unit prefix
    num_before_building = re.match(r"^(\d+[A-Za-z]?)\s+(.+)$", addr1)
    if num_before_building:
        number = num_before_building.group(1)
        remainder = num_before_building.group(2)
        remainder_words = remainder.split()
        if (len(remainder_words) >= 2
                and all(w.isalpha() for w in remainder_words)
                and not _looks_like_road(remainder)
                and not UNIT_PREFIXES_RE.match(remainder)):
            old_addr1 = addr1
            new_addr1 = f"{remainder} {number}"
            row["Address1"] = new_addr1
            report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
                "number before building name reordered"))

    # Refresh locals after fix 4
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 4b: Number before building name that ends in a road-suffix word ---
    # Fix 4 skips names like "Sheil Court" because "court" is in ROAD_SUFFIXES.
    # This second pass catches those when Address2 confirms a road is already present.
    if not addr2:
        pass  # Skip — could genuinely be a road name
    else:
        num_before_ambig = re.match(r"^(\d+[A-Za-z]?)\s+(.+)$", addr1)
        if num_before_ambig:
            number = num_before_ambig.group(1)
            remainder = num_before_ambig.group(2)
            remainder_words = remainder.split()
            if (len(remainder_words) >= 2
                    and all(w.isalpha() for w in remainder_words)
                    and _looks_like_road(remainder)
                    and remainder_words[-1].lower() in BUILDING_NAME_SUFFIXES
                    and not UNIT_PREFIXES_RE.match(remainder)
                    and _looks_like_road(addr2)):
                old_addr1 = addr1
                new_addr1 = f"{remainder} {number}"
                row["Address1"] = new_addr1
                report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
                    "number before building name reordered (Address2 confirms road)"))

    # Refresh locals after fix 4b
    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")

    # --- Fix 4c: Single-word building name with Address2 road confirmation ---
    # Handles cases like "26 Dorada" where Address2 = "30 Chamberlayne Road"
    # confirms the single word is a building name, not a road.
    # Requires len >= 2 to exclude single-letter house-number suffixes (e.g. "14 B").
    # Hyphenated single-word names (e.g. "St-Johns") are excluded by the alpha
    # check — this is a conscious limitation; they are rare and ambiguous enough
    # to warrant manual review rather than auto-reordering.
    if addr2 and _looks_like_road(addr2):
        num_before_single = re.match(r"^(\d+[A-Za-z]?)\s+(.+)$", addr1)
        if num_before_single:
            number = num_before_single.group(1)
            remainder = num_before_single.group(2)
            remainder_words = remainder.split()
            if (len(remainder_words) == 1
                    and len(remainder) >= 2
                    and remainder.isalpha()):
                old_addr1 = addr1
                new_addr1 = f"{remainder} {number}"
                row["Address1"] = new_addr1
                report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
                    "single-word building name reordered (Address2 confirms road)"))

    # Refresh for flagging
    addr1 = row.get("Address1", "")

    # --- Flags (NEEDS MANUAL FIX — logged as WARNING) ---
    if "," in addr1:
        # Check if it's a flat comma pattern (which was already handled above)
        flat_match = re.match(r"^((?:Flat|Unit|Apt|Room|Studio)\s+\S+)\s*,", addr1, re.IGNORECASE)
        # Check if it's a bracketed prefix (e.g. "[506], 10 Evelina Gardens")
        bracket_match = re.match(r"^\[.+?\]\s*,", addr1)
        if not flat_match and not bracket_match:
            report.warnings.append((row_num, "Address1", addr1,
                "NEEDS MANUAL FIX: Contains comma but not a 'Flat X, N Road' pattern -- may need manual splitting"))

    # Advisory: long flat-prefix Address1 that wasn't auto-split
    # Fire even when Addr2 is occupied — TTW may still misparse a long unsplit Addr1.
    addr2 = row.get("Address2", "")
    if UNIT_PREFIXES_RE.match(addr1) and "," not in addr1:
        word_count = len(addr1.split())
        if word_count >= 5:
            report.warnings.append((row_num, "Address1", addr1,
                f"Flat designation with {word_count} words and no comma -- may need manual splitting"))

    # Note: bracket notation (e.g. "[100-102]") is valid per UG C3 slide 10 — no warning needed.


_FLAT_RE = re.compile(r"^((?:Flat|Unit|Apt|Room|Studio)\s+)(\d+)([A-Za-z]?)$", re.IGNORECASE)
# Width-detection only: tolerates trailing context (e.g. "Flat 0302 Queensbrook
# Building"). Used by _compute_flat_widths so reference rows whose House Name
# embeds the building name still contribute their flat-number width. Never
# feeds into address rewrites — the strict _FLAT_RE above governs those.
_FLAT_WIDTH_RE = re.compile(
    r"^(?:Flat|Unit|Apt|Room|Studio)\s+(\d+)[A-Za-z]?(?:\s|$)", re.IGNORECASE)
_BUILDING_NUM_RE = re.compile(r"^(.+\S)\s+(\d+)([A-Za-z]?)$")


def _padding_key(addr_field, postcode):
    """Build a canonical key for padding-group lookups.

    Uppercases and collapses whitespace on the address-side field and runs
    PostCode through the standard normaliser, so reference and update keys
    compare equal regardless of casing/spacing differences between sources.
    """
    addr_norm = " ".join(addr_field.upper().split()) if addr_field else ""
    pc_norm, _ = normalize_postcode(postcode) if postcode else ("", None)
    return (addr_norm, pc_norm)


def _compute_flat_widths(rows):
    """Compute max flat-number widths grouped by canonical (Address2, PostCode).
    Returns dict[(addr2_norm, postcode_norm)] -> max_numeric_width.

    Uses the loose _FLAT_WIDTH_RE so rows whose Address1 carries trailing
    context (e.g. "Flat 0302 Queensbrook Building", as TTW exports flats
    when the building name lives in House Name) still contribute their
    flat-number width to the group max.
    """
    groups = defaultdict(list)
    for row in rows:
        addr1 = row.get("Address1", "")
        addr2 = row.get("Address2", "")
        postcode = row.get("PostCode", "")
        if not addr2:
            continue
        m = _FLAT_WIDTH_RE.match(addr1)
        if m:
            groups[_padding_key(addr2, postcode)].append(len(m.group(1)))
    return {key: max(widths) for key, widths in groups.items()}


def _compute_building_widths(rows):
    """Compute max building-number widths grouped by canonical (building_name, postcode).
    Returns dict[(building_name_norm, postcode_norm)] -> max_numeric_width."""
    groups = defaultdict(list)
    for row in rows:
        addr1 = row.get("Address1", "")
        postcode = row.get("PostCode", "")
        if UNIT_PREFIXES_RE.match(addr1):
            continue
        m = _BUILDING_NUM_RE.match(addr1)
        if m and any(c.isalpha() for c in m.group(1)):
            building_name = m.group(1)
            groups[_padding_key(building_name, postcode)].append(len(m.group(2)))
    return {key: max(widths) for key, widths in groups.items()}


def _parse_voter_number(voter_number):
    """Parse a TTW Voter Number (e.g. 'KG1-1-0', 'KG1-1') into (prefix, number, suffix)."""
    parts = voter_number.split("-")
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return parts[0], parts[1], "0"
    return "", "", ""


_FLAT_PREFIX_SPLIT_RE = re.compile(
    r"^((?:Flat|Unit|Apt|Room|Studio)\s+\d+[A-Za-z]?)\s+(.+)$", re.IGNORECASE)


def build_padding_reference(reference_path, report=None):
    """Read a reference CSV (TTW app-export or cleaned register) and compute
    padding widths + suffix data.

    Auto-detects format:
    - TTW app-export: 'Voter Number', 'First Name', 'Surname', 'House Name/Number/Road'
    - Cleaned register: 'Elector No. Prefix', 'Elector No.', 'Elector No. Suffix',
      'Forename', 'Surname', 'Address1'-'Address4'

    For app-export rows whose 'House Name' embeds both the flat designator and
    building name in one string (e.g. "Flat 0302 Queensbrook Building", with
    'House Number' empty), the function parses it into Address1="Flat 0302" /
    Address2="Queensbrook Building" so padding-group keys align with the
    update side (which carries flat-then-building in Address1/Address2
    after SubHouse/House composition in map_row).

    Returns (flat_widths, building_widths, suffix_taken, suffix_entries) where:
    - suffix_taken: dict[(prefix, number)] -> set of existing suffix strings
    - suffix_entries: dict[(prefix, number)] -> list of {suffix, surname, forename, addr}
    """
    rows, _, headers = read_input(reference_path)
    header_set = set(headers)

    # Detect format
    is_app_export = "Voter Number" in header_set
    is_cleaned = "Elector No. Prefix" in header_set

    nonstandard_shape_count = 0
    nonstandard_example = None

    if is_app_export:
        # Remap TTW app-export columns to cleaned-register equivalents for padding
        # Build Address1/Address2 from House Name/Number/Road for padding functions
        for row in rows:
            house_name = row.get("House Name", "").strip()
            house_num = row.get("House Number", "").strip()
            road = row.get("Road", "").strip()
            # Build Address1: flat/building info, Address2: road (default).
            if house_name and house_num:
                row["Address1"] = f"{house_name} {house_num}"
                row["Address2"] = road
            elif house_name:
                # When House Name embeds "Flat NNNN BuildingName", split it so
                # the flat designator and building name end up in distinct
                # Address1/Address2 fields — matching the post-SubHouse/House
                # shape on the update side. Keeps padding keys aligned.
                m = _FLAT_PREFIX_SPLIT_RE.match(house_name)
                if m:
                    row["Address1"] = m.group(1)        # "Flat 0302"
                    row["Address2"] = m.group(2)        # "Queensbrook Building"
                    # Road shifts to Address3 for completeness; not used by the
                    # padding helpers but preserved for any downstream consumer.
                    if road:
                        row["Address3"] = road
                else:
                    row["Address1"] = house_name
                    row["Address2"] = road
            elif house_num:
                row["Address1"] = house_num
                row["Address2"] = road
            else:
                row["Address1"] = road
                row["Address2"] = ""
            # Normalize PostCode column name
            if "Post Code" in row and "PostCode" not in row:
                row["PostCode"] = row["Post Code"]

            # Defensive: detect rows in a non-standard shape that won't
            # contribute to flat-width matching. Aggregated, single warning
            # emitted after the loop.
            if (house_name and house_num
                    and not _FLAT_WIDTH_RE.match(f"{house_name} {house_num}")):
                nonstandard_shape_count += 1
                if nonstandard_example is None:
                    nonstandard_example = (house_name, house_num)

        if nonstandard_shape_count and report is not None:
            hn, hnum = nonstandard_example
            report.warnings.append((0, "ReferenceShape", "",
                f"Reference contains {nonstandard_shape_count} row(s) with "
                f"non-standard flat shape (e.g. House Name={hn!r}, "
                f"House Number={hnum!r}). Cross-file padding will not fire "
                f"for these rows."))

    flat_w = _compute_flat_widths(rows)
    building_w = _compute_building_widths(rows)

    suffix_taken = defaultdict(set)
    suffix_entries = defaultdict(list)

    for row in rows:
        if is_app_export:
            voter_num = row.get("Voter Number", "").strip()
            prefix, number, suffix = _parse_voter_number(voter_num)
            surname = row.get("Surname", "").strip()
            forename = row.get("First Name", "").strip()
            addr_parts = [row.get("House Name", "").strip(),
                          row.get("House Number", "").strip(),
                          row.get("Road", "").strip()]
        else:
            prefix = row.get("Elector No. Prefix", "").strip()
            number = row.get("Elector No.", "").strip()
            suffix = row.get("Elector No. Suffix", "").strip() or "0"
            surname = row.get("Surname", "").strip()
            forename = row.get("Forename", "").strip()
            addr_parts = [row.get(f"Address{i}", "").strip() for i in range(1, 5)]

        if prefix and number:
            suffix_taken[(prefix, number)].add(suffix)
            suffix_entries[(prefix, number)].append({
                "suffix": suffix,
                "surname": surname,
                "forename": forename,
                "addr": " ".join(p for p in addr_parts if p),
            })

    return flat_w, building_w, dict(suffix_taken), dict(suffix_entries)


def zero_pad_flat_numbers(rows, report, reference_widths=None):
    """Zero-pad flat numbers for consistent sort order within each building.

    Groups rows by (Address2, PostCode). Within each group, pads numeric
    flat IDs to the maximum width found (e.g. Flat 1 -> Flat 01 if max is 12).
    If reference_widths is provided, uses max(reference, group) for width.
    """
    group_widths = _compute_flat_widths(rows)

    # Build groups with match objects for padding
    groups = defaultdict(list)
    for i, row in enumerate(rows):
        addr1 = row.get("Address1", "")
        addr2 = row.get("Address2", "")
        postcode = row.get("PostCode", "")
        if not addr2:
            continue
        m = _FLAT_RE.match(addr1)
        if m:
            groups[_padding_key(addr2, postcode)].append((i, m))

    for key, members in groups.items():
        own_width = group_widths.get(key, 0)
        ref_width = reference_widths.get(key, 0) if reference_widths else 0
        effective_width = max(own_width, ref_width)
        if effective_width <= 1:
            continue
        for idx, m in members:
            num_str = m.group(2)
            if len(num_str) < effective_width:
                old_addr1 = rows[idx]["Address1"]
                padded = num_str.zfill(effective_width)
                new_addr1 = f"{m.group(1)}{padded}{m.group(3)}"
                rows[idx]["Address1"] = new_addr1
                report.fixes.append((idx + 2, "Address1", old_addr1, new_addr1,
                    "flat number zero-padded for sort order"))


def zero_pad_building_numbers(rows, report, reference_widths=None):
    """Zero-pad building numbers for consistent sort order within each building.

    Groups rows by (building_name, PostCode). Within each group, pads numeric
    building IDs to the maximum width found (e.g. Sheil Court 3 -> Sheil Court 03
    if max is 14). If reference_widths is provided, uses max(reference, group).

    Only applies to trailing numbers in Address1 (e.g. "Sheil Court 10").
    Flat/Unit/Apt/Room/Studio patterns are skipped (handled by zero_pad_flat_numbers).
    """
    group_widths = _compute_building_widths(rows)

    groups = defaultdict(list)
    for i, row in enumerate(rows):
        addr1 = row.get("Address1", "")
        postcode = row.get("PostCode", "")
        if UNIT_PREFIXES_RE.match(addr1):
            continue
        m = _BUILDING_NUM_RE.match(addr1)
        if m and any(c.isalpha() for c in m.group(1)):
            building_name = m.group(1)
            groups[_padding_key(building_name, postcode)].append((i, m))

    for key, members in groups.items():
        own_width = group_widths.get(key, 0)
        ref_width = reference_widths.get(key, 0) if reference_widths else 0
        effective_width = max(own_width, ref_width)
        if effective_width <= 1:
            continue
        for idx, m in members:
            num_str = m.group(2)
            if len(num_str) < effective_width:
                old_addr1 = rows[idx]["Address1"]
                padded = num_str.zfill(effective_width)
                new_addr1 = f"{m.group(1)} {padded}{m.group(3)}"
                rows[idx]["Address1"] = new_addr1
                report.fixes.append((idx + 2, "Address1", old_addr1, new_addr1,
                    "building number zero-padded for sort order"))


# ---------------------------------------------------------------------------
# Validation and flagging
# ---------------------------------------------------------------------------

def validate_row(row, row_num, report):
    """Validate a single mapped TTW row. Returns 'delete', 'keep', or 'keep'."""
    prefix = row.get("Elector No. Prefix", "")
    number = row.get("Elector No.", "")
    forename = row.get("Forename", "")
    surname = row.get("Surname", "")
    name = f"{forename} {surname}".strip() or "(no name)"

    addr1 = row.get("Address1", "")
    addr2 = row.get("Address2", "")
    postcode = row.get("PostCode", "")

    # No-address check → DELETE
    if not addr1 and not addr2 and not postcode:
        report.deletions.append((prefix, number, name, "no address"))
        return "delete"

    # Postcode-only, no street → WARNING
    if not addr1 and not addr2 and postcode:
        report.warnings.append((row_num, "Address", "",
            "PostCode present but no street address (Address1+Address2 empty)"))

    # Partial address (street but no postcode)
    if (addr1 or addr2) and not postcode:
        report.warnings.append((row_num, "PostCode", "",
            "Missing PostCode (Address data exists)"))

    # Missing name
    if not forename:
        report.warnings.append((row_num, "Forename", "", "Missing forename (required field)"))
    if not surname:
        report.warnings.append((row_num, "Surname", "", "Missing surname (required field)"))

    return "keep"


# ---------------------------------------------------------------------------
# Election data mapping
# ---------------------------------------------------------------------------

def map_election_data(row, council_row, elections, election_types, row_num, report):
    """Map election columns from council row to TTW row."""
    for election_name, election_type in zip(elections, election_types):
        # Green Voting Intention
        gvi_key = f"{election_name} Green Voting Intention"
        gvi_val = council_row.get(gvi_key, "").strip()
        if gvi_val and gvi_val not in ("1", "2", "3", "4", "5"):
            report.warnings.append((row_num, gvi_key, gvi_val,
                f"Invalid voting intention '{gvi_val}' (must be 1-5 or blank), cleared"))
            gvi_val = ""
        row[gvi_key] = gvi_val

        # Party
        party_key = f"{election_name} Party"
        party_val = council_row.get(party_key, "").strip()
        if party_val and party_val not in VALID_PARTY_CODES:
            report.warnings.append((row_num, party_key, party_val,
                f"Unrecognized party code '{party_val}', kept as-is"))
        row[party_key] = party_val

        # Voted (historic only) — TTW expects "Y" or blank
        if election_type == "historic":
            voted_key = f"{election_name} Voted"
            voted_val = council_row.get(voted_key, "").strip()
            # Treat explicit "N"/"No" as blank (did not vote)
            if voted_val.upper() in ("N", "NO"):
                voted_val = ""
            row[voted_key] = "Y" if voted_val else ""

        # Postal Voter (future only) — TTW expects "Y" or blank
        if election_type == "future":
            postal_key = f"{election_name} Postal Voter"
            postal_val = council_row.get(postal_key, "").strip()
            # Treat explicit "N"/"No" as blank (no postal vote)
            if postal_val.upper() in ("N", "NO"):
                postal_val = ""
            row[postal_key] = "Y" if postal_val else ""


# ---------------------------------------------------------------------------
# Enriched election data mapping
# ---------------------------------------------------------------------------

def map_enriched_election_data(row, council_row, elections, election_types,
                               row_num, report):
    """Map enrichment columns (GE24, Party, 1-5, PostalVoter?) to TTW election columns.

    GE24 is historic data -> maps to {election} Voted for historic elections.
    Party and 1-5 are current loyalty -> map to {election} Party and
    {election} Green Voting Intention for the future election.
    PostalVoter? -> maps to {election} Postal Voter for the future election.
    """
    for election_name, election_type in zip(elections, election_types):
        if election_type == "historic":
            # GE24 -> Voted: TTW expects "Y" or blank
            voted_key = f"{election_name} Voted"
            ge24_val = council_row.get("GE24", "").strip()
            # Treat explicit "N"/"No" as blank (did not vote)
            if ge24_val.upper() in ("N", "NO"):
                ge24_val = ""
            row[voted_key] = "Y" if ge24_val else ""

        elif election_type == "future":
            gvi_key = f"{election_name} Green Voting Intention"
            party_key = f"{election_name} Party"
            postal_key = f"{election_name} Postal Voter"

            # Party -> mapped via party name table
            party_raw = council_row.get("Party", "").strip()
            party_mapped, party_warning = map_party_name(party_raw)
            row[party_key] = party_mapped
            if party_warning:
                report.warnings.append((row_num, party_key, party_raw, party_warning))

            # 1-5 -> Green Voting Intention (validate 1-5)
            gvi_val = council_row.get("1-5", "").strip()
            if gvi_val and gvi_val not in ("1", "2", "3", "4", "5"):
                report.warnings.append((row_num, gvi_key, gvi_val,
                    f"Invalid voting intention '{gvi_val}' (must be 1-5 or blank), cleared"))
                gvi_val = ""
            row[gvi_key] = gvi_val

            # PostalVoter? / PostalVoter / Postal Voter -> TTW "Y" or blank
            postal_raw = ""
            for _pv_key in ("PostalVoter?", "PostalVoter", "Postal Voter",
                            "postalvoter?", "postalvoter", "postal voter",
                            "POSTALVOTER?", "POSTALVOTER", "POSTAL VOTER",
                            "Postal voter", "postal Voter"):
                postal_raw = council_row.get(_pv_key, "").strip()
                if postal_raw:
                    break
            # Treat explicit "N"/"No" as blank (no postal vote)
            if postal_raw.upper() in ("N", "NO"):
                postal_raw = ""
            row[postal_key] = "Y" if postal_raw else ""


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def build_output_headers(rows, elections, election_types, has_date_data=False,
                         strip_empty=False, enriched_columns=False,
                         strip_extra=False, input_headers=None):
    """Build final output header list.

    By default, keeps all columns (including empty ones) to match TTW test data
    format. Date of Attainment is only included if the input data has date values.
    Use strip_empty=True to remove entirely-empty optional columns.
    When enriched_columns=True, extra columns are appended unless strip_extra=True.
    """
    # Start with full register headers
    headers = list(TTW_REGISTER_HEADERS)

    # Include Date of Attainment only if input data had date values
    if has_date_data:
        headers.insert(DOA_INSERT_POSITION, "Date of Attainment")

    # Add election columns
    for election_name, election_type in zip(elections, election_types):
        if enriched_columns and election_type == "historic":
            # Historic enriched: only Voted (no GVI/Party — those go on future)
            headers.append(f"{election_name} Voted")
        else:
            # Standard layout: GVI + Party + Voted/Postal Voter
            headers.append(f"{election_name} Green Voting Intention")
            headers.append(f"{election_name} Party")
            if election_type == "historic":
                headers.append(f"{election_name} Voted")
            elif election_type == "future":
                headers.append(f"{election_name} Postal Voter")

    # Always preserve ALWAYS_PRESERVE_COLUMNS if present in input
    header_set = set(headers)
    for col in ALWAYS_PRESERVE_COLUMNS:
        if col in set(input_headers or []) and col not in header_set:
            headers.append(col)
            header_set.add(col)

    # Append extra input columns to output (unless --strip-extra)
    if not strip_extra:
        input_set = set(input_headers or [])
        exclude = set(FIELD_MAP.keys())
        # SubHouse/House are consumed by map_row's composition into
        # Address1/Address2 and must not be re-emitted as raw output columns.
        exclude.add("SubHouse")
        exclude.add("House")
        # Exclude TTW-named columns only when their mapped source is present
        for ttw_name, source_name in _FIELD_MAP_REVERSE.items():
            if source_name in input_set:
                exclude.add(ttw_name)
        if enriched_columns:
            exclude |= set(ENRICHMENT_SOURCE_COLUMNS)
        for col in (input_headers or []):
            if col and col not in header_set and col not in exclude:
                headers.append(col)
                header_set.add(col)

    # Optionally remove entirely-empty optional columns
    removed = []
    if strip_empty:
        for col in list(headers):
            if col in OPTIONAL_COLUMNS:
                if all(not row.get(col, "").strip() for row in rows):
                    headers.remove(col)
                    removed.append(col)

    return headers, removed


def main():
    parser = argparse.ArgumentParser(
        description="Transform council electoral register CSV to TTW Digital format."
    )
    parser.add_argument("input", help="Input CSV in council format")
    parser.add_argument("output", help="Output CSV in TTW format")
    parser.add_argument("--mode", choices=["register", "register+elections"],
                        default="register", help="Output mode (default: register)")
    parser.add_argument("--elections", nargs="*", default=[],
                        help="Election names (e.g. 2022 2026)")
    parser.add_argument("--election-types", nargs="*", default=[],
                        help="Election types: 'historic' or 'future' per election")
    # --suffix-mode removed: auto-detects decimal RollNo values
    parser.add_argument("--date-format", choices=["DMY", "YMD", "MDY"],
                        default="DMY", help="Input date format hint (default: DMY)")
    parser.add_argument("--report", default=None,
                        help="QA report path (default: OUTPUT.report.txt)")
    parser.add_argument("--max-rows", type=int, default=100000,
                        help="Warn if input exceeds this many rows (default: 100000)")
    parser.add_argument("--strip-empty", action="store_true",
                        help="Remove entirely-empty optional columns from output")
    parser.add_argument("--enriched-columns", action="store_true",
                        help="Input has enrichment columns (GE24, Party, 1-5) in "
                             "non-standard names. Maps them to TTW election columns.")
    parser.add_argument("--strip-extra", action="store_true",
                        help="Remove non-TTW columns from output. By default, all "
                             "input columns are preserved in the output.")
    parser.add_argument("--full-register", default=None,
                        help="Full register CSV for reference (TTW app-export or previously "
                             "cleaned register). Required when processing electoral updates "
                             "(ChangeTypeID column). Provides zero-padding widths and suffix "
                             "matching for A/D rows.")
    parser.add_argument("--quiet", action="store_true", help="Suppress stdout progress")
    parser.add_argument("--no-aliases", action="store_true",
                        help="Disable automatic column name alias resolution")
    args = parser.parse_args()

    # Validate election args
    if args.mode == "register+elections":
        if not args.elections:
            parser.error("--elections required with --mode register+elections")
        if not args.election_types:
            parser.error("--election-types required with --mode register+elections")
        if len(args.elections) != len(args.election_types):
            parser.error("--elections and --election-types must have the same number of values")
        for et in args.election_types:
            if et not in ("historic", "future"):
                parser.error(f"Invalid election type '{et}': must be 'historic' or 'future'")

    # Validate enriched-columns constraints
    if args.enriched_columns:
        if args.mode != "register+elections":
            parser.error("--enriched-columns requires --mode register+elections")
        historic_count = sum(1 for et in args.election_types if et == "historic")
        future_count = sum(1 for et in args.election_types if et == "future")
        if historic_count > 1:
            parser.error("--enriched-columns only supports one historic election "
                         "(enrichment columns GE24/Party/1-5 are not per-election)")
        if future_count != 1:
            parser.error("--enriched-columns requires exactly one future election "
                         "(Party/1-5 map to a single future election)")

    report = QAReport()
    report.input_file = args.input
    report.output_file = args.output
    report.mode = args.mode
    report.suffix_mode = "auto"
    report.strip_extra = args.strip_extra
    report_path = args.report or f"{args.output}.report.txt"

    # --- Step 1: Read input ---
    if not args.quiet:
        print(f"Reading {args.input}...")
    council_rows, encoding, headers = read_input(args.input)

    # File-swap detection on raw headers (before alias resolution, which would
    # rename TTW indicator headers like "Elector No. Prefix" -> "PDCode")
    raw_header_set = set(headers)
    if raw_header_set & TTW_INDICATOR_HEADERS:
        print("ERROR: Input file appears to be in TTW format already, not council format.",
              file=sys.stderr)
        print("Did you accidentally swap the input and output files?", file=sys.stderr)
        print(f"Headers found: {headers}", file=sys.stderr)
        sys.exit(1)

    # Resolve column name aliases (e.g. "Address1" -> "RegisteredAddress1")
    if not args.no_aliases:
        headers, alias_log = resolve_aliases(headers, quiet=args.quiet)
        if alias_log:
            old_to_new = dict(alias_log)
            for row in council_rows:
                for old_name, new_name in old_to_new.items():
                    if old_name in row:
                        row[new_name] = row.pop(old_name)
        report.alias_log = alias_log

    report.input_encoding = encoding
    report.input_columns = headers
    report.total_input = len(council_rows)

    # --- Check for unrecognized input columns ---
    known_columns = (set(FIELD_MAP.keys()) | set(COUNCIL_ONLY_COLUMNS)
                     | set(ALWAYS_PRESERVE_COLUMNS) | {"Suffix"})
    if args.enriched_columns:
        known_columns |= set(ENRICHMENT_EXTRA_COLUMNS)
        known_columns |= set(ENRICHMENT_DISCARD_COLUMNS)
        known_columns |= set(ENRICHMENT_SOURCE_COLUMNS)
    if args.mode == "register+elections" and not args.enriched_columns:
        for ename, etype in zip(args.elections, args.election_types):
            known_columns.add(f"{ename} Green Voting Intention")
            known_columns.add(f"{ename} Party")
            if etype == "historic":
                known_columns.add(f"{ename} Voted")
            if etype == "future":
                known_columns.add(f"{ename} Postal Voter")

    report.unrecognized_columns = [h for h in headers if h and h not in known_columns]
    if report.unrecognized_columns and not args.quiet:
        if args.strip_extra:
            print(f"NOTE: {len(report.unrecognized_columns)} input column(s) not recognized "
                  f"and will be stripped: {report.unrecognized_columns}", file=sys.stderr)
        else:
            print(f"NOTE: {len(report.unrecognized_columns)} input column(s) not recognized "
                  f"and will be passed through unchanged: {report.unrecognized_columns}", file=sys.stderr)

    # --- Step 2-4: Validate ---
    validate_input(headers, council_rows, report, args.max_rows)

    # --- Log discarded and special columns ---
    header_set = set(headers)
    report.discarded_columns = []
    if args.strip_extra:
        report.discarded_columns = [c for c in COUNCIL_ONLY_COLUMNS if c in header_set]
        if args.enriched_columns:
            report.discarded_columns += [c for c in ENRICHMENT_DISCARD_COLUMNS if c in header_set]
    # --- Step 5-6: Strip whitespace and map columns ---
    mapped_rows = []
    for i, council_row in enumerate(council_rows):
        # Strip whitespace on all council fields (handle None keys/values from malformed CSV)
        stripped = {k: (v.strip() if isinstance(v, str) else (v or ""))
                    for k, v in council_row.items() if k is not None}
        # Map to TTW fields. Pass row_num/report so SubHouse/House composition
        # can warn when RA5/RA6 are dropped during the shift.
        ttw_row = map_row(stripped, row_num=i + 2, report=report)
        # Preserve all input columns not already mapped by FIELD_MAP.
        # If an input column has the same name as a TTW output field (e.g. "Address2"),
        # only skip it if the corresponding mapped source column (e.g. "RegisteredAddress2")
        # is present in the input — otherwise the data would be silently lost.
        # SubHouse/House are treated as consumed: map_row folds their content
        # into Address1/Address2, so re-emitting them as raw passthrough
        # columns would duplicate the data in the output CSV.
        for col, val in stripped.items():
            if col in FIELD_MAP:
                continue  # Source column already consumed by map_row
            if col in ("SubHouse", "House"):
                continue  # Consumed by map_row's SubHouse/House composition
            source_col = _FIELD_MAP_REVERSE.get(col)
            if source_col and source_col in stripped:
                continue  # TTW-named col would overwrite properly mapped value
            ttw_row[col] = val
        mapped_rows.append(ttw_row)

    # --- Step 6.5: Normalize names ---
    for i, row in enumerate(mapped_rows):
        normalize_names(row, i + 2, report)

    # --- Step 6.6: Reformat addresses ---
    for i, row in enumerate(mapped_rows):
        reformat_addresses(row, i + 2, report)

    # --- Step 6.65: Normalize postcodes (must precede zero-padding so the
    #     reference and update sides build matching padding-group keys) ---
    for i, row in enumerate(mapped_rows):
        row_num = i + 2
        pc = row.get("PostCode", "")
        normalized, warning = normalize_postcode(pc)
        row["PostCode"] = normalized
        if warning:
            report.warnings.append((row_num, "PostCode", pc, warning))

    # --- Step 6.66: Strip decimal RollNos universally before any suffix logic ---
    roll_no_frac = _strip_decimal_elector_no(mapped_rows, report)

    # --- Step 6.7-6.8: Zero-pad flat and building numbers ---
    flat_ref_widths = None
    building_ref_widths = None
    ref_suffix_data = None
    ref_suffix_entries = None
    if args.full_register:
        if not args.quiet:
            print(f"Reading full register reference: {args.full_register}")
        flat_ref_widths, building_ref_widths, ref_suffix_data, ref_suffix_entries = \
            build_padding_reference(args.full_register, report=report)

    zero_pad_flat_numbers(mapped_rows, report, reference_widths=flat_ref_widths)
    zero_pad_building_numbers(mapped_rows, report, reference_widths=building_ref_widths)

    # --- Step 7: Compute suffix ---
    compute_suffixes(mapped_rows, council_rows, report=report,
                     reference_suffixes=ref_suffix_data,
                     reference_entries=ref_suffix_entries,
                     roll_no_frac=roll_no_frac)

    # --- Step 8: Normalize dates ---
    has_date_data = False
    for i, row in enumerate(mapped_rows):
        row_num = i + 2
        doa = row.get("Date of Attainment", "")
        normalized, warning = normalize_date(doa, args.date_format)
        row["Date of Attainment"] = normalized
        if normalized:
            has_date_data = True
        if warning:
            report.warnings.append((row_num, "Date of Attainment", doa, warning))

    # --- Step 10: Validate and flag ---
    delete_indices = set()
    for i, row in enumerate(mapped_rows):
        row_num = i + 2
        action = validate_row(row, row_num, report)
        if action == "delete":
            delete_indices.add(i)

    # --- Step 11: Map election data ---
    if args.mode == "register+elections":
        for i, (row, council_row) in enumerate(zip(mapped_rows, council_rows)):
            if i not in delete_indices:
                if args.enriched_columns:
                    map_enriched_election_data(row, council_row, args.elections,
                                              args.election_types, i + 2, report)
                else:
                    map_election_data(row, council_row, args.elections,
                                      args.election_types, i + 2, report)

    # --- Step 12: Apply deletions ---
    output_rows = [row for i, row in enumerate(mapped_rows) if i not in delete_indices]
    report.total_output = len(output_rows)

    # --- Step 13: Duplicate detection ---
    # Only warn about shared (prefix, number) when suffixes don't make them unique
    key_counts = Counter(
        (row.get("Elector No. Prefix", ""), row.get("Elector No.", ""))
        for row in output_rows
    )
    for (prefix, number), count in key_counts.items():
        if count > 1:
            dup_rows = [i + 2 for i, row in enumerate(mapped_rows)
                        if row.get("Elector No. Prefix") == prefix
                        and row.get("Elector No.") == number
                        and i not in delete_indices]
            # Check if suffixes make them unique
            dup_suffixes = [row.get("Elector No. Suffix", "")
                           for row in output_rows
                           if row.get("Elector No. Prefix") == prefix
                           and row.get("Elector No.") == number]
            if len(dup_suffixes) != len(set(dup_suffixes)):
                # Suffixes don't resolve the collision — genuine duplicate
                report.warnings.append((dup_rows[0], "Elector No.",
                    f"{prefix}-{number}",
                    f"Duplicate: appears {count} times (rows {dup_rows})"))

    # --- Step 14: Full Elector No. uniqueness ---
    fen_counts = Counter(row.get("Full Elector No.", "") for row in output_rows)
    fen_dups = {k: v for k, v in fen_counts.items() if v > 1}
    if fen_dups:
        print("ERROR: Duplicate Full Elector No. values found:", file=sys.stderr)
        for fen, count in fen_dups.items():
            print(f"  {fen} appears {count} times", file=sys.stderr)
        print("Output would be invalid for TTW upload. Resolve duplicates.", file=sys.stderr)
        # Still write the report for debugging
        report.write(report_path)
        sys.exit(1)

    # --- Step 15: Build output headers ---
    elections = args.elections if args.mode == "register+elections" else []
    election_types = args.election_types if args.mode == "register+elections" else []
    output_headers, removed_cols = build_output_headers(
        output_rows, elections, election_types,
        has_date_data=has_date_data,
        strip_empty=args.strip_empty,
        enriched_columns=args.enriched_columns,
        strip_extra=args.strip_extra,
        input_headers=headers,
    )
    report.output_columns = output_headers
    report.removed_optional = removed_cols

    # --- Step 16: Write output ---
    if not args.quiet:
        print(f"Writing {args.output}...")
    write_output(output_rows, output_headers, args.output)

    # --- Step 17: Write report ---
    report.write(report_path)

    # --- Console summary ---
    if not args.quiet:
        print(f"\nInput:   {report.total_input} rows")
        print(f"Output:  {report.total_output} rows")
        print(f"Deleted: {len(report.deletions)} rows (no address - UG C3 mandated)")
        print(f"Fixes:   {len(report.fixes)}")
        print(f"Warnings: {len(report.warnings)}")
        print(f"Report:  {report_path}")

    # --- Enrichment/canvassing detection guidance ---
    if not args.quiet:
        input_header_set = set(headers)

        # Detect enrichment source columns (GE24, Party, 1-5, PostalVoter?)
        detected_enrichment = [c for c in ENRICHMENT_SOURCE_COLUMNS if c in input_header_set]
        # Detect canvassing/extra columns
        detected_canvassing = [c for c in ENRICHMENT_EXTRA_COLUMNS if c in input_header_set]

        if detected_enrichment and not args.enriched_columns:
            print(f"\nWARNING: Detected enrichment data columns: {detected_enrichment}",
                  file=sys.stderr)
            print("  These columns are preserved as-is but NOT mapped to TTW election format.",
                  file=sys.stderr)
            print("  To properly map them, re-run with:", file=sys.stderr)
            print("    --mode register+elections --enriched-columns \\", file=sys.stderr)
            print("    --elections <HISTORIC> <FUTURE> --election-types historic future",
                  file=sys.stderr)
            print("  (Replace <HISTORIC> and <FUTURE> with your election names, "
                  "e.g. GE2024 LE2026)", file=sys.stderr)

        if detected_canvassing:
            print(f"\nNOTE: Detected canvassing data columns: {detected_canvassing}",
                  file=sys.stderr)
            print("  These columns are preserved in the output.", file=sys.stderr)

        # TTW upload guidance
        ttw_columns = set(TTW_REGISTER_HEADERS) | {"Date of Attainment"}
        for ename in (args.elections if args.mode == "register+elections" else []):
            ttw_columns |= {f"{ename} Green Voting Intention", f"{ename} Party",
                            f"{ename} Voted", f"{ename} Postal Voter"}
        extra_count = sum(1 for h in output_headers if h not in ttw_columns)
        if extra_count > 0 and not args.strip_extra:
            print(f"\nNOTE: Output contains {extra_count} non-TTW column(s). "
                  f"If TTW rejects these, re-run with --strip-extra.", file=sys.stderr)


if __name__ == "__main__":
    main()
