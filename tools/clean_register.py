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
    python3 tools/clean_register.py combined.csv output.csv \\
        --mode register+elections \\
        --elections GE2024 LE2026 \\
        --election-types historic future \\
        --enriched-columns \\
        --suffix-mode normalize

    # Upload-ready (strip extra columns like Email, Phone, DNK, etc.)
    python3 tools/clean_register.py combined.csv upload.csv \\
        --mode register+elections \\
        --elections GE2024 LE2026 \\
        --election-types historic future \\
        --enriched-columns \\
        --suffix-mode normalize \\
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

# Columns discarded from council data (logged in report)
COUNCIL_ONLY_COLUMNS = [
    "ElectorTitle", "IERStatus", "FranchiseMarker",
    "Euro", "Parl", "County", "Ward",
    "MethodOfVerification", "ElectorID",
]

# Columns with potential address data (logged with extra detail)
SPECIAL_COLUMNS = ["SubHouse", "House"]

# Enrichment columns discarded (logged when --enriched-columns)
ENRICHMENT_DISCARD_COLUMNS = ["Full Name"]

# Enrichment extra columns preserved in output (unless --strip-extra)
ENRICHMENT_EXTRA_COLUMNS = [
    "Email Address", "Phone number", "Comments", "Issues",
    "P/PB", "DNK", "New", "1st round",
    "Identifier", "Address Identifier",
]

# Enrichment source columns (mapped to TTW election columns)
ENRICHMENT_SOURCE_COLUMNS = ["GE24", "Party", "1-5"]

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

# Regex for unit prefixes (Flat, Unit, Apt, Room)
UNIT_PREFIXES_RE = re.compile(r"^(flat|unit|apt|room)\s+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class QAReport:
    """Collects report entries during processing."""

    def __init__(self):
        self.input_file = ""
        self.output_file = ""
        self.mode = ""
        self.suffix_mode = ""
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
        self.special_columns = {} # {col: [(row, value)]}

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
            "Address1": "(+ SubHouse incorporation, address reformatting)",
            "Address2": "(+ address reformatting)",
            "PostCode": "(+ spacing/case normalization)",
        }

        # Suffix mode descriptions
        suffix_descriptions = {
            "blank": "Empty (suffix not applicable)",
            "zero": "Generated: always '0'",
            "per-address": "Generated: sequential per unique address",
            "from-column": "Input: Suffix column",
            "normalize": "Input: Suffix column (fractional values renumbered sequentially)",
        }

        col_width = max(
            (len(f) for f in self.output_columns),
            default=25,
        )
        lines.append(f"  {'Output Field':<{col_width}}    <- Source")
        lines.append(f"  {'-' * col_width}    ------")

        for field in self.output_columns:
            if field == "Elector No. Suffix":
                source = suffix_descriptions.get(self.suffix_mode, "Unknown")
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

        if self.special_columns:
            lines.append("--- Special Column Data ---")
            for col, entries in self.special_columns.items():
                lines.append(f"  {col}: {len(entries)} non-empty values (discarded).")
                lines.append(f"  These may contain address info not in Address1-6. Review recommended.")
                for row, val in entries[:10]:
                    lines.append(f"    Row {row}: '{val}'")
                if len(entries) > 10:
                    lines.append(f"    ... and {len(entries) - 10} more")
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

def map_row(council_row):
    """Map council-format fields to TTW-format fields."""
    ttw_row = {}
    for council_col, ttw_col in FIELD_MAP.items():
        val = council_row.get(council_col) or ""
        ttw_row[ttw_col] = val.strip()
    return ttw_row


# ---------------------------------------------------------------------------
# Suffix computation
# ---------------------------------------------------------------------------

def compute_suffixes(rows, mode, council_rows=None, report=None):
    """Compute Elector No. Suffix for each row based on mode."""
    if mode == "blank":
        for row in rows:
            row["Elector No. Suffix"] = ""

    elif mode == "zero":
        for row in rows:
            row["Elector No. Suffix"] = "0"

    elif mode == "per-address":
        addr_counter = {}
        for row in rows:
            key = (row.get("Address1", ""), row.get("Address2", ""), row.get("PostCode", ""))
            addr_counter[key] = addr_counter.get(key, 0) + 1
            row["Elector No. Suffix"] = str(addr_counter[key])

    elif mode == "from-column":
        if council_rows is None:
            print("ERROR: --suffix-mode=from-column requires a 'Suffix' column in input.",
                  file=sys.stderr)
            sys.exit(1)
        for row, council_row in zip(rows, council_rows):
            suffix = council_row.get("Suffix", "").strip()
            row["Elector No. Suffix"] = suffix if suffix else ""

    elif mode == "normalize":
        if council_rows is None:
            print("ERROR: --suffix-mode=normalize requires a 'Suffix' column in input.",
                  file=sys.stderr)
            sys.exit(1)
        _normalize_suffixes(rows, council_rows, report)

    # Build Full Elector No.
    for row in rows:
        prefix = row.get("Elector No. Prefix", "")
        number = row.get("Elector No.", "")
        suffix = row.get("Elector No. Suffix", "")
        if suffix:
            row["Full Elector No."] = f"{prefix}-{number}-{suffix}"
        else:
            row["Full Elector No."] = f"{prefix}-{number}"


def _normalize_suffixes(rows, council_rows, report):
    """Normalize fractional suffixes (.5, .75, .875) to sequential integers.

    Groups rows by (PDCode, RollNo). Within each group:
    - Single row: suffix stays blank
    - Multiple rows: blank/zero first, then ascending by float value,
      non-parseable suffixes after floats alphabetically.
      Primary (first) gets blank suffix, rest get 1, 2, 3...
    """
    # Group row indices by (prefix, number)
    groups = defaultdict(list)
    for i, (row, council_row) in enumerate(zip(rows, council_rows)):
        prefix = row.get("Elector No. Prefix", "")
        number = row.get("Elector No.", "")
        suffix_raw = council_row.get("Suffix", "").strip()
        groups[(prefix, number)].append((i, suffix_raw))

    for (prefix, number), members in groups.items():
        if len(members) == 1:
            # Single row: blank suffix
            idx, old_suffix = members[0]
            rows[idx]["Elector No. Suffix"] = ""
            if old_suffix and report:
                report.fixes.append((idx + 2, "Elector No. Suffix", old_suffix, "",
                    "suffix normalized (single row in group)"))
            continue

        # Multiple rows: sort and renumber
        # Classify each suffix as float-parseable or not
        float_members = []
        non_float_members = []
        for idx, suffix_raw in members:
            if not suffix_raw:
                float_members.append((0.0, idx, suffix_raw))
            else:
                try:
                    val = float(suffix_raw)
                    float_members.append((val, idx, suffix_raw))
                except ValueError:
                    non_float_members.append((suffix_raw, idx, suffix_raw))

        # Sort: floats ascending, then non-floats alphabetically
        float_members.sort(key=lambda x: x[0])
        non_float_members.sort(key=lambda x: x[0])

        sorted_members = [(idx, raw) for _, idx, raw in float_members] + \
                         [(idx, raw) for _, idx, raw in non_float_members]

        # Assign: first gets blank, rest get 1, 2, 3...
        for pos, (idx, old_suffix) in enumerate(sorted_members):
            if pos == 0:
                new_suffix = ""
            else:
                new_suffix = str(pos)

            rows[idx]["Elector No. Suffix"] = new_suffix
            if old_suffix != new_suffix and report:
                report.fixes.append((idx + 2, "Elector No. Suffix", old_suffix,
                    new_suffix, "suffix normalized (fractional -> sequential)"))


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
# SubHouse incorporation
# ---------------------------------------------------------------------------

def incorporate_subhouse(ttw_row, council_row, row_num, report):
    """Incorporate SubHouse data into Address1 when it adds flat/unit info.

    Council data sometimes puts flat designations in a separate SubHouse column
    rather than in Address1. Per UG C3 slide 10, the flat designation must be
    in Address1 (e.g. "Regency Court Flat 2") for TTW to parse it correctly.
    """
    subhouse = (council_row.get("SubHouse") or "").strip()
    if not subhouse:
        return

    addr1 = ttw_row.get("Address1", "")

    # Skip if SubHouse value is already present in Address1
    if subhouse.lower() in addr1.lower():
        return

    # SubHouse often has "Flat X, HouseNum" format. Extract the flat part.
    if "," in subhouse:
        flat_part = subhouse.split(",", 1)[0].strip()
    else:
        flat_part = subhouse

    # Skip if the flat designation from SubHouse is already in Address1.
    # Use word-boundary check to avoid "Flat 1" matching "Flat 12".
    if addr1 and flat_part:
        pattern = re.escape(flat_part) + r"(?:\s|,|$)"
        if re.search(pattern, addr1, re.IGNORECASE):
            return

    old_addr1 = addr1
    if addr1:
        new_addr1 = f"{addr1} {subhouse}"
    else:
        new_addr1 = subhouse
    ttw_row["Address1"] = new_addr1
    report.fixes.append((row_num, "Address1", old_addr1, new_addr1,
        "SubHouse data incorporated into Address1"))


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

def _looks_like_road(text):
    """Return True if text ends with a road suffix."""
    words = text.lower().split()
    return bool(words) and words[-1] in ROAD_SUFFIXES


def reformat_addresses(row, row_num, report):
    """Auto-fix detectable address formatting issues, flag ambiguous patterns.

    Fix order: 1 (gap) -> 2 (flat comma) -> 2b (comma-free flat+road) -> 3 (number before flat) -> 4 (number before building)
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

    # --- Fix 2: Flat comma split (Address1 matches "Flat/Unit/Apt X, rest" AND Address2 empty) ---
    flat_comma_match = re.match(r"^((?:Flat|Unit|Apt|Room)\s+\S+)\s*,\s*(.+)$", addr1, re.IGNORECASE)
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
        r"^((?:Flat|Unit|Apt|Room)\s+(?:\d+[A-Za-z]?|[A-Za-z]))\s+(.+)$", addr1, re.IGNORECASE)
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
    num_before_flat = re.match(r"^(\d+[A-Za-z]?)\s+((?:Flat|Unit|Apt|Room)\s+.*)$", addr1, re.IGNORECASE)
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

    # Refresh for flagging
    addr1 = row.get("Address1", "")

    # --- Flags (NEEDS MANUAL FIX — logged as WARNING) ---
    if "&" in addr1:
        report.warnings.append((row_num, "Address1", addr1,
            "NEEDS MANUAL FIX: Contains '&' -- too complex to auto-parse (see UG C3 slide 12)"))

    if "," in addr1:
        # Check if it's a flat comma pattern (which was already handled above)
        flat_match = re.match(r"^((?:Flat|Unit|Apt|Room)\s+\S+)\s*,", addr1, re.IGNORECASE)
        if not flat_match:
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

        # Voted (historic only)
        if election_type == "historic":
            voted_key = f"{election_name} Voted"
            voted_val = council_row.get(voted_key, "").strip()
            row[voted_key] = "v" if voted_val else ""

        # Postal Voter (future only)
        if election_type == "future":
            postal_key = f"{election_name} Postal Voter"
            postal_val = council_row.get(postal_key, "").strip()
            row[postal_key] = "v" if postal_val else ""


# ---------------------------------------------------------------------------
# Enriched election data mapping
# ---------------------------------------------------------------------------

def map_enriched_election_data(row, council_row, elections, election_types,
                               row_num, report):
    """Map enrichment columns (GE24, Party, 1-5) to TTW election columns.

    GE24 is historic data -> maps to {election} Voted for historic elections.
    Party and 1-5 are current loyalty -> map to {election} Party and
    {election} Green Voting Intention for the future election.
    """
    for election_name, election_type in zip(elections, election_types):
        if election_type == "historic":
            # GE24 -> Voted: any non-empty value -> "v"
            voted_key = f"{election_name} Voted"
            ge24_val = council_row.get("GE24", "").strip()
            row[voted_key] = "v" if ge24_val else ""

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

            # No postal voter source in enrichment data
            row[postal_key] = ""


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

    # Add extra columns when --enriched-columns is active (unless --strip-extra)
    if enriched_columns and not strip_extra:
        # Only include extra columns that exist in the input
        input_set = set(input_headers or [])
        for col in ENRICHMENT_EXTRA_COLUMNS:
            if col in input_set:
                headers.append(col)

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
    parser.add_argument("--suffix-mode",
                        choices=["blank", "zero", "per-address", "from-column", "normalize"],
                        default="blank",
                        help="How to compute Elector No. Suffix (default: blank). "
                             "blank=empty suffix, Full Elector No.=Prefix-No; "
                             "zero=always '0'; per-address=sequential per address; "
                             "from-column=read from input Suffix column; "
                             "normalize=renumber fractional suffixes (.5, .75) sequentially")
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
                        help="Remove non-TTW extra columns (Email, Phone, DNK, etc.) "
                             "from output. Only relevant with --enriched-columns.")
    parser.add_argument("--quiet", action="store_true", help="Suppress stdout progress")
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
    report.suffix_mode = args.suffix_mode
    report_path = args.report or f"{args.output}.report.txt"

    # --- Step 1: Read input ---
    if not args.quiet:
        print(f"Reading {args.input}...")
    council_rows, encoding, headers = read_input(args.input)
    report.input_encoding = encoding
    report.input_columns = headers
    report.total_input = len(council_rows)

    # --- Step 2-4: Validate ---
    validate_input(headers, council_rows, report, args.max_rows)

    # --- Log discarded and special columns ---
    header_set = set(headers)
    report.discarded_columns = [c for c in COUNCIL_ONLY_COLUMNS if c in header_set]
    if args.enriched_columns:
        report.discarded_columns += [c for c in ENRICHMENT_DISCARD_COLUMNS if c in header_set]
    for col in SPECIAL_COLUMNS:
        if col in header_set:
            non_empty = [(i + 2, r.get(col) or "") for i, r in enumerate(council_rows)
                         if (r.get(col) or "").strip()]
            if non_empty:
                report.special_columns[col] = non_empty

    # --- Step 5-6: Strip whitespace and map columns ---
    mapped_rows = []
    for council_row in council_rows:
        # Strip whitespace on all council fields (handle None keys/values from malformed CSV)
        stripped = {k: (v.strip() if isinstance(v, str) else (v or ""))
                    for k, v in council_row.items() if k is not None}
        # Map to TTW fields
        ttw_row = map_row(stripped)
        mapped_rows.append(ttw_row)

    # --- Step 6.4: Incorporate SubHouse data ---
    for i, (row, council_row) in enumerate(zip(mapped_rows, council_rows)):
        incorporate_subhouse(row, council_row, i + 2, report)

    # --- Step 6.5: Normalize names ---
    for i, row in enumerate(mapped_rows):
        normalize_names(row, i + 2, report)

    # --- Step 6.6: Reformat addresses ---
    for i, row in enumerate(mapped_rows):
        reformat_addresses(row, i + 2, report)

    # --- Step 7: Compute suffix ---
    suffix_needs_column = args.suffix_mode in ("from-column", "normalize")
    suffix_council = council_rows if suffix_needs_column else None
    if suffix_needs_column and "Suffix" not in header_set:
        print(f"ERROR: --suffix-mode={args.suffix_mode} requires a 'Suffix' column in input.",
              file=sys.stderr)
        sys.exit(1)
    compute_suffixes(mapped_rows, args.suffix_mode, suffix_council, report=report)

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

    # --- Step 9: Normalize postcodes ---
    for i, row in enumerate(mapped_rows):
        row_num = i + 2
        pc = row.get("PostCode", "")
        normalized, warning = normalize_postcode(pc)
        row["PostCode"] = normalized
        if warning:
            report.warnings.append((row_num, "PostCode", pc, warning))

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

    # --- Step 11b: Copy extra columns when --enriched-columns ---
    if args.enriched_columns:
        for i, (row, council_row) in enumerate(zip(mapped_rows, council_rows)):
            if i not in delete_indices:
                for col in ENRICHMENT_EXTRA_COLUMNS:
                    val = council_row.get(col) or ""
                    row[col] = val.strip()

    # --- Step 12: Apply deletions ---
    output_rows = [row for i, row in enumerate(mapped_rows) if i not in delete_indices]
    report.total_output = len(output_rows)

    # --- Step 13: Duplicate detection ---
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


if __name__ == "__main__":
    main()
