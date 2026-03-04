#!/usr/bin/env python3
"""Generate dummy electoral register data in council CSV format.

Produces test datasets for testing clean_register.py:
- Golden input/expected pairs (reverse-mapped from TTW test data)
- Edge case records covering all known data quality scenarios
- Malformed CSV files for error handling tests

Usage:
    python3 tools/generate_dummy_data.py
"""

import csv
import os
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent
TEST_DATA_DIR = SCRIPT_DIR / "test_data"
TTW_TEST_DIR = REPO_DIR / "User Material - Local Admins"
TTW_NOELECTIONS = TTW_TEST_DIR / "Test data-1000voters-oneElection-2wards-6pollingDistricts-NoElections.csv"
TTW_ELECTIONS = TTW_TEST_DIR / "Test data-1000voters-oneElection-2wards-6pollingDistricts2HistoricFuture.csv"

# Council format headers
COUNCIL_HEADERS = [
    "PDCode", "RollNo", "ElectorTitle", "ElectorSurname", "ElectorForename",
    "ElectorMiddleName", "IERStatus", "DateOfAttainment", "FranchiseMarker",
    "RegisteredAddress1", "RegisteredAddress2", "RegisteredAddress3",
    "RegisteredAddress4", "RegisteredAddress5", "RegisteredAddress6",
    "PostCode", "Euro", "Parl", "County", "Ward",
    "SubHouse", "House", "MethodOfVerification", "ElectorID", "UPRN",
]

# Council headers + Suffix column (for golden input where suffix is known)
COUNCIL_HEADERS_WITH_SUFFIX = COUNCIL_HEADERS + ["Suffix"]

# TTW test data column order (matches the supplied test data exactly)
# Note: TTW test data has Surname before Forename and no Date of Attainment
TTW_REGISTER_HEADERS_TEST_DATA = [
    "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
    "Surname", "Forename", "Middle Names",
    "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
    "PostCode", "UPRN",
]

# TTW template column order (includes Date of Attainment, Forename before Surname)
TTW_REGISTER_HEADERS_TEMPLATE = [
    "Elector No. Prefix", "Elector No.", "Elector No. Suffix", "Full Elector No.",
    "Forename", "Middle Names", "Surname", "Date of Attainment",
    "Address1", "Address2", "Address3", "Address4", "Address5", "Address6",
    "PostCode", "UPRN",
]


def read_ttw_csv(path, max_rows=20):
    """Read TTW test data CSV and return header + first max_rows data rows."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append(row)
    return headers, rows


def ttw_row_to_council(ttw_row):
    """Reverse-map a TTW test data row to council format."""
    return {
        "PDCode": ttw_row.get("Elector No. Prefix", ""),
        "RollNo": ttw_row.get("Elector No.", ""),
        "ElectorTitle": "Mr" if ttw_row.get("Forename", "") else "",
        "ElectorSurname": ttw_row.get("Surname", ""),
        "ElectorForename": ttw_row.get("Forename", ""),
        "ElectorMiddleName": ttw_row.get("Middle Names", ""),
        "IERStatus": "V",
        "DateOfAttainment": "",
        "FranchiseMarker": "E",
        "RegisteredAddress1": ttw_row.get("Address1", ""),
        "RegisteredAddress2": ttw_row.get("Address2", ""),
        "RegisteredAddress3": ttw_row.get("Address3", ""),
        "RegisteredAddress4": ttw_row.get("Address4", ""),
        "RegisteredAddress5": ttw_row.get("Address5", ""),
        "RegisteredAddress6": ttw_row.get("Address6", ""),
        "PostCode": ttw_row.get("PostCode", ""),
        "Euro": "Eastern",
        "Parl": "Norwich South",
        "County": "Norfolk",
        "Ward": "Bowthorpe",
        "SubHouse": "",
        "House": "",
        "MethodOfVerification": "D",
        "ElectorID": f"E{ttw_row.get('Elector No.', '')}",
        "UPRN": ttw_row.get("UPRN", ""),
        "Suffix": ttw_row.get("Elector No. Suffix", ""),
    }



def ttw_row_to_council_elections(ttw_row):
    """Reverse-map a TTW test data row (with elections) to council format."""
    base = ttw_row_to_council(ttw_row)
    base["2022 Green Voting Intention"] = ttw_row.get("2022 Green Voting Intention", "")
    base["2022 Party"] = ttw_row.get("2022 Party", "")
    base["2022 Voted"] = ttw_row.get("2022 Voted", "")
    base["2026 Green Voting Intention"] = ttw_row.get("2026 Green Voting Intention", "")
    base["2026 Party"] = ttw_row.get("2026 Party", "")
    base["2026 Postal Voter"] = ttw_row.get("2026 Postal Voter", "")
    return base


def write_csv(path, headers, rows, encoding="utf-8-sig", line_ending="\r\n"):
    """Write a CSV file with specified encoding and line endings."""
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator=line_ending,
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Dataset A: Golden register-only
# ---------------------------------------------------------------------------

def generate_golden_register_only():
    """Generate golden input (council format) and expected output (TTW format).

    The golden expected output is extracted DIRECTLY from the TTW test data,
    preserving the exact column order and all columns (including empty ones).
    """
    ttw_headers, ttw_rows = read_ttw_csv(TTW_NOELECTIONS, max_rows=20)

    # Golden input: council format with Suffix column (reverse-mapped)
    council_rows = [ttw_row_to_council(r) for r in ttw_rows]
    write_csv(
        TEST_DATA_DIR / "golden_input_register_only.csv",
        COUNCIL_HEADERS_WITH_SUFFIX,
        council_rows,
    )

    # Golden expected: extracted directly from TTW test data
    # ALL columns preserved (including empty Address3-6, UPRN), exact column order
    write_csv(
        TEST_DATA_DIR / "golden_expected_register_only.csv",
        ttw_headers,
        ttw_rows,
    )
    print(f"  Golden register-only: {len(council_rows)} rows")
    print(f"  TTW columns preserved: {ttw_headers}")


# ---------------------------------------------------------------------------
# Dataset B: Golden register + elections
# ---------------------------------------------------------------------------

def generate_golden_register_plus_elections():
    """Generate golden input and expected output with election data.

    The golden expected output is extracted DIRECTLY from the TTW test data,
    preserving the exact column order and all columns (including empty ones).
    """
    ttw_headers, ttw_rows = read_ttw_csv(TTW_ELECTIONS, max_rows=20)

    # Golden input: council format + election columns + Suffix
    election_cols = [
        "2022 Green Voting Intention", "2022 Party", "2022 Voted",
        "2026 Green Voting Intention", "2026 Party", "2026 Postal Voter",
    ]
    council_rows = [ttw_row_to_council_elections(r) for r in ttw_rows]
    write_csv(
        TEST_DATA_DIR / "golden_input_register_plus_elections.csv",
        COUNCIL_HEADERS_WITH_SUFFIX + election_cols,
        council_rows,
    )

    # Golden expected: extracted directly from TTW test data
    # ALL columns preserved (including empty ones), exact column order
    write_csv(
        TEST_DATA_DIR / "golden_expected_register_plus_elections.csv",
        ttw_headers,
        ttw_rows,
    )
    print(f"  Golden register+elections: {len(council_rows)} rows")
    print(f"  TTW columns preserved: {ttw_headers}")


# ---------------------------------------------------------------------------
# Dataset C: Edge cases
# ---------------------------------------------------------------------------

def make_row(**overrides):
    """Create a council-format row with defaults, overridden by kwargs."""
    defaults = {
        "PDCode": "EBP1",
        "RollNo": "1",
        "ElectorTitle": "Mr",
        "ElectorSurname": "Smith",
        "ElectorForename": "John",
        "ElectorMiddleName": "",
        "IERStatus": "V",
        "DateOfAttainment": "",
        "FranchiseMarker": "E",
        "RegisteredAddress1": "42 Chamberlayne Road",
        "RegisteredAddress2": "London",
        "RegisteredAddress3": "",
        "RegisteredAddress4": "",
        "RegisteredAddress5": "",
        "RegisteredAddress6": "",
        "PostCode": "NW10 3JU",
        "Euro": "London",
        "Parl": "Brent North",
        "County": "",
        "Ward": "Kensal Green",
        "SubHouse": "",
        "House": "",
        "MethodOfVerification": "D",
        "ElectorID": "E1",
        "UPRN": "",
    }
    defaults.update(overrides)
    return defaults


def generate_edge_cases():
    """Generate edge case records for comprehensive testing."""
    rows = []

    # 1. No address (all address fields + PostCode empty) → should be deleted
    rows.append(make_row(
        RollNo="1", ElectorForename="Alice", ElectorSurname="NoAddress",
        RegisteredAddress1="", RegisteredAddress2="", PostCode="",
    ))

    # 2. Postcode only, no street → flagged, kept
    rows.append(make_row(
        RollNo="2", ElectorForename="Bob", ElectorSurname="PostcodeOnly",
        RegisteredAddress1="", RegisteredAddress2="", PostCode="NW6 7AA",
    ))

    # 3. Partial address (Address1 filled, PostCode empty) → flagged, kept
    rows.append(make_row(
        RollNo="3", ElectorForename="Carol", ElectorSurname="PartialAddr",
        RegisteredAddress1="15 Kilburn Lane", RegisteredAddress2="", PostCode="",
    ))

    # 4. Date DD/MM/YYYY → pass through as DD/MM/YYYY
    rows.append(make_row(
        RollNo="4", ElectorForename="David", ElectorSurname="DateDMY",
        DateOfAttainment="15/03/2008",
    ))

    # 5. Date YYYY-MM-DD → convert to DD/MM/YYYY
    rows.append(make_row(
        RollNo="5", ElectorForename="Eve", ElectorSurname="DateISO",
        DateOfAttainment="2008-03-15",
    ))

    # 6. Date empty → leave blank
    rows.append(make_row(
        RollNo="6", ElectorForename="Fiona", ElectorSurname="DateEmpty",
        DateOfAttainment="",
    ))

    # 7. Date invalid → blank + WARNING
    rows.append(make_row(
        RollNo="7", ElectorForename="George", ElectorSurname="DateInvalid",
        DateOfAttainment="not-a-date",
    ))

    # 8. Date unreasonable year → blank + WARNING
    rows.append(make_row(
        RollNo="8", ElectorForename="Hannah", ElectorSurname="DateOldYear",
        DateOfAttainment="15/03/1802",
    ))

    # 9. Decimal RollNo
    rows.append(make_row(
        RollNo="70.5", ElectorForename="Ivan", ElectorSurname="DecimalRoll",
    ))

    # 10. Non-ASCII name
    rows.append(make_row(
        RollNo="10", ElectorForename="Sean", ElectorSurname="O'Brien-Smythe",
        ElectorMiddleName="Jean-Pierre",
    ))

    # 11. SubHouse and House populated
    rows.append(make_row(
        RollNo="11", ElectorForename="Kate", ElectorSurname="WithSubHouse",
        SubHouse="Flat 3", House="Oak Manor",
        RegisteredAddress1="Oak Manor", RegisteredAddress2="21 Willesden Lane",
    ))

    # 12. Long address (all 6 fields populated)
    rows.append(make_row(
        RollNo="12", ElectorForename="Liam", ElectorSurname="LongAddress",
        RegisteredAddress1="Flat 4B",
        RegisteredAddress2="Victoria Mansions",
        RegisteredAddress3="23 Brondesbury Park",
        RegisteredAddress4="Kilburn",
        RegisteredAddress5="London",
        RegisteredAddress6="Greater London",
    ))

    # 13. PostCode with extra spaces
    rows.append(make_row(
        RollNo="13", ElectorForename="Maya", ElectorSurname="SpaceyPostcode",
        PostCode="  NR5   9LD  ",
    ))

    # 14. Comma in quoted surname
    rows.append(make_row(
        RollNo="14", ElectorForename="Noah", ElectorSurname="Smith, Jr",
    ))

    # 15. Missing Forename → flagged, kept
    rows.append(make_row(
        RollNo="15", ElectorForename="", ElectorSurname="NoForename",
    ))

    # 16. Missing Surname → flagged, kept
    rows.append(make_row(
        RollNo="16", ElectorForename="Olivia", ElectorSurname="",
    ))

    # 17. Ampersand address
    rows.append(make_row(
        RollNo="17", ElectorForename="Paul", ElectorSurname="AmpersandAddr",
        RegisteredAddress1="1ST & 2ND", RegisteredAddress2="9 Coleman Road",
    ))

    # 18. Comma building+road
    rows.append(make_row(
        RollNo="18", ElectorForename="Queenie", ElectorSurname="CommaAddr",
        RegisteredAddress1="3 Connell Court, Lindal Road",
    ))

    # 19. Bracket notation
    rows.append(make_row(
        RollNo="19", ElectorForename="Rachel", ElectorSurname="BracketAddr",
        RegisteredAddress1="[100-102] Coleman Road",
    ))

    # 20. Duplicate PDCode+RollNo (same as row 19)
    rows.append(make_row(
        RollNo="19", ElectorForename="Rachel", ElectorSurname="BracketAddr-Dup",
        RegisteredAddress1="[100-102] Coleman Road",
    ))

    # 21-25. Election data edge cases (add election columns)
    rows.append(make_row(
        RollNo="21", ElectorForename="Sam", ElectorSurname="VoteGreen",
        **{"2022 Green Voting Intention": "1", "2022 Party": "G", "2022 Voted": "v",
           "2026 Green Voting Intention": "1", "2026 Party": "G", "2026 Postal Voter": "v"},
    ))
    rows.append(make_row(
        RollNo="22", ElectorForename="Tina", ElectorSurname="BlankElection",
        **{"2022 Green Voting Intention": "", "2022 Party": "", "2022 Voted": "",
           "2026 Green Voting Intention": "", "2026 Party": "", "2026 Postal Voter": ""},
    ))
    rows.append(make_row(
        RollNo="23", ElectorForename="Uma", ElectorSurname="BadVoteIntent",
        **{"2022 Green Voting Intention": "7", "2022 Party": "G", "2022 Voted": "v",
           "2026 Green Voting Intention": "X", "2026 Party": "G", "2026 Postal Voter": ""},
    ))
    rows.append(make_row(
        RollNo="24", ElectorForename="Victor", ElectorSurname="UnknownParty",
        **{"2022 Green Voting Intention": "3", "2022 Party": "UKIP", "2022 Voted": "",
           "2026 Green Voting Intention": "3", "2026 Party": "SNP", "2026 Postal Voter": ""},
    ))
    rows.append(make_row(
        RollNo="25", ElectorForename="Wendy", ElectorSurname="VotedAny",
        **{"2022 Green Voting Intention": "", "2022 Party": "", "2022 Voted": "Y",
           "2026 Green Voting Intention": "", "2026 Party": "", "2026 Postal Voter": "Y"},
    ))

    # 26. All-empty optional fields
    rows.append(make_row(
        RollNo="26", ElectorForename="Xander", ElectorSurname="MinFields",
        ElectorTitle="", ElectorMiddleName="", DateOfAttainment="",
        RegisteredAddress3="", RegisteredAddress4="", RegisteredAddress5="",
        RegisteredAddress6="", UPRN="", SubHouse="", House="",
        IERStatus="", FranchiseMarker="",
    ))

    # 27-29. Multiple electors at same address (for per-address suffix testing)
    shared_addr = {
        "RegisteredAddress1": "Flat 1", "RegisteredAddress2": "88 Kilburn Lane",
        "PostCode": "NW6 5HT",
    }
    rows.append(make_row(RollNo="27", ElectorForename="Yuki", ElectorSurname="SameAddr1", **shared_addr))
    rows.append(make_row(RollNo="28", ElectorForename="Zara", ElectorSurname="SameAddr2", **shared_addr))
    rows.append(make_row(RollNo="29", ElectorForename="Amir", ElectorSurname="SameAddr3", **shared_addr))

    # 30. Only required fields, everything else empty
    rows.append(make_row(
        RollNo="30", ElectorForename="Beth", ElectorSurname="MinViable",
        ElectorTitle="", ElectorMiddleName="", DateOfAttainment="",
        RegisteredAddress1="1 High Road", RegisteredAddress2="London",
        RegisteredAddress3="", RegisteredAddress4="", RegisteredAddress5="",
        RegisteredAddress6="", PostCode="NW10 2AA",
        UPRN="", SubHouse="", House="", IERStatus="", FranchiseMarker="",
        Euro="", Parl="", County="", Ward="",
        MethodOfVerification="", ElectorID="",
    ))

    # 31. ALL CAPS name + ALL CAPS Address2 (name should fix, address should NOT)
    rows.append(make_row(
        RollNo="31", ElectorForename="JOHN", ElectorSurname="SMITH",
        RegisteredAddress1="42 Chamberlayne Road", RegisteredAddress2="LONDON",
        PostCode="NW10 3JU",
    ))

    # 32. lowercase name
    rows.append(make_row(
        RollNo="32", ElectorForename="jane", ElectorSurname="doe",
        RegisteredAddress1="44 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 33. ALL CAPS hyphenated name
    rows.append(make_row(
        RollNo="33", ElectorForename="JEAN-CLAUDE", ElectorSurname="VAN DAMME",
        RegisteredAddress1="46 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 34. ALL CAPS apostrophe name
    rows.append(make_row(
        RollNo="34", ElectorForename="SIOBHAN", ElectorSurname="O'BRIEN",
        RegisteredAddress1="48 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 35. Mc prefix ALL CAPS
    rows.append(make_row(
        RollNo="35", ElectorForename="ANGUS", ElectorSurname="MCDONALD",
        RegisteredAddress1="50 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 36. Mac prefix ALL CAPS
    rows.append(make_row(
        RollNo="36", ElectorForename="FIONA", ElectorSurname="MACDONALD",
        RegisteredAddress1="52 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 37. Mixed case (should NOT change)
    rows.append(make_row(
        RollNo="37", ElectorForename="Sarah", ElectorSurname="McDonald",
        RegisteredAddress1="54 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 38. Address gap (Addr1 filled, Addr2 empty, Addr3 filled)
    rows.append(make_row(
        RollNo="38", ElectorForename="Tom", ElectorSurname="GapAddr",
        RegisteredAddress1="Flat 5", RegisteredAddress2="",
        RegisteredAddress3="88 Kilburn Lane", PostCode="NW6 5HT",
    ))

    # 39. Flat comma with empty Address2 (should auto-split)
    rows.append(make_row(
        RollNo="39", ElectorForename="Lucy", ElectorSurname="FlatComma",
        RegisteredAddress1="Flat 7, 45 High Road", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 40. Flat comma with occupied Address2 (should NOT split, NOT warn)
    rows.append(make_row(
        RollNo="40", ElectorForename="Mark", ElectorSurname="FlatCommaOccupied",
        RegisteredAddress1="Flat 7, 45 High Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 41. Number before Flat with empty Address2 (should reorder)
    rows.append(make_row(
        RollNo="41", ElectorForename="Nina", ElectorSurname="NumFlat",
        RegisteredAddress1="56 Flat 1", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 42. Single-char remainder (should NOT reorder — "14 B" stays)
    rows.append(make_row(
        RollNo="42", ElectorForename="Olaf", ElectorSurname="SingleChar",
        RegisteredAddress1="14 B", RegisteredAddress2="Coleman Road",
        PostCode="NW10 3JU",
    ))

    # 43. Single-word remainder (should NOT reorder — "14 London" stays)
    rows.append(make_row(
        RollNo="43", ElectorForename="Petra", ElectorSurname="SingleWord",
        RegisteredAddress1="14 London", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 44. Comma-free flat+road: "Flat 3 30 Chamberlayne Road" -> split "Flat 3" / "30 Chamberlayne Road"
    rows.append(make_row(
        RollNo="44", ElectorForename="Quinn", ElectorSurname="CommaFreeFlatRoad",
        RegisteredAddress1="Flat 3 30 Chamberlayne Road", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 45. Comma-free flat+road without house number: "Flat 3 Chamberlayne Road" -> split
    rows.append(make_row(
        RollNo="45", ElectorForename="Rosa", ElectorSurname="CommaFreeFlatRoadNoNum",
        RegisteredAddress1="Flat 3 Chamberlayne Road", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 46. Flat + building name (no road suffix): "Flat 3 Ontario Point" -> NOT split
    rows.append(make_row(
        RollNo="46", ElectorForename="Steve", ElectorSurname="FlatBuilding",
        RegisteredAddress1="Flat 3 Ontario Point", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 47. Comma-free flat+road with occupied Address2: should NOT split
    rows.append(make_row(
        RollNo="47", ElectorForename="Tara", ElectorSurname="CommaFreeFlatOccupied",
        RegisteredAddress1="Flat 3 30 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU",
    ))

    # 48. Alphanumeric house number before building name: "14A South House" -> "South House 14A"
    rows.append(make_row(
        RollNo="48", ElectorForename="Uma", ElectorSurname="AlphaNumBuilding",
        RegisteredAddress1="14A South House", RegisteredAddress2="Coleman Road",
        PostCode="NW10 3JU",
    ))

    # 49. Alphanumeric house number before Flat: "14A Flat 1" -> "Flat 1" / "14A"
    rows.append(make_row(
        RollNo="49", ElectorForename="Vlad", ElectorSurname="AlphaNumFlat",
        RegisteredAddress1="14A Flat 1", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # 50. Long flat designation (multi-word flat ID) -> NOT split + advisory WARNING
    rows.append(make_row(
        RollNo="50", ElectorForename="Wren", ElectorSurname="LongFlatNoSplit",
        RegisteredAddress1="Flat Ground Floor 30 Chamberlayne Road", RegisteredAddress2="",
        PostCode="NW10 3JU",
    ))

    # Determine all headers needed (council headers + any election columns)
    election_cols = [
        "2022 Green Voting Intention", "2022 Party", "2022 Voted",
        "2026 Green Voting Intention", "2026 Party", "2026 Postal Voter",
    ]
    all_headers = COUNCIL_HEADERS + election_cols

    write_csv(TEST_DATA_DIR / "edge_cases.csv", all_headers, rows)
    print(f"  Edge cases: {len(rows)} rows")


# ---------------------------------------------------------------------------
# Dataset E: Realistic messy council data
# ---------------------------------------------------------------------------

def generate_realistic_messy_data():
    """Generate ~200 rows of realistic messy council electoral register data for Brent.

    Simulates the kind of data quality issues that appear in real council exports,
    based on the common problems described in UG C3 (Electoral Register Initial Upload).
    Most rows (~70%) are clean. The remainder have realistic issues mixed in naturally.

    Issues included (keyed to UG C3 slides):
    - Address: number before building name (slide 12 - invalid)
    - Address: number before Flat (slide 12 - invalid)
    - Address: ampersand (slide 12 - invalid)
    - Address: comma building+road (slide 12 - invalid)
    - Address: bracket notation (slide 10)
    - Address: gap — road in Address3, Address2 empty (slide 8 - invalid)
    - Address: valid flat comma format (slide 11 - valid)
    - Address: building name split across Address1/2 (slide 11 - valid)
    - No-address electors (slide 18 - should be deleted)
    - SubHouse/House populated with address data
    - Invalid Date of Attainment values (slide 6)
    - Missing forename or surname
    - Postcode with extra spaces or lowercase
    - Duplicate PDCode+RollNo
    - Multiple electors at same address (families)
    - Decimal RollNo
    - Non-ASCII names
    """
    rows = []
    roll = 0  # auto-increment

    def r(**overrides):
        nonlocal roll
        roll += 1
        base = {
            "PDCode": "KG1",
            "RollNo": str(roll),
            "ElectorTitle": "",
            "ElectorSurname": "",
            "ElectorForename": "",
            "ElectorMiddleName": "",
            "IERStatus": "V",
            "DateOfAttainment": "",
            "FranchiseMarker": "E",
            "RegisteredAddress1": "",
            "RegisteredAddress2": "",
            "RegisteredAddress3": "",
            "RegisteredAddress4": "",
            "RegisteredAddress5": "",
            "RegisteredAddress6": "",
            "PostCode": "",
            "Euro": "London",
            "Parl": "Brent East",
            "County": "",
            "Ward": "Kensal Green",
            "SubHouse": "",
            "House": "",
            "MethodOfVerification": "D",
            "ElectorID": f"E{roll}",
            "UPRN": "",
        }
        base.update(overrides)
        return base

    # ====================================================================
    # POLLING DISTRICT KG1 — Kensal Green (Chamberlayne Road area)
    # Mostly clean terrace houses, some flats
    # ====================================================================

    # --- Clean: simple number + road addresses ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Patel", ElectorForename="Raj",
        ElectorMiddleName="Kumar",
        RegisteredAddress1="1 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU", UPRN="100023456789"))
    rows.append(r(ElectorTitle="Mrs", ElectorSurname="Patel", ElectorForename="Priya",
        RegisteredAddress1="1 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU", UPRN="100023456789"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Williams", ElectorForename="David",
        ElectorMiddleName="John",
        RegisteredAddress1="3 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Okafor", ElectorForename="Chioma",
        RegisteredAddress1="5 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Cohen", ElectorForename="Daniel",
        RegisteredAddress1="7 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Begum", ElectorForename="Fatima",
        ElectorMiddleName="Noor",
        RegisteredAddress1="9 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Murphy", ElectorForename="Sean",
        RegisteredAddress1="11 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- Clean: number in Address1, road in Address2 (valid per slide 7) ---
    rows.append(r(ElectorTitle="Mrs", ElectorSurname="Adeyemi", ElectorForename="Bola",
        RegisteredAddress1="13", RegisteredAddress2="Chamberlayne Road",
        RegisteredAddress3="London", PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Hassan", ElectorForename="Mohammed",
        RegisteredAddress1="15", RegisteredAddress2="Chamberlayne Road",
        RegisteredAddress3="London", PostCode="NW10 3JU"))

    # --- Clean: mixed alphanumeric house number (valid per slide 8) ---
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Taylor", ElectorForename="Emma",
        RegisteredAddress1="17A Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Taylor", ElectorForename="James",
        RegisteredAddress1="17B Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- ISSUE: Number before building name (UG C3 slide 12 - INVALID) ---
    # TTW will misparse: "14 South House" → House Number=14, Road Name=South House
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Nowak", ElectorForename="Tomasz",
        RegisteredAddress1="14 South House", RegisteredAddress2="Chamberlayne Road",
        PostCode="NW10 3JU"))

    # --- ISSUE: Number before Flat (UG C3 slide 12 - INVALID) ---
    # TTW will misparse: "56 Flat 1" → House Number=56, Road Name=Flat 1
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Garcia", ElectorForename="Maria",
        RegisteredAddress1="56 Flat 1", RegisteredAddress2="Chamberlayne Road",
        PostCode="NW10 3JU"))

    # --- VALID: Building name on its own, number+road in Address2 (slide 11) ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Ahmed", ElectorForename="Tariq",
        RegisteredAddress1="South House", RegisteredAddress2="21 Chamberlayne Road",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mrs", ElectorSurname="Ahmed", ElectorForename="Aisha",
        RegisteredAddress1="South House", RegisteredAddress2="21 Chamberlayne Road",
        PostCode="NW10 3JU"))

    # --- VALID: Flat comma format (slide 11) ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Byrne", ElectorForename="Patrick",
        RegisteredAddress1="Flat 3, 30 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- ISSUE: Comma building+road (UG C3 slide 12 - INVALID) ---
    # TTW will misparse: only sees "Connell Court", loses "Lindal Road"
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Kowalski", ElectorForename="Anna",
        RegisteredAddress1="3 Connell Court, Lindal Road", RegisteredAddress2="London",
        PostCode="NW10 3ED"))

    # --- ISSUE: Ampersand address (UG C3 slide 12 - INVALID) ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Mensah", ElectorForename="Kwame",
        RegisteredAddress1="1ST & 2ND", RegisteredAddress2="9 Chamberlayne Road",
        PostCode="NW10 3JU"))

    # --- ISSUE: Address gap — road in Address3 (UG C3 slide 7/8 - INVALID) ---
    rows.append(r(ElectorTitle="Mrs", ElectorSurname="Singh", ElectorForename="Harpreet",
        RegisteredAddress1="25", RegisteredAddress2="",
        RegisteredAddress3="Chamberlayne Road", PostCode="NW10 3JU"))

    # --- ISSUE: Bracket notation (UG C3 slide 10) ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Ali", ElectorForename="Yusuf",
        RegisteredAddress1="[100-102]", RegisteredAddress2="Chamberlayne Road",
        PostCode="NW10 3JU"))

    # --- ISSUE: SubHouse/House populated (address data in extra fields) ---
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Fernandez", ElectorForename="Carmen",
        SubHouse="Flat 2", House="Regency Court",
        RegisteredAddress1="Regency Court", RegisteredAddress2="35 Chamberlayne Road",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Fernandez", ElectorForename="Diego",
        SubHouse="Flat 3", House="Regency Court",
        RegisteredAddress1="Regency Court", RegisteredAddress2="35 Chamberlayne Road",
        PostCode="NW10 3JU"))

    # --- Clean: more normal rows ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Thompson", ElectorForename="Mark",
        RegisteredAddress1="37 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Nguyen", ElectorForename="Linh",
        RegisteredAddress1="39 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Dr", ElectorSurname="Osei", ElectorForename="Kwabena",
        ElectorMiddleName="Adu",
        RegisteredAddress1="41 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Rahman", ElectorForename="Abdur",
        RegisteredAddress1="43 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- ISSUE: No address — elector for security reasons (UG C3 slide 18 - DELETE) ---
    rows.append(r(ElectorTitle="", ElectorSurname="Witness", ElectorForename="Protected",
        RegisteredAddress1="", RegisteredAddress2="", PostCode=""))

    # --- ISSUE: Postcode with extra spaces ---
    rows.append(r(ElectorTitle="Mrs", ElectorSurname="Brown", ElectorForename="Sarah",
        RegisteredAddress1="45 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="  NW10   3JU  "))

    # --- ISSUE: Postcode lowercase ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Kamara", ElectorForename="Ibrahim",
        RegisteredAddress1="47 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="nw10 3ju"))

    # --- ISSUE: Missing forename ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Abdi", ElectorForename="",
        RegisteredAddress1="49 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- ISSUE: Date of Attainment — invalid string ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Fletcher", ElectorForename="Tom",
        DateOfAttainment="PENDING",
        RegisteredAddress1="51 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- ISSUE: Date of Attainment — ISO format (needs conversion) ---
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Kone", ElectorForename="Aminata",
        DateOfAttainment="2008-06-15",
        RegisteredAddress1="53 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- Clean with valid Date of Attainment ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="Lewis", ElectorForename="Ryan",
        DateOfAttainment="15/03/2008",
        RegisteredAddress1="55 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # --- ISSUE: Decimal RollNo ---
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Dos Santos", ElectorForename="Ana",
        RollNo="35.5",
        RegisteredAddress1="57 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    roll -= 1  # manual RollNo, don't advance auto counter weirdly
    roll += 1

    # --- Non-ASCII names ---
    rows.append(r(ElectorTitle="Mr", ElectorSurname="O'Brien", ElectorForename="Ciaran",
        RegisteredAddress1="59 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))
    rows.append(r(ElectorTitle="Ms", ElectorSurname="Muller", ElectorForename="Helene",
        ElectorMiddleName="Marie-Claire",
        RegisteredAddress1="61 Chamberlayne Road", RegisteredAddress2="London",
        PostCode="NW10 3JU"))

    # ====================================================================
    # POLLING DISTRICT KG2 — Kensal Green (Kilburn Lane area)
    # More flats, converted houses
    # ====================================================================
    roll = 0  # Reset for new PD

    # --- Flat block: Kilburn Court, 10 Kilburn Lane ---
    for flat_n, (surname, forename, middle, title) in enumerate([
        ("Davies", "Rhys", "", "Mr"),
        ("Chowdhury", "Rafiq", "Iqbal", "Mr"),
        ("Johnson", "Tracy", "", "Ms"),
        ("Okonkwo", "Emeka", "Chukwu", "Mr"),
        ("Okonkwo", "Grace", "", "Mrs"),
        ("Blanc", "Sylvie", "Marie", "Ms"),
        ("Hossain", "Kamal", "", "Mr"),
        ("Dube", "Tendai", "", "Mr"),
    ], start=1):
        rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            ElectorMiddleName=middle,
            RegisteredAddress1=f"Flat {flat_n}", RegisteredAddress2="10 Kilburn Lane",
            RegisteredAddress3="London", PostCode="NW6 5HT"))

    # --- ISSUE: Same flat block, but SubHouse/House used instead of Address1 ---
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Rivera", ElectorForename="Sofia",
        SubHouse="Flat 9", House="Kilburn Court",
        RegisteredAddress1="Kilburn Court", RegisteredAddress2="10 Kilburn Lane",
        RegisteredAddress3="London", PostCode="NW6 5HT"))

    # --- Clean: terrace houses on Kilburn Lane ---
    for num, (surname, forename, title) in [
        (12, ("Abara", "Ngozi", "Ms")),
        (14, ("Walsh", "Kevin", "Mr")),
        (16, ("Begum", "Rashida", "Mrs")),
        (18, ("Phillips", "Chloe", "Ms")),
        (20, ("Diallo", "Oumar", "Mr")),
        (22, ("Clarke", "Beverley", "Mrs")),
        (24, ("Zaman", "Farhan", "Mr")),
        (26, ("Baptiste", "Jean-Claude", "Mr")),
    ]:
        rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1=f"{num} Kilburn Lane", RegisteredAddress2="London",
            PostCode="NW6 5HT"))

    # --- ISSUE: Building name with number+road across address fields (valid) ---
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Fitzpatrick", ElectorForename="Dermot",
        RegisteredAddress1="The Lodge", RegisteredAddress2="28 Kilburn Lane",
        PostCode="NW6 5HT"))

    # --- ISSUE: Building name split with number in Address2, road in Address3 (valid) ---
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Chen", ElectorForename="Mei-Ling",
        RegisteredAddress1="The Lodge", RegisteredAddress2="28",
        RegisteredAddress3="Kilburn Lane", PostCode="NW6 5HT"))

    # --- ISSUE: Duplicate PDCode+RollNo (accidental double entry) ---
    dup_roll = str(roll + 1)
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Bakare", ElectorForename="Oluwaseun",
        RegisteredAddress1="30 Kilburn Lane", RegisteredAddress2="London",
        PostCode="NW6 5HT"))
    saved_roll = rows[-1]["RollNo"]
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        RollNo=saved_roll,  # duplicate!
        ElectorTitle="Mr", ElectorSurname="Bakare", ElectorForename="Oluwaseun",
        RegisteredAddress1="30 Kilburn Lane", RegisteredAddress2="London",
        PostCode="NW6 5HT"))

    # --- ISSUE: Missing surname ---
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="", ElectorForename="Victoria",
        RegisteredAddress1="32 Kilburn Lane", RegisteredAddress2="London",
        PostCode="NW6 5HT"))

    # --- Clean: more normal rows ---
    for num, (surname, forename, title) in [
        (34, ("Owusu", "Akua", "Ms")),
        (36, ("Kelly", "Patrick", "Mr")),
        (38, ("Islam", "Tanvir", "Mr")),
        (40, ("Pryce", "Angela", "Mrs")),
        (42, ("Sow", "Mariama", "Ms")),
    ]:
        rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1=f"{num} Kilburn Lane", RegisteredAddress2="London",
            PostCode="NW6 5HT"))

    # --- ISSUE: No address — another protected elector ---
    rows.append(r(PDCode="KG2", Ward="Kensal Green", Parl="Brent East",
        ElectorTitle="", ElectorSurname="Anon", ElectorForename="Protected",
        RegisteredAddress1="", RegisteredAddress2="", PostCode=""))

    # ====================================================================
    # POLLING DISTRICT HP1 — Harlesden (Craven Park Road area)
    # Mix of houses and purpose-built flats
    # ====================================================================
    roll = 0

    # --- Clean rows on Craven Park Road ---
    for num, (surname, forename, title, middle) in [
        (1, ("Grant", "Michael", "Mr", "Anthony")),
        (3, ("Afolabi", "Tunde", "Mr", "")),
        (5, ("Hussain", "Imran", "Mr", "")),
        (7, ("Campbell", "Sharon", "Mrs", "Marie")),
        (9, ("Mensah", "Ama", "Ms", "")),
        (11, ("Morris", "Wayne", "Mr", "")),
        (13, ("Uddin", "Shamim", "Mrs", "")),
        (15, ("Owens", "Gareth", "Mr", "Huw")),
        (17, ("Adesanya", "Folake", "Ms", "")),
        (19, ("Reid", "Donna", "Ms", "")),
    ]:
        rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            ElectorMiddleName=middle,
            RegisteredAddress1=f"{num} Craven Park Road", RegisteredAddress2="London",
            PostCode="NW10 4AB"))

    # --- Family at same address ---
    for surname, forename, title in [
        ("Akinwale", "Biodun", "Mr"),
        ("Akinwale", "Folashade", "Mrs"),
        ("Akinwale", "Tobi", "Mr"),
    ]:
        rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1="21 Craven Park Road", RegisteredAddress2="London",
            PostCode="NW10 4AB"))

    # --- ISSUE: Date of Attainment — unreasonable year ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Walker", ElectorForename="Steven",
        DateOfAttainment="15/03/1850",
        RegisteredAddress1="23 Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- ISSUE: Date of Attainment — looks like DOB not attainment ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Okafor", ElectorForename="Adaeze",
        DateOfAttainment="22/11/1985",
        RegisteredAddress1="25 Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- ISSUE: Date of Attainment — DD.MM.YYYY format ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Jelani", ElectorForename="Kofi",
        DateOfAttainment="01.06.2009",
        RegisteredAddress1="27 Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- VALID: Building name then number+road (slide 11) ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Okeke", ElectorForename="Blessing",
        RegisteredAddress1="Maple House 4", RegisteredAddress2="29 Craven Park Road",
        PostCode="NW10 4AB"))

    # --- VALID: James Cottage pattern (slide 11) ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Whitfield", ElectorForename="Peter",
        RegisteredAddress1="James Cottage 52a", RegisteredAddress2="52 Craven Park Road",
        PostCode="NW10 4AB"))

    # --- ISSUE: Long address using all 6 fields ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Nkrumah", ElectorForename="Efua",
        RegisteredAddress1="Flat 12B",
        RegisteredAddress2="Harlesden Towers",
        RegisteredAddress3="31 Craven Park Road",
        RegisteredAddress4="Harlesden",
        RegisteredAddress5="London",
        RegisteredAddress6="Greater London",
        PostCode="NW10 4AB"))

    # --- Clean: more normal rows ---
    for num, (surname, forename, title) in [
        (33, ("Mwangi", "Joseph", "Mr")),
        (35, ("James", "Denise", "Mrs")),
        (37, ("Chandra", "Vikram", "Mr")),
        (39, ("Thomas", "Beverley", "Ms")),
        (41, ("Eze", "Chinedu", "Mr")),
    ]:
        rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1=f"{num} Craven Park Road", RegisteredAddress2="London",
            PostCode="NW10 4AB"))

    # --- ISSUE: ALL CAPS name (council data sometimes exported in ALL CAPS) ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="JOHNSON", ElectorForename="MICHAEL",
        RegisteredAddress1="43A Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- ISSUE: ALL CAPS apostrophe name ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="O'CONNOR", ElectorForename="SIOBHAN",
        RegisteredAddress1="43B Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- ISSUE: ALL CAPS Mc prefix name ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="MCKENZIE", ElectorForename="JAMES",
        RegisteredAddress1="43C Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- ISSUE: all lowercase name ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="van der berg", ElectorForename="anna",
        RegisteredAddress1="43D Craven Park Road", RegisteredAddress2="London",
        PostCode="NW10 4AB"))

    # --- ISSUE: Postcode-only, no street address (possible data error) ---
    rows.append(r(PDCode="HP1", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Doe", ElectorForename="John",
        RegisteredAddress1="", RegisteredAddress2="", PostCode="NW10 4AB"))

    # ====================================================================
    # POLLING DISTRICT HP2 — Harlesden (High Street / Station Road area)
    # Includes a large estate
    # ====================================================================
    roll = 0

    # --- Stonebridge Estate: Ontario Point flats ---
    for flat_n, (surname, forename, title) in enumerate([
        ("Wilson", "Tracey", "Ms"),
        ("Agyemang", "Yaw", "Mr"),
        ("Mohammed", "Halima", "Mrs"),
        ("Pinnock", "Desmond", "Mr"),
        ("Onyekachi", "Ijeoma", "Ms"),
        ("Stewart", "Leonard", "Mr"),
        ("Bah", "Mariama", "Ms"),
        ("Asante", "Kofi", "Mr"),
        ("Pierre", "Jean", "Mr"),
        ("Oyewole", "Bukola", "Ms"),
    ], start=1):
        rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1=f"Ontario Point {flat_n}",
            RegisteredAddress2="High Street",
            PostCode="NW10 4LX"))

    # --- ISSUE: Ontario Point with number BEFORE building name (INVALID per slide 12) ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Jallow", ElectorForename="Lamin",
        RegisteredAddress1="11 Ontario Point",
        RegisteredAddress2="High Street",
        PostCode="NW10 4LX"))

    # --- More clean rows on Station Road ---
    for num, (surname, forename, title) in [
        (2, ("Barker", "Christine", "Mrs")),
        (4, ("Ogunyemi", "Babatunde", "Mr")),
        (6, ("Robinson", "Lloyd", "Mr")),
        (8, ("Lawal", "Fatimah", "Ms")),
        (10, ("Edwards", "Clive", "Mr")),
        (12, ("Conteh", "Aminata", "Ms")),
        (14, ("Henry", "Claudette", "Mrs")),
        (16, ("Yeboah", "Kwasi", "Mr")),
    ]:
        rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1=f"{num} Station Road", RegisteredAddress2="Harlesden",
            RegisteredAddress3="London", PostCode="NW10 4UJ"))

    # --- ISSUE: Date of Attainment — various formats ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Opoku", ElectorForename="Daniel",
        DateOfAttainment="2026-05-15",
        RegisteredAddress1="18 Station Road", RegisteredAddress2="Harlesden",
        PostCode="NW10 4UJ"))
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Price", ElectorForename="Megan",
        DateOfAttainment="15-05-2026",
        RegisteredAddress1="20 Station Road", RegisteredAddress2="Harlesden",
        PostCode="NW10 4UJ"))
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Daniels", ElectorForename="Troy",
        DateOfAttainment="N/A",
        RegisteredAddress1="22 Station Road", RegisteredAddress2="Harlesden",
        PostCode="NW10 4UJ"))

    # --- ISSUE: Postcode missing space ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mrs", ElectorSurname="Amponsah", ElectorForename="Abena",
        RegisteredAddress1="24 Station Road", RegisteredAddress2="Harlesden",
        PostCode="NW104UJ"))

    # --- Clean: a few more normal rows ---
    for num, (surname, forename, title) in [
        (26, ("Lewis", "Pauline", "Mrs")),
        (28, ("Sesay", "Mohamed", "Mr")),
        (30, ("Carter", "Gloria", "Ms")),
        (32, ("Annan", "Emmanuel", "Mr")),
        (34, ("Duncan", "Sandra", "Mrs")),
        (36, ("Touray", "Alieu", "Mr")),
        (38, ("George", "Marcia", "Ms")),
        (40, ("Osei-Bonsu", "Akosua", "Ms")),
    ]:
        rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1=f"{num} Station Road", RegisteredAddress2="Harlesden",
            RegisteredAddress3="London", PostCode="NW10 4UJ"))

    # --- ISSUE: No address — third protected elector ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="", ElectorSurname="Redacted", ElectorForename="Name",
        RegisteredAddress1="", RegisteredAddress2="", PostCode=""))

    # --- ISSUE: Building with "Basement Flat" (UG C3 slide 10) ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Appiah", ElectorForename="Francis",
        RegisteredAddress1="Basement Flat", RegisteredAddress2="42 Station Road",
        PostCode="NW10 4UJ"))

    # --- ISSUE: Mixed alphanumeric in Address1, road in Address2 (valid, slide 8) ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Ms", ElectorSurname="Darko", ElectorForename="Esther",
        RegisteredAddress1="44B", RegisteredAddress2="Station Road",
        PostCode="NW10 4UJ"))

    # --- Family at end of road ---
    for surname, forename, title in [
        ("Adu", "Kwame", "Mr"),
        ("Adu", "Akua", "Mrs"),
        ("Adu", "Kofi", "Mr"),
        ("Adu", "Ama", "Ms"),
    ]:
        rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
            ElectorTitle=title, ElectorSurname=surname, ElectorForename=forename,
            RegisteredAddress1="46 Station Road", RegisteredAddress2="Harlesden",
            RegisteredAddress3="London", PostCode="NW10 4UJ"))

    # --- ISSUE: UPRN populated (rare but happens) ---
    rows.append(r(PDCode="HP2", Ward="Harlesden", Parl="Brent East",
        ElectorTitle="Mr", ElectorSurname="Fofana", ElectorForename="Moussa",
        RegisteredAddress1="48 Station Road", RegisteredAddress2="Harlesden",
        PostCode="NW10 4UJ", UPRN="10070834521"))

    write_csv(TEST_DATA_DIR / "realistic_messy_council_data.csv", COUNCIL_HEADERS, rows)

    # Count issues for summary
    no_addr = sum(1 for r in rows if not r["RegisteredAddress1"] and not r["RegisteredAddress2"] and not r["PostCode"])
    has_date = sum(1 for r in rows if r["DateOfAttainment"])
    has_subhouse = sum(1 for r in rows if r["SubHouse"] or r["House"])
    pds = set(r["PDCode"] for r in rows)

    print(f"  Realistic messy data: {len(rows)} rows across {len(pds)} polling districts")
    print(f"  No-address electors (should be deleted): {no_addr}")
    print(f"  Rows with Date of Attainment data: {has_date}")
    print(f"  Rows with SubHouse/House data: {has_subhouse}")


# ---------------------------------------------------------------------------
# Dataset D: Malformed files
# ---------------------------------------------------------------------------

def generate_malformed_files():
    """Generate deliberately broken CSV files for error handling tests."""

    # Missing required column (no RollNo)
    headers_missing = [h for h in COUNCIL_HEADERS if h != "RollNo"]
    row = make_row()
    with open(TEST_DATA_DIR / "malformed_missing_header.csv", "w",
              encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers_missing, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(row)

    # Empty file (header only, no data)
    with open(TEST_DATA_DIR / "malformed_empty.csv", "w",
              encoding="utf-8", newline="") as f:
        f.write(",".join(COUNCIL_HEADERS) + "\r\n")

    # Inconsistent field counts (too many commas on some rows)
    with open(TEST_DATA_DIR / "malformed_extra_commas.csv", "w",
              encoding="utf-8", newline="") as f:
        f.write(",".join(COUNCIL_HEADERS) + "\r\n")
        # Row with correct count
        f.write(",".join(["val"] * len(COUNCIL_HEADERS)) + "\r\n")
        # Row with too many fields
        f.write(",".join(["val"] * (len(COUNCIL_HEADERS) + 5)) + "\r\n")
        # Row with too few fields
        f.write(",".join(["val"] * 3) + "\r\n")

    # TTW format headers (for file-swap detection)
    ttw_headers = [
        "Elector No. Prefix", "Elector No.", "Elector No. Suffix",
        "Full Elector No.", "Forename", "Middle Names", "Surname",
        "Address1", "PostCode",
    ]
    with open(TEST_DATA_DIR / "malformed_ttw_format.csv", "w",
              encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ttw_headers)
        writer.writeheader()
        writer.writerow({h: "test" for h in ttw_headers})

    print("  Malformed files: 4 files")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(TEST_DATA_DIR, exist_ok=True)
    print("Generating test datasets...")

    print("\nDataset A: Golden register-only")
    generate_golden_register_only()

    print("\nDataset B: Golden register+elections")
    generate_golden_register_plus_elections()

    print("\nDataset C: Edge cases")
    generate_edge_cases()

    print("\nDataset E: Realistic messy council data")
    generate_realistic_messy_data()

    print("\nDataset D: Malformed files")
    generate_malformed_files()

    print(f"\nAll datasets written to {TEST_DATA_DIR}")


if __name__ == "__main__":
    main()
