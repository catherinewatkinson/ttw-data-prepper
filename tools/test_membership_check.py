#!/usr/bin/env python3
"""Test suite for check_membership_registration.py.

Usage:
    python3 tools/test_membership_check.py                          # All tests
    python3 tools/test_membership_check.py -v                       # Verbose
    python3 tools/test_membership_check.py TestExactMatching        # Single class

Uses stdlib unittest. Zero external dependencies.
"""

import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
TOOL = SCRIPT_DIR / "check_membership_registration.py"

# ---------------------------------------------------------------------------
# Field schemas — must match real data exactly
# ---------------------------------------------------------------------------

MEMBERSHIP_HEADERS = [
    "first_name", "last_name", "email", "can2_phone", "Home_Phone",
    "Member", "can2_user_address", "zip_code", "Ward",
    "Membership_Status", "Membership_Join_Date",
]

REGISTER_HEADERS = [
    "PDCode", "RollNo", "FranchiseMarker", "DateOfAttainment", "GE24",
    "New", "P/PB", "DNK", "1st round", "ElectorSurname", "ElectorForename",
    "Full Name", "RegisteredAddress1", "RegisteredAddress2",
    "RegisteredAddress3", "RegisteredAddress4", "RegisteredAddress5",
    "RegisteredAddress6", "PostCode", "Euro", "Parl", "Ward", "SubHouse",
    "House", "MethodOfVerification", "ElectorID", "UPRN", "Party", "1-5",
    "Comments", "Email Address", "Phone number", "Issues", "Identifier",
    "Address Identifier",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_check(membership_file, register_file, output_file,
              extra_args=None, report_file=None):
    """Run check_membership_registration.py as subprocess."""
    cmd = [sys.executable, str(TOOL),
           str(membership_file), str(register_file), str(output_file),
           "--quiet"]
    if report_file:
        cmd += ["--report", str(report_file)]
    if extra_args:
        cmd += extra_args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def write_temp_csv(rows, headers, encoding="utf-8-sig"):
    """Write rows to a temp CSV and return the path."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def read_output_csv(path):
    """Read output CSV and return (headers, rows)."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = list(reader)
    return headers, rows


def read_report(path):
    """Read QA report and return (full_text, machine_readable_lines)."""
    text = Path(path).read_text(encoding="utf-8")
    machine_lines = []
    in_section = False
    for line in text.splitlines():
        if line.strip() == "### MACHINE-READABLE SECTION ###":
            in_section = True
            continue
        if line.strip() == "### END MACHINE-READABLE SECTION ###":
            break
        if in_section and line.strip():
            machine_lines.append(line.strip())
    return text, machine_lines


def make_member(**overrides):
    """Factory for membership rows with Brent-realistic defaults."""
    base = {
        "first_name": "Priya",
        "last_name": "Patel",
        "email": "priya.patel@example.com",
        "can2_phone": "07700900123",
        "Home_Phone": "",
        "Member": "Y",
        "can2_user_address": "45 Chamberlayne Road, London, NW10 3JU",
        "zip_code": "NW10 3JU",
        "Ward": "Kensal Green",
        "Membership_Status": "Current",
        "Membership_Join_Date": "2024-01-15",
    }
    base.update(overrides)
    return base


def make_register_row(**overrides):
    """Factory for council-format register rows with Brent-realistic defaults."""
    base = {h: "" for h in REGISTER_HEADERS}
    base.update({
        "PDCode": "KG1",
        "RollNo": "1",
        "ElectorSurname": "Patel",
        "ElectorForename": "Priya",
        "Full Name": "Priya Patel",
        "RegisteredAddress1": "45 Chamberlayne Road",
        "RegisteredAddress2": "London",
        "PostCode": "NW10 3JU",
        "Ward": "Kensal Green",
    })
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------

class TestInputValidation(unittest.TestCase):
    """Test error handling for invalid inputs."""

    def test_missing_first_name_column(self):
        """Membership CSV missing first_name column."""
        bad_headers = [h for h in MEMBERSHIP_HEADERS if h != "first_name"]
        mem_path = write_temp_csv([{"last_name": "Test"}], bad_headers)
        reg_path = write_temp_csv([make_register_row()], REGISTER_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_check(mem_path, reg_path, out_path)
            self.assertNotEqual(rc, 0)
            self.assertIn("first_name", stderr)
        finally:
            for p in [mem_path, reg_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_missing_register_surname_column(self):
        """Register CSV missing any surname column."""
        bad_headers = [h for h in REGISTER_HEADERS if h != "ElectorSurname"]
        reg_path = write_temp_csv([{h: "" for h in bad_headers}], bad_headers)
        mem_path = write_temp_csv([make_member()], MEMBERSHIP_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_check(mem_path, reg_path, out_path)
            self.assertNotEqual(rc, 0)
            self.assertIn("surname", stderr.lower())
        finally:
            for p in [mem_path, reg_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_empty_membership_file(self):
        """Membership CSV with header but no data rows."""
        mem_path = write_temp_csv([], MEMBERSHIP_HEADERS)
        reg_path = write_temp_csv([make_register_row()], REGISTER_HEADERS)
        fd, out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rc, _, stderr = run_check(mem_path, reg_path, out_path)
            self.assertNotEqual(rc, 0)
            self.assertIn("empty", stderr.lower())
        finally:
            for p in [mem_path, reg_path, out_path]:
                if os.path.exists(p):
                    os.unlink(p)

    def test_overwrite_protection(self):
        """Output path same as input should fail."""
        mem_path = write_temp_csv([make_member()], MEMBERSHIP_HEADERS)
        reg_path = write_temp_csv([make_register_row()], REGISTER_HEADERS)
        try:
            rc, _, stderr = run_check(mem_path, reg_path, mem_path)
            self.assertNotEqual(rc, 0)
            self.assertIn("overwrite", stderr.lower())
        finally:
            for p in [mem_path, reg_path]:
                if os.path.exists(p):
                    os.unlink(p)


class TestExactMatching(unittest.TestCase):
    """Test straightforward match/no-match cases with council-format register."""

    @classmethod
    def setUpClass(cls):
        # Members: 3 members — 1 in register, 1 not, 1 different name
        cls.members = [
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU"),
            make_member(first_name="Oluwaseun", last_name="Adeyemi",
                        email="olu@example.com", zip_code="NW10 3JU",
                        can2_user_address="88 High Road Willesden, NW10 3JU"),
            make_member(first_name="Sean", last_name="Murphy",
                        email="sean@example.com", zip_code="NW2 4PJ",
                        can2_user_address="12 Walm Lane, NW2 4PJ"),
        ]
        # Register: only contains Priya Patel and Sean Murphy
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU",
                              RegisteredAddress1="45 Chamberlayne Road"),
            make_register_row(PDCode="HP1", RollNo="5",
                              ElectorSurname="Murphy", ElectorForename="Sean",
                              PostCode="NW2 4PJ",
                              RegisteredAddress1="12 Walm Lane"),
            make_register_row(PDCode="KG1", RollNo="3",
                              ElectorSurname="Cohen", ElectorForename="David",
                              PostCode="NW10 3JU",
                              RegisteredAddress1="50 Chamberlayne Road"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, cls.stdout, cls.stderr = run_check(
            cls.mem_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_exit_code(self):
        self.assertEqual(self.rc, 0, self.stderr)

    def test_matched_member_excluded(self):
        """Priya Patel is in register — should NOT appear in output."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Priya", "Patel"), names)

    def test_unmatched_member_in_output(self):
        """Oluwaseun Adeyemi is NOT in register — should appear in output."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertIn(("Oluwaseun", "Adeyemi"), names)

    def test_matched_member_sean_excluded(self):
        """Sean Murphy IS in register — should NOT appear in output."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Sean", "Murphy"), names)

    def test_output_row_count(self):
        """Only 1 member should be in output (Adeyemi)."""
        self.assertEqual(len(self.rows), 1)

    def test_match_status_present(self):
        """Output rows have Match_Status column."""
        self.assertIn("Match_Status", self.headers)
        self.assertEqual(self.rows[0]["Match_Status"], "unmatched")

    def test_best_candidate_present(self):
        """Output rows have Best_Candidate column."""
        self.assertIn("Best_Candidate", self.headers)


class TestFuzzyNameMatching(unittest.TestCase):
    """Test fuzzy matching with name variants."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            # Case difference — should match
            make_member(first_name="PRIYA", last_name="PATEL", zip_code="NW10 3JU"),
            # Typo in surname — should still match (high Dice similarity)
            make_member(first_name="David", last_name="Cohan",
                        zip_code="NW10 3JU", email="d@example.com"),
            # Completely different name — should NOT match
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
            # Hyphenated name vs non-hyphenated
            make_member(first_name="Sarah", last_name="Smith-Jones",
                        zip_code="NW2 4PJ", email="s@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
            make_register_row(PDCode="KG1", RollNo="2",
                              ElectorSurname="Cohen", ElectorForename="David",
                              PostCode="NW10 3JU"),
            make_register_row(PDCode="HP1", RollNo="1",
                              ElectorSurname="Smith-Jones", ElectorForename="Sarah",
                              PostCode="NW2 4PJ"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, _, cls.stderr = run_check(
            cls.mem_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, cls.machine = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_case_difference_matches(self):
        """PRIYA PATEL should match Priya Patel (case-insensitive)."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("PRIYA", "PATEL"), names)

    def test_typo_is_possible_match(self):
        """David Cohan vs David Cohen: score=0.7, below 0.8 threshold — appears as possible match."""
        cohan_rows = [r for r in self.rows
                      if r["first_name"] == "David" and r["last_name"] == "Cohan"]
        self.assertEqual(len(cohan_rows), 1)
        self.assertEqual(cohan_rows[0]["Match_Status"], "possible")

    def test_different_name_unmatched(self):
        """Kenji Tanaka has no match — should be in output."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertIn(("Kenji", "Tanaka"), names)

    def test_hyphenated_name_matches(self):
        """Sarah Smith-Jones should match exact hyphenated name."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Sarah", "Smith-Jones"), names)


class TestPostcodeHandling(unittest.TestCase):
    """Test postcode edge cases."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            # No zip_code but postcode in address
            make_member(first_name="Ade", last_name="Okafor",
                        zip_code="",
                        can2_user_address="22 Craven Park Road, London, NW10 4AB",
                        email="ade@example.com"),
            # Completely missing postcode
            make_member(first_name="Li", last_name="Wei",
                        zip_code="", can2_user_address="Somewhere in London",
                        email="li@example.com"),
            # Out-of-area postcode (not in register at all)
            make_member(first_name="Tom", last_name="Brown",
                        zip_code="SW1A 1AA",
                        can2_user_address="10 Downing Street, SW1A 1AA",
                        email="tom@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Okafor", ElectorForename="Ade",
                              PostCode="NW10 4AB",
                              RegisteredAddress1="22 Craven Park Road"),
            make_register_row(PDCode="KG1", RollNo="2",
                              ElectorSurname="Wei", ElectorForename="Li",
                              PostCode="NW10 3JU",
                              RegisteredAddress1="5 High Road"),
            make_register_row(PDCode="KG1", RollNo="3",
                              ElectorSurname="Begum", ElectorForename="Fatima",
                              PostCode="NW10 3JU",
                              RegisteredAddress1="10 High Road"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, _, cls.stderr = run_check(
            cls.mem_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, cls.machine = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_postcode_from_address_matches(self):
        """Ade Okafor: no zip_code but postcode in can2_user_address — should match."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Ade", "Okafor"), names)

    def test_no_postcode_member(self):
        """Li Wei: no postcode at all — falls back to full scan at 0.95 threshold."""
        # Li Wei IS in the register but at NW10 3JU — with no postcode,
        # full scan should find her with exact name match (score=1.0 > 0.95)
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Li", "Wei"), names)

    def test_out_of_area(self):
        """Tom Brown: SW1A 1AA not in register — should be out_of_area."""
        tom_rows = [r for r in self.rows
                    if r["first_name"] == "Tom" and r["last_name"] == "Brown"]
        self.assertEqual(len(tom_rows), 1)
        self.assertEqual(tom_rows[0]["Match_Status"], "out_of_area")

    def test_out_of_area_in_machine_readable(self):
        """Out-of-area entry appears in machine-readable report."""
        oa_lines = [l for l in self.machine if l.startswith("OUT_OF_AREA")]
        self.assertTrue(any("Tom Brown" in l for l in oa_lines))


class TestAddressTiebreaker(unittest.TestCase):
    """Test address similarity as tiebreaker for same-name candidates."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="John", last_name="Smith",
                        zip_code="NW10 3JU",
                        can2_user_address="45 Chamberlayne Road, NW10 3JU"),
        ]
        # Two John Smiths at same postcode, different addresses
        cls.register = [
            make_register_row(ElectorSurname="Smith", ElectorForename="John",
                              PostCode="NW10 3JU",
                              RegisteredAddress1="45 Chamberlayne Road"),
            make_register_row(PDCode="KG1", RollNo="2",
                              ElectorSurname="Smith", ElectorForename="John",
                              PostCode="NW10 3JU",
                              RegisteredAddress1="88 High Road Willesden"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, _, cls.stderr = run_check(
            cls.mem_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, cls.machine = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_address_tiebreaker_resolves_match(self):
        """John Smith at 45 Chamberlayne Rd should match, not be ambiguous."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("John", "Smith"), names,
                         "John Smith should be matched (address tiebreaker), not in output")

    def test_matched_in_report(self):
        """Confident match should appear in machine-readable section."""
        matched = [l for l in self.machine if l.startswith("MATCHED")]
        self.assertTrue(any("John Smith" in l for l in matched))


class TestDuplicateMembers(unittest.TestCase):
    """Test handling of duplicate names in membership list."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="priya1@example.com"),
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="priya2@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.rc, _, cls.stderr = run_check(cls.mem_path, cls.reg_path, cls.out_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_both_duplicates_matched(self):
        """Both Priya Patels match the register entry — neither in output."""
        self.assertEqual(len(self.rows), 0)


class TestOutputFormat(unittest.TestCase):
    """Test output CSV format and encoding."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.rc, _, cls.stderr = run_check(cls.mem_path, cls.reg_path, cls.out_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_all_membership_fields_preserved(self):
        """Output contains all original membership columns."""
        for field in MEMBERSHIP_HEADERS:
            self.assertIn(field, self.headers,
                          f"Missing membership field: {field}")

    def test_match_status_column(self):
        self.assertIn("Match_Status", self.headers)

    def test_best_candidate_column(self):
        self.assertIn("Best_Candidate", self.headers)

    def test_utf8_bom(self):
        """Output file should have UTF-8 BOM."""
        with open(self.out_path, "rb") as f:
            bom = f.read(3)
        self.assertEqual(bom, b"\xef\xbb\xbf")

    def test_original_data_preserved(self):
        """Membership data values should be unchanged in output."""
        row = self.rows[0]
        self.assertEqual(row["first_name"], "Kenji")
        self.assertEqual(row["last_name"], "Tanaka")
        self.assertEqual(row["email"], "priya.patel@example.com")  # default from factory


class TestQAReport(unittest.TestCase):
    """Test QA report generation."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Priya", last_name="Patel", zip_code="NW10 3JU"),
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
            make_member(first_name="Tom", last_name="Brown",
                        zip_code="SW1A 1AA", email="t@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, _, cls.stderr = run_check(
            cls.mem_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.report_text, cls.machine = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_report_created(self):
        self.assertTrue(os.path.exists(self.report_path))

    def test_report_has_timestamp(self):
        self.assertIn("Date:", self.report_text)

    def test_report_has_summary(self):
        self.assertIn("Total members: 3", self.report_text)
        self.assertIn("Matched (in register): 1", self.report_text)

    def test_report_has_machine_readable(self):
        self.assertTrue(len(self.machine) > 0)

    def test_matched_in_machine_section(self):
        matched = [l for l in self.machine if l.startswith("MATCHED")]
        self.assertTrue(any("Priya Patel" in l for l in matched))

    def test_out_of_area_in_machine_section(self):
        oa = [l for l in self.machine if l.startswith("OUT_OF_AREA")]
        self.assertTrue(any("Tom Brown" in l for l in oa))

    def test_report_has_note(self):
        self.assertIn("different name", self.report_text.lower())


class TestStrictMode(unittest.TestCase):
    """Test --strict flag excludes possible matches."""

    @classmethod
    def setUpClass(cls):
        # Create a member whose name is somewhat similar but not confident
        cls.members = [
            make_member(first_name="Priyanka", last_name="Patel",
                        zip_code="NW10 3JU"),
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)

        # Run without --strict
        fd, cls.out_default = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        run_check(cls.mem_path, cls.reg_path, cls.out_default)
        _, cls.rows_default = read_output_csv(cls.out_default)

        # Run with --strict
        fd, cls.out_strict = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        run_check(cls.mem_path, cls.reg_path, cls.out_strict,
                  extra_args=["--strict"])
        _, cls.rows_strict = read_output_csv(cls.out_strict)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_default, cls.out_strict]:
            if os.path.exists(p):
                os.unlink(p)

    def test_strict_has_fewer_or_equal_rows(self):
        """--strict should produce fewer or equal rows (excludes possible matches)."""
        self.assertLessEqual(len(self.rows_strict), len(self.rows_default))

    def test_strict_no_possible_status(self):
        """--strict output should have no 'possible' Match_Status."""
        statuses = [r.get("Match_Status") for r in self.rows_strict]
        self.assertNotIn("possible", statuses)


class TestUnicodeNames(unittest.TestCase):
    """Test handling of unicode/accented characters."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Zuzana", last_name="Bláhová",
                        zip_code="NW10 3JU", email="z@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Bláhová", ElectorForename="Zuzana",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.rc, _, cls.stderr = run_check(cls.mem_path, cls.reg_path, cls.out_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_unicode_name_matches(self):
        """Accented name should match exactly."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Zuzana", "Bláhová"), names)

    def test_exit_code(self):
        self.assertEqual(self.rc, 0, self.stderr)


if __name__ == "__main__":
    unittest.main()
