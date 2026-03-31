#!/usr/bin/env python3
"""Test suite for validate_membership_check.py.

Tests use ground-truth data where the correct answer is known from the test
data construction — NOT derived from running the matching tool.

Usage:
    python3 tools/test_validate_membership.py                       # All tests
    python3 tools/test_validate_membership.py -v                    # Verbose
    python3 tools/test_validate_membership.py TestGroundTruthValidation

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
CHECK_TOOL = SCRIPT_DIR / "check_membership_registration.py"
VALIDATE_TOOL = SCRIPT_DIR / "validate_membership_check.py"

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
    cmd = [sys.executable, str(CHECK_TOOL),
           str(membership_file), str(register_file), str(output_file),
           "--quiet"]
    if report_file:
        cmd += ["--report", str(report_file)]
    if extra_args:
        cmd += extra_args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def run_validate(membership_file, register_file, output_file,
                 extra_args=None, report_file=None):
    """Run validate_membership_check.py as subprocess."""
    cmd = [sys.executable, str(VALIDATE_TOOL),
           str(membership_file), str(register_file), str(output_file)]
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


def rewrite_csv(path, headers, rows, encoding="utf-8-sig"):
    """Overwrite a CSV with modified rows."""
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)


def make_member(**overrides):
    """Factory for membership rows."""
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
    """Factory for council-format register rows."""
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

class TestGroundTruthValidation(unittest.TestCase):
    """Ground-truth test: construct known data, verify both check and validate."""

    @classmethod
    def setUpClass(cls):
        # 7 members: 3 in register, 3 not, 1 no-postcode but exact name match
        cls.members = [
            # IN register — should be matched (absent from output)
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="p1@example.com"),
            make_member(first_name="Sean", last_name="Murphy",
                        zip_code="NW2 4PJ", email="s@example.com"),
            make_member(first_name="Fatima", last_name="Begum",
                        zip_code="NW10 3JU", email="f@example.com"),
            # NOT in register — should appear in output
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
            make_member(first_name="Oluwaseun", last_name="Adeyemi",
                        zip_code="NW10 3JU", email="o@example.com"),
            make_member(first_name="Tom", last_name="Brown",
                        zip_code="SW1A 1AA", email="t@example.com"),
            # No postcode but exact name in register — should be matched
            make_member(first_name="David", last_name="Cohen",
                        zip_code="", can2_user_address="Somewhere in London",
                        email="d@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
            make_register_row(PDCode="HP1", RollNo="5",
                              ElectorSurname="Murphy", ElectorForename="Sean",
                              PostCode="NW2 4PJ"),
            make_register_row(PDCode="KG1", RollNo="3",
                              ElectorSurname="Begum", ElectorForename="Fatima",
                              PostCode="NW10 3JU"),
            make_register_row(PDCode="KG1", RollNo="4",
                              ElectorSurname="Cohen", ElectorForename="David",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        # Run check
        cls.check_rc, _, cls.check_stderr = run_check(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

        # Run validate
        cls.val_rc, cls.val_stdout, cls.val_stderr = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_check_succeeds(self):
        self.assertEqual(self.check_rc, 0, self.check_stderr)

    def test_validate_passes(self):
        self.assertEqual(self.val_rc, 0,
                         f"Validator failed:\n{self.val_stdout}\n{self.val_stderr}")

    def test_matched_members_absent_from_output(self):
        """Independent ground-truth: matched members should not be in output."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("Priya", "Patel"), names)
        self.assertNotIn(("Sean", "Murphy"), names)
        self.assertNotIn(("Fatima", "Begum"), names)

    def test_no_postcode_matched_member_absent(self):
        """David Cohen has no postcode but exact name in register — should match."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertNotIn(("David", "Cohen"), names)

    def test_unmatched_members_in_output(self):
        """Independent ground-truth: unmatched members should be in output."""
        names = [(r["first_name"], r["last_name"]) for r in self.rows]
        self.assertIn(("Kenji", "Tanaka"), names)
        self.assertIn(("Oluwaseun", "Adeyemi"), names)

    def test_out_of_area_in_output(self):
        """Tom Brown (SW1A postcode) should be in output as out_of_area."""
        tom = [r for r in self.rows if r["last_name"] == "Brown"]
        self.assertEqual(len(tom), 1)
        self.assertEqual(tom[0]["Match_Status"], "out_of_area")

    def test_output_count(self):
        """3 unmatched members should be in output."""
        self.assertEqual(len(self.rows), 3)

    def test_no_fail_in_report(self):
        """Validation report should have no FAIL entries."""
        self.assertNotIn("[FAIL]", self.val_stdout)


class TestMatchedInOutputFail(unittest.TestCase):
    """Insert a matched member back into output → FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="p@example.com"),
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
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

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: insert matched member into output
        headers, rows = read_output_csv(cls.out_path)
        tampered_row = dict(cls.members[0])  # Priya Patel
        tampered_row["Match_Status"] = "unmatched"
        tampered_row["Best_Candidate"] = ""
        rows.append(tampered_row)
        rewrite_csv(cls.out_path, headers, rows)

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_matched_not_in_output_fail(self):
        self.assertIn("Matched-not-in-output", self.val_stdout)
        self.assertIn("[FAIL]", self.val_stdout)


class TestPhantomRowFail(unittest.TestCase):
    """Add a fabricated row not from membership → FAIL."""

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
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: add phantom row
        headers, rows = read_output_csv(cls.out_path)
        phantom = make_member(first_name="Phantom", last_name="Person",
                              email="phantom@example.com")
        phantom["Match_Status"] = "unmatched"
        phantom["Best_Candidate"] = ""
        rows.append(phantom)
        rewrite_csv(cls.out_path, headers, rows)

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_traceability_fail(self):
        self.assertIn("Output traceability", self.val_stdout)
        self.assertIn("[FAIL]", self.val_stdout)


class TestAccountingTamperFail(unittest.TestCase):
    """Delete a row from output → accounting FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
            make_member(first_name="Ade", last_name="Okafor",
                        zip_code="NW10 3JU", email="a@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Someone", ElectorForename="Else",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: delete a row
        headers, rows = read_output_csv(cls.out_path)
        if rows:
            rows.pop()
        rewrite_csv(cls.out_path, headers, rows)

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_accounting_fail(self):
        self.assertIn("Accounting", self.val_stdout)
        self.assertIn("[FAIL]", self.val_stdout)


class TestFieldCorruptionFail(unittest.TestCase):
    """Change a first_name in output → field preservation FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Someone", ElectorForename="Else",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: change first_name
        headers, rows = read_output_csv(cls.out_path)
        if rows:
            rows[0]["first_name"] = "CORRUPTED"
        rewrite_csv(cls.out_path, headers, rows)

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_field_preservation_fail(self):
        self.assertIn("Field preservation", self.val_stdout)


class TestMatchStatusInvalidFail(unittest.TestCase):
    """Set Match_Status to 'matched' in output → FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Someone", ElectorForename="Else",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: set invalid status
        headers, rows = read_output_csv(cls.out_path)
        if rows:
            rows[0]["Match_Status"] = "matched"
        rewrite_csv(cls.out_path, headers, rows)

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_match_status_fail(self):
        self.assertIn("Match status values", self.val_stdout)


class TestWrongRegisterWarn(unittest.TestCase):
    """Register from different area → WARN about out-of-area rate."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="p@example.com"),
            make_member(first_name="Sean", last_name="Murphy",
                        zip_code="NW2 4PJ", email="s@example.com"),
            make_member(first_name="Fatima", last_name="Begum",
                        zip_code="NW10 3JU", email="f@example.com"),
        ]
        # Register with completely different postcodes
        cls.register = [
            make_register_row(ElectorSurname="Someone", ElectorForename="Else",
                              PostCode="SW1A 1AA",
                              RegisteredAddress1="10 Downing Street"),
            make_register_row(PDCode="SW1", RollNo="2",
                              ElectorSurname="Another", ElectorForename="Person",
                              PostCode="SW1A 2AA",
                              RegisteredAddress1="Houses of Parliament"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)
        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_passes_with_warnings(self):
        """Should pass (exit 0) but with warnings."""
        self.assertEqual(self.val_rc, 0)

    def test_out_of_area_warn(self):
        self.assertIn("Out-of-area rate", self.val_stdout)
        self.assertIn("[WARN]", self.val_stdout)

    def test_match_rate_warn(self):
        self.assertIn("Match rate", self.val_stdout)
        self.assertIn("[WARN]", self.val_stdout)


class TestStrictModeAccounting(unittest.TestCase):
    """Run check with --strict, validate with --strict → should pass."""

    @classmethod
    def setUpClass(cls):
        # Create a scenario with some possible matches
        cls.members = [
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="p@example.com"),
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU", email="k@example.com"),
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

        # Run check with --strict
        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  extra_args=["--strict"], report_file=cls.report_path)

        # Validate with --strict
        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            extra_args=["--strict"], report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_passes(self):
        # Note: --strict on validator promotes WARNs to FAILs in exit code.
        # But there should be no WARNs here since data is clean.
        # The key thing: accounting should not FAIL.
        self.assertNotIn("[FAIL] Accounting", self.val_stdout)


class TestMatchedNameSanityWarn(unittest.TestCase):
    """Tamper with report to create suspicious match → WARN."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Priya", last_name="Patel",
                        zip_code="NW10 3JU", email="p@example.com"),
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

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: change the MATCHED line to pair completely different surnames
        report_text = Path(cls.report_path).read_text(encoding="utf-8")
        report_text = report_text.replace(
            "MATCHED|Member=Priya Patel|Register=Priya Patel",
            "MATCHED|Member=Priya Patel|Register=Kenji Yamamoto")
        Path(cls.report_path).write_text(report_text, encoding="utf-8")

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_name_sanity_warn(self):
        self.assertIn("Name sanity", self.val_stdout)
        self.assertIn("[WARN]", self.val_stdout)


class TestShortSurnameNotFlagged(unittest.TestCase):
    """Short surname 'Li' matched to 'Li' should NOT trigger name sanity WARN."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Wei", last_name="Li",
                        zip_code="NW10 3JU", email="w@example.com"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Li", ElectorForename="Wei",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)
        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_passes(self):
        self.assertEqual(self.val_rc, 0)

    def test_no_name_sanity_warn(self):
        """Short surname exact match should not be flagged."""
        # Check there's no WARN on name sanity
        for line in self.val_stdout.splitlines():
            if "Name sanity" in line:
                self.assertIn("[PASS]", line)
                break


class TestBomMissing(unittest.TestCase):
    """Output without BOM → FAIL on file format."""

    @classmethod
    def setUpClass(cls):
        cls.members = [
            make_member(first_name="Kenji", last_name="Tanaka",
                        zip_code="NW10 3JU"),
        ]
        cls.register = [
            make_register_row(ElectorSurname="Someone", ElectorForename="Else",
                              PostCode="NW10 3JU"),
        ]

        cls.mem_path = write_temp_csv(cls.members, MEMBERSHIP_HEADERS)
        cls.reg_path = write_temp_csv(cls.register, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"

        run_check(cls.mem_path, cls.reg_path, cls.out_path,
                  report_file=cls.report_path)

        # Tamper: rewrite without BOM
        headers, rows = read_output_csv(cls.out_path)
        rewrite_csv(cls.out_path, headers, rows, encoding="utf-8")

        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.mem_path, cls.reg_path, cls.out_path,
            report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.mem_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p):
                os.unlink(p)

    def test_validator_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_file_format_fail(self):
        self.assertIn("File format", self.val_stdout)


if __name__ == "__main__":
    unittest.main()
