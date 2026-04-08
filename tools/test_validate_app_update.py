#!/usr/bin/env python3
"""Test suite for validate_app_update.py.

Uses ground-truth data — runs update_app_export.py then validates the output.

Usage:
    python3 tools/test_validate_app_update.py -v
    python -m pytest tools/test_validate_app_update.py -v
"""

import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
UPDATE_TOOL = SCRIPT_DIR / "update_app_export.py"
VALIDATE_TOOL = SCRIPT_DIR / "validate_app_update.py"

TEST_DATE = "2026-Mar-31"

LE2026 = "Brent London Borough Council election (2026-May-07)"
GE2024 = "Brent London Borough Council election (2024-Jul-04)"

APP_EXPORT_HEADERS = [
    "Voter Number", "First Name", "Middle Name", "Surname",
    "Date of Attainment", "Date Entered onto Register", "Voter UUID",
    "House Name", "House Number", "Road", "Post Code", "Status",
    "Casework Phone Number", "Casework Email Address",
    "Date of Note 1 (most recent)", "Text of Note 1",
    "Date of Note 2", "Text of Note 2",
    "Date of Note 3", "Text of Note 3",
    "Date of Note 4", "Text of Note 4",
    "Date of Note 5", "Text of Note 5",
    "Date of Note 6", "Text of Note 6",
    "Date of Note 7", "Text of Note 7",
    "Date of Note 8", "Text of Note 8",
    "Date of Note 9", "Text of Note 9",
    "Date of Note 10", "Text of Note 10",
    "Casework ticked", "Deliver Leaflets ticked",
    "Poster ticked", "Board ticked", "Candidate ticked",
    "Do Not Knock ticked", "No Longer at Address ticked",
    "Member ticked", "Prospective Member ticked",
    "Most Recent Attempt - Date", "Most Recent Attempt - Answered",
    "Most Recent Attempt - Canvasser",
    "Previous 1 - Date", "Previous 1 - Answered", "Previous 1 - Canvasser",
    "Previous 2 - Date", "Previous 2 - Answered", "Previous 2 - Canvasser",
    "Previous 3 - Date", "Previous 3 - Answered", "Previous 3 - Canvasser",
    "Previous 4 - Date", "Previous 4 - Answered", "Previous 4 - Canvasser",
    f"{LE2026} Most Recent Data - Date",
    f"{LE2026} Most Recent Data - GVI",
    f"{LE2026} Most Recent Data - Usual Party",
    f"{LE2026} Most Recent Data - Postal Voter",
    f"{LE2026} Previous Data 1 - Date",
    f"{LE2026} Previous Data 1 - GVI",
    f"{LE2026} Previous Data 1 - Usual Party",
    f"{LE2026} Previous Data 2 - Date",
    f"{LE2026} Previous Data 2 - GVI",
    f"{LE2026} Previous Data 2 - Usual Party",
    f"{LE2026} Previous Data 3 - Date",
    f"{LE2026} Previous Data 3 - GVI",
    f"{LE2026} Previous Data 3 - Usual Party",
    f"{LE2026} Previous Data 4 - Date",
    f"{LE2026} Previous Data 4 - GVI",
    f"{LE2026} Previous Data 4 - Usual Party",
    f"{GE2024} Date", f"{GE2024} GVI",
    f"{GE2024} Usual Party", f"{GE2024} Voted",
    f"{GE2024} Postal Voter",
]

REGISTER_HEADERS = [
    "PDCode", "RollNo", "FranchiseMarker", "DateOfAttainment", "GE24",
    "PostalVoter?", "New", "P/PB", "DNK", "1st round",
    "ElectorSurname", "ElectorForename", "Full name",
    "RegisteredAddress1", "RegisteredAddress2", "RegisteredAddress3",
    "RegisteredAddress4", "RegisteredAddress5", "RegisteredAddress6",
    "PostCode", "Party", "1-5", "Comments", "Email Address",
    "Phone number", "Issues",
]


def write_temp_csv(rows, headers, encoding="utf-8-sig"):
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def read_output_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def make_app_row(**overrides):
    base = {h: "" for h in APP_EXPORT_HEADERS}
    base.update({
        "Voter Number": "KG1-1",
        "First Name": "Priya",
        "Surname": "Patel",
        "House Number": "45",
        "Road": "Chamberlayne Road",
        "Post Code": "NW10 3JU",
        "Status": "Active",
    })
    base.update(overrides)
    return base


def make_register_row(**overrides):
    base = {h: "" for h in REGISTER_HEADERS}
    base.update({
        "PDCode": "KG1", "RollNo": "1",
        "ElectorSurname": "Patel", "ElectorForename": "Priya",
        "RegisteredAddress1": "45 Chamberlayne Road",
        "PostCode": "NW10 3JU",
    })
    base.update(overrides)
    return base


def run_update(app_file, reg_file, output_file, extra_args=None):
    cmd = [sys.executable, str(UPDATE_TOOL),
           str(app_file), str(reg_file), str(output_file),
           "--quiet", "--date", TEST_DATE]
    if extra_args:
        cmd += extra_args
    return subprocess.run(cmd, capture_output=True, text=True)


def run_validate(orig_file, updated_file, extra_args=None):
    cmd = [sys.executable, str(VALIDATE_TOOL),
           str(orig_file), str(updated_file)]
    if extra_args:
        cmd += extra_args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def rewrite_csv(path, headers, rows, encoding="utf-8-sig"):
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)


class TestValidUpdatePasses(unittest.TestCase):
    """Run update then validate — should pass."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(),
            make_app_row(**{"Voter Number": "HP1-5", "First Name": "Kenji",
                            "Surname": "Tanaka"}),
        ]
        cls.reg_rows = [
            make_register_row(Party="G", GE24="Yes", **{"1-5": "1"}),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        cls.val_rc, cls.val_stdout, _ = run_validate(cls.app_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_passes(self):
        self.assertEqual(self.val_rc, 0, self.val_stdout)

    def test_no_fails(self):
        self.assertNotIn("[FAIL]", self.val_stdout)

    def test_protected_fields_pass(self):
        self.assertIn("[PASS] Protected fields", self.val_stdout)


class TestChangedOnlyPasses(unittest.TestCase):
    """Validate --changed-only output."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(),
            make_app_row(**{"Voter Number": "HP1-5", "First Name": "Kenji",
                            "Surname": "Tanaka"}),
        ]
        cls.reg_rows = [make_register_row(**{"1-5": "1"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path,
                   extra_args=["--changed-only"])
        cls.val_rc, cls.val_stdout, _ = run_validate(
            cls.app_path, cls.out_path, extra_args=["--changed-only"])

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_passes(self):
        self.assertEqual(self.val_rc, 0, self.val_stdout)


class TestProtectedFieldTamperFails(unittest.TestCase):
    """Modify a protected field (Surname) → FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row()]
        cls.reg_rows = [make_register_row(**{"1-5": "1"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        # Tamper: change Surname
        headers, rows = read_output_csv(cls.out_path)
        rows[0]["Surname"] = "TAMPERED"
        rewrite_csv(cls.out_path, headers, rows)
        cls.val_rc, cls.val_stdout, _ = run_validate(cls.app_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_protected_field_fail(self):
        self.assertIn("[FAIL] Protected fields", self.val_stdout)
        self.assertIn("Surname", self.val_stdout)


class TestPhantomRowFails(unittest.TestCase):
    """Add a row not in original → FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row()]
        cls.reg_rows = [make_register_row()]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        # Tamper: add phantom row
        headers, rows = read_output_csv(cls.out_path)
        phantom = make_app_row(**{"Voter Number": "PHANTOM-1"})
        rows.append(phantom)
        rewrite_csv(cls.out_path, headers, rows)
        cls.val_rc, cls.val_stdout, _ = run_validate(cls.app_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_traceability_fail(self):
        self.assertIn("PHANTOM-1", self.val_stdout)


class TestDeletedRowFails(unittest.TestCase):
    """Remove a row → FAIL on row count."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(),
            make_app_row(**{"Voter Number": "HP1-5", "First Name": "Kenji",
                            "Surname": "Tanaka"}),
        ]
        cls.reg_rows = [make_register_row()]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        # Tamper: delete a row
        headers, rows = read_output_csv(cls.out_path)
        rows.pop()
        rewrite_csv(cls.out_path, headers, rows)
        cls.val_rc, cls.val_stdout, _ = run_validate(cls.app_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_row_count_fail(self):
        self.assertIn("[FAIL] Row count", self.val_stdout)


class TestBomMissingFails(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row()]
        cls.reg_rows = [make_register_row()]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        # Tamper: rewrite without BOM
        headers, rows = read_output_csv(cls.out_path)
        rewrite_csv(cls.out_path, headers, rows, encoding="utf-8")
        cls.val_rc, cls.val_stdout, _ = run_validate(cls.app_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_fails(self):
        self.assertEqual(self.val_rc, 1)

    def test_file_format_fail(self):
        self.assertIn("[FAIL] File format", self.val_stdout)


class TestAmendableFieldChangeAllowed(unittest.TestCase):
    """Changes to amendable fields should NOT cause a FAIL."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row()]
        cls.reg_rows = [make_register_row(
            Party="G", GE24="Yes", DNK="X", Comments="A note",
            **{"1-5": "1", "PostalVoter?": "Y", "P/PB": "P/PB"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        cls.val_rc, cls.val_stdout, _ = run_validate(cls.app_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_passes(self):
        self.assertEqual(self.val_rc, 0, self.val_stdout)

    def test_statistics_show_changes(self):
        self.assertIn("Rows with changes: 1", self.val_stdout)


if __name__ == "__main__":
    unittest.main()
