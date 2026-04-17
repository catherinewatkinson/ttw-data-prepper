#!/usr/bin/env python3
"""Test suite for update_app_export.py.

Usage:
    python3 tools/test_update_app_export.py -v
    python -m pytest tools/test_update_app_export.py -v

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
TOOL = SCRIPT_DIR / "update_app_export.py"

# Fixed date for deterministic tests
TEST_DATE = "2026-Mar-31"

# ---------------------------------------------------------------------------
# App-export headers (verified from real TTW export)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_update(app_file, reg_file, output_file, extra_args=None, report_file=None):
    """Run update_app_export.py as subprocess."""
    cmd = [sys.executable, str(TOOL),
           str(app_file), str(reg_file), str(output_file),
           "--quiet", "--date", TEST_DATE]
    if report_file:
        cmd += ["--report", str(report_file)]
    if extra_args:
        cmd += extra_args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


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
        headers = list(reader.fieldnames or [])
        rows = list(reader)
    return headers, rows


def read_report(path):
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
        "PDCode": "KG1",
        "RollNo": "1",
        "ElectorSurname": "Patel",
        "ElectorForename": "Priya",
        "RegisteredAddress1": "45 Chamberlayne Road",
        "PostCode": "NW10 3JU",
    })
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestInputValidation(unittest.TestCase):

    def test_missing_app_column(self):
        bad_headers = [h for h in APP_EXPORT_HEADERS if h != "Poster ticked"]
        app = write_temp_csv([{h: "" for h in bad_headers}], bad_headers)
        reg = write_temp_csv([make_register_row()], REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            rc, _, stderr = run_update(app, reg, out)
            self.assertNotEqual(rc, 0)
            self.assertIn("Poster ticked", stderr)
        finally:
            for p in [app, reg, out]: os.path.exists(p) and os.unlink(p)

    def test_missing_register_column(self):
        app = write_temp_csv([make_app_row()], APP_EXPORT_HEADERS)
        bad_headers = [h for h in REGISTER_HEADERS if h != "ElectorSurname"]
        reg = write_temp_csv([{h: "" for h in bad_headers}], bad_headers)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            rc, _, stderr = run_update(app, reg, out)
            self.assertNotEqual(rc, 0)
            self.assertIn("surname", stderr.lower())
        finally:
            for p in [app, reg, out]: os.path.exists(p) and os.unlink(p)

    def test_empty_app_export(self):
        app = write_temp_csv([], APP_EXPORT_HEADERS)
        reg = write_temp_csv([make_register_row()], REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            rc, _, stderr = run_update(app, reg, out)
            self.assertNotEqual(rc, 0)
            self.assertIn("empty", stderr.lower())
        finally:
            for p in [app, reg, out]: os.path.exists(p) and os.unlink(p)

    def test_overwrite_protection(self):
        app = write_temp_csv([make_app_row()], APP_EXPORT_HEADERS)
        reg = write_temp_csv([make_register_row()], REGISTER_HEADERS)
        try:
            rc, _, stderr = run_update(app, reg, app)
            self.assertNotEqual(rc, 0)
            self.assertIn("overwrite", stderr.lower())
        finally:
            for p in [app, reg]: os.path.exists(p) and os.unlink(p)


class TestExactMatching(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "Priya", "Surname": "Patel", "Post Code": "NW10 3JU"}),
            make_app_row(**{"Voter Number": "HP1-5", "First Name": "Kenji", "Surname": "Tanaka", "Post Code": "NW10 3JU"}),
        ]
        cls.reg_rows = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU", Party="G", **{"1-5": "1"}),
            make_register_row(PDCode="HP1", RollNo="99",
                              ElectorSurname="Nobody", ElectorForename="John",
                              PostCode="NW10 3JU"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, _, cls.stderr = run_update(
            cls.app_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p): os.unlink(p)

    def test_exit_code(self):
        self.assertEqual(self.rc, 0, self.stderr)

    def test_matched_row_updated(self):
        patel = [r for r in self.rows if r["Surname"] == "Patel"][0]
        self.assertEqual(patel[f"{LE2026} Most Recent Data - GVI"], "1")
        self.assertEqual(patel[f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_unmatched_app_row_unchanged(self):
        tanaka = [r for r in self.rows if r["Surname"] == "Tanaka"][0]
        self.assertEqual(tanaka[f"{LE2026} Most Recent Data - GVI"], "")

    def test_output_row_count(self):
        self.assertEqual(len(self.rows), 2)


class TestFuzzyMatching(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "Priya", "Surname": "Patel", "Post Code": "NW10 3JU"}),
        ]
        cls.reg_rows = [
            make_register_row(ElectorSurname="PATEL", ElectorForename="PRIYA",
                              PostCode="NW10 3JU", **{"1-5": "2"}),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.rc, _, cls.stderr = run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_case_insensitive_match(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "2")


class TestPartyMappingAllCodes(unittest.TestCase):

    CODES = {
        "G": "Greens", "Con": "Conservatives", "Lab": "Labour", "L": "Labour",
        "LD": "Liberal Democrats", "REF": "Reform/UKIP/Brexit",
        "PC": "Plaid Cymru", "Ind": "Independent",
        "RA": "Residents Association", "Oth": "Others",
    }

    def test_each_code(self):
        for code, expected in self.CODES.items():
            with self.subTest(code=code):
                app = [make_app_row(**{"First Name": "Test", "Surname": f"Code{code}", "Post Code": "NW10 3JU"})]
                reg = [make_register_row(ElectorSurname=f"Code{code}", ElectorForename="Test",
                                         PostCode="NW10 3JU", Party=code)]
                app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
                reg_path = write_temp_csv(reg, REGISTER_HEADERS)
                fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
                try:
                    rc, _, stderr = run_update(app_path, reg_path, out)
                    self.assertEqual(rc, 0, stderr)
                    _, rows = read_output_csv(out)
                    self.assertEqual(rows[0][f"{LE2026} Most Recent Data - Usual Party"], expected)
                finally:
                    for p in [app_path, reg_path, out]:
                        if os.path.exists(p): os.unlink(p)


class TestPartyMappingInvalid(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "Invalid", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - Usual Party": "Greens",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Invalid", ElectorForename="Test",
            PostCode="NW10 3JU", Party="INVALID_CODE")]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rc, _, _ = run_update(cls.app_path, cls.reg_path, cls.out_path,
                                   report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p): os.unlink(p)

    def test_invalid_party_keeps_existing(self):
        # The existing value "Greens" should be preserved since INVALID_CODE maps via
        # map_party_name which keeps unrecognized values as-is. The reverse map then
        # won't find the code. But map_party_name returns (raw, warning) for unrecognized,
        # so the code IS the raw value "INVALID_CODE" which isn't in REVERSE_PARTY_MAP.
        # The field should remain unchanged OR get the raw value. Let's check:
        # Actually, map_party_name returns (raw, warning) for unrecognized values.
        # Then REVERSE_PARTY_MAP.get("INVALID_CODE") returns None.
        # So reverse_map_party returns ("", warning) and the field is NOT updated.
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_warning_in_report(self):
        self.assertIn("INVALID_CODE", self.report_text)


class TestGVIValidation(unittest.TestCase):

    def _run_gvi(self, gvi_value, existing=""):
        app = [make_app_row(**{
            "First Name": "Test", "Surname": "GVI", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - GVI": existing,
        })]
        reg = [make_register_row(ElectorSurname="GVI", ElectorForename="Test",
                                  PostCode="NW10 3JU", **{"1-5": gvi_value})]
        app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
        reg_path = write_temp_csv(reg, REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        report = out + ".report.txt"
        try:
            rc, _, stderr = run_update(app_path, reg_path, out, report_file=report)
            self.assertEqual(rc, 0, stderr)
            _, rows = read_output_csv(out)
            report_text = Path(report).read_text(encoding="utf-8") if os.path.exists(report) else ""
            return rows[0][f"{LE2026} Most Recent Data - GVI"], report_text
        finally:
            for p in [app_path, reg_path, out, report]:
                if os.path.exists(p): os.unlink(p)

    def test_valid_1(self):
        val, _ = self._run_gvi("1")
        self.assertEqual(val, "1")

    def test_valid_5(self):
        val, _ = self._run_gvi("5")
        self.assertEqual(val, "5")

    def test_invalid_0(self):
        val, report = self._run_gvi("0")
        self.assertEqual(val, "")
        self.assertIn("Invalid GVI", report)

    def test_invalid_6(self):
        val, report = self._run_gvi("6")
        self.assertEqual(val, "")
        self.assertIn("Invalid GVI", report)

    def test_blank_preserves_existing(self):
        val, _ = self._run_gvi("", existing="3")
        self.assertEqual(val, "3")

    def test_invalid_text(self):
        val, report = self._run_gvi("abc")
        self.assertEqual(val, "")
        self.assertIn("Invalid GVI", report)


class TestVotedFlag(unittest.TestCase):

    def _run_voted(self, ge24_value):
        app = [make_app_row(**{"First Name": "Test", "Surname": "Voted", "Post Code": "NW10 3JU"})]
        reg = [make_register_row(ElectorSurname="Voted", ElectorForename="Test",
                                  PostCode="NW10 3JU", GE24=ge24_value)]
        app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
        reg_path = write_temp_csv(reg, REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            run_update(app_path, reg_path, out)
            _, rows = read_output_csv(out)
            return rows[0][f"{GE2024} Voted"]
        finally:
            for p in [app_path, reg_path, out]:
                if os.path.exists(p): os.unlink(p)

    def test_yes(self):
        self.assertEqual(self._run_voted("Yes"), "Y")

    def test_y(self):
        self.assertEqual(self._run_voted("Y"), "Y")

    def test_empty(self):
        self.assertEqual(self._run_voted(""), "")

    def test_no(self):
        self.assertEqual(self._run_voted("No"), "")

    def test_n(self):
        self.assertEqual(self._run_voted("N"), "")


class TestPostalVoter(unittest.TestCase):

    def _run_pv(self, pv_value, col_name="PostalVoter?"):
        app = [make_app_row(**{"First Name": "Test", "Surname": "PV", "Post Code": "NW10 3JU"})]
        reg_data = make_register_row(ElectorSurname="PV", ElectorForename="Test", PostCode="NW10 3JU")
        reg_data[col_name] = pv_value
        reg = [reg_data]
        app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
        # Use custom headers if col_name differs
        headers = list(REGISTER_HEADERS)
        if col_name not in headers:
            headers.append(col_name)
        reg_path = write_temp_csv(reg, headers)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            run_update(app_path, reg_path, out)
            _, rows = read_output_csv(out)
            return rows[0][f"{LE2026} Most Recent Data - Postal Voter"]
        finally:
            for p in [app_path, reg_path, out]:
                if os.path.exists(p): os.unlink(p)

    def test_y(self):
        self.assertEqual(self._run_pv("Y"), "Y")

    def test_empty(self):
        self.assertEqual(self._run_pv(""), "")

    def test_n(self):
        self.assertEqual(self._run_pv("N"), "")

    def test_variant_column_name(self):
        self.assertEqual(self._run_pv("Y", col_name="PostalVoter"), "Y")


class TestDateConversion(unittest.TestCase):

    def _run_date(self, doa_value):
        app = [make_app_row(**{"First Name": "Test", "Surname": "Date", "Post Code": "NW10 3JU"})]
        reg = [make_register_row(ElectorSurname="Date", ElectorForename="Test",
                                  PostCode="NW10 3JU", DateOfAttainment=doa_value)]
        app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
        reg_path = write_temp_csv(reg, REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            run_update(app_path, reg_path, out)
            _, rows = read_output_csv(out)
            return rows[0]["Date of Attainment"]
        finally:
            for p in [app_path, reg_path, out]:
                if os.path.exists(p): os.unlink(p)

    def test_dd_mm_yyyy(self):
        self.assertEqual(self._run_date("31/03/2026"), "2026-Mar-31")

    def test_iso_format(self):
        self.assertEqual(self._run_date("2026-03-31"), "2026-Mar-31")

    def test_empty(self):
        self.assertEqual(self._run_date(""), "")

    def test_january(self):
        self.assertEqual(self._run_date("15/01/2008"), "2008-Jan-15")

    def test_december(self):
        self.assertEqual(self._run_date("25/12/2000"), "2000-Dec-25")


class TestPPBTags(unittest.TestCase):

    def _run_ppb(self, ppb_value, existing_poster="", existing_board=""):
        app = [make_app_row(**{
            "First Name": "Test", "Surname": "PPB", "Post Code": "NW10 3JU",
            "Poster ticked": existing_poster, "Board ticked": existing_board,
        })]
        reg = [make_register_row(ElectorSurname="PPB", ElectorForename="Test",
                                  PostCode="NW10 3JU", **{"P/PB": ppb_value})]
        app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
        reg_path = write_temp_csv(reg, REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            run_update(app_path, reg_path, out)
            _, rows = read_output_csv(out)
            return rows[0]["Poster ticked"], rows[0]["Board ticked"]
        finally:
            for p in [app_path, reg_path, out]:
                if os.path.exists(p): os.unlink(p)

    def test_p_only(self):
        poster, board = self._run_ppb("P")
        self.assertEqual(poster, "TRUE")
        self.assertEqual(board, "")

    def test_pb_only(self):
        poster, board = self._run_ppb("PB")
        self.assertEqual(poster, "")
        self.assertEqual(board, "TRUE")

    def test_both(self):
        poster, board = self._run_ppb("P/PB")
        self.assertEqual(poster, "TRUE")
        self.assertEqual(board, "TRUE")

    def test_empty(self):
        poster, board = self._run_ppb("")
        self.assertEqual(poster, "")
        self.assertEqual(board, "")

    def test_already_true(self):
        poster, board = self._run_ppb("", existing_poster="TRUE")
        self.assertEqual(poster, "TRUE")


class TestDNKTag(unittest.TestCase):

    def _run_dnk(self, dnk_value, existing=""):
        app = [make_app_row(**{
            "First Name": "Test", "Surname": "DNK", "Post Code": "NW10 3JU",
            "Do Not Knock ticked": existing,
        })]
        reg = [make_register_row(ElectorSurname="DNK", ElectorForename="Test",
                                  PostCode="NW10 3JU", DNK=dnk_value)]
        app_path = write_temp_csv(app, APP_EXPORT_HEADERS)
        reg_path = write_temp_csv(reg, REGISTER_HEADERS)
        fd, out = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        try:
            run_update(app_path, reg_path, out)
            _, rows = read_output_csv(out)
            return rows[0]["Do Not Knock ticked"]
        finally:
            for p in [app_path, reg_path, out]:
                if os.path.exists(p): os.unlink(p)

    def test_non_empty(self):
        self.assertEqual(self._run_dnk("X"), "TRUE")

    def test_empty(self):
        self.assertEqual(self._run_dnk(""), "")

    def test_already_true(self):
        self.assertEqual(self._run_dnk("", existing="TRUE"), "TRUE")

    def test_n_does_not_set_true(self):
        self.assertEqual(self._run_dnk("N"), "")

    def test_no_does_not_set_true(self):
        self.assertEqual(self._run_dnk("No"), "")


class TestNotesInsertion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "Notes", "Post Code": "NW10 3JU",
            "Text of Note 1": "Old note 1",
            "Date of Note 1 (most recent)": "2026-Jan-01",
            "Text of Note 2": "Old note 2",
            "Date of Note 2": "2025-Dec-15",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Notes", ElectorForename="Test",
            PostCode="NW10 3JU", Comments="New comment")]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_note1_is_new_comment(self):
        self.assertEqual(self.rows[0]["Text of Note 1"], "New comment")

    def test_note1_date(self):
        self.assertEqual(self.rows[0]["Date of Note 1 (most recent)"], TEST_DATE)

    def test_old_note1_shifted_to_note2(self):
        self.assertEqual(self.rows[0]["Text of Note 2"], "Old note 1")
        self.assertEqual(self.rows[0]["Date of Note 2"], "2026-Jan-01")

    def test_old_note2_shifted_to_note3(self):
        self.assertEqual(self.rows[0]["Text of Note 3"], "Old note 2")
        self.assertEqual(self.rows[0]["Date of Note 3"], "2025-Dec-15")


class TestNotesEmpty(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "NoComment", "Post Code": "NW10 3JU",
            "Text of Note 1": "Existing note",
            "Date of Note 1 (most recent)": "2026-Jan-01",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="NoComment", ElectorForename="Test",
            PostCode="NW10 3JU", Comments="")]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_notes_untouched(self):
        self.assertEqual(self.rows[0]["Text of Note 1"], "Existing note")
        self.assertEqual(self.rows[0]["Date of Note 1 (most recent)"], "2026-Jan-01")


class TestNotesOverflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        overrides = {"First Name": "Test", "Surname": "Full", "Post Code": "NW10 3JU"}
        overrides["Text of Note 1"] = "Note 1"
        overrides["Date of Note 1 (most recent)"] = "2026-Jan-01"
        for i in range(2, 11):
            overrides[f"Text of Note {i}"] = f"Note {i}"
            overrides[f"Date of Note {i}"] = f"2025-Dec-{i:02d}"
        cls.app_rows = [make_app_row(**overrides)]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Full", ElectorForename="Test",
            PostCode="NW10 3JU", Comments="Overflow comment")]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        run_update(cls.app_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p): os.unlink(p)

    def test_note1_is_new(self):
        self.assertEqual(self.rows[0]["Text of Note 1"], "Overflow comment")

    def test_note10_is_old_note9(self):
        self.assertEqual(self.rows[0]["Text of Note 10"], "Note 9")

    def test_overflow_warning(self):
        self.assertIn("Note 10", self.report_text)


class TestLE2026VisitShift(unittest.TestCase):
    """New GVI/Party shifts existing visit data down, preserves history."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "Shift", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - Date": "2026-Feb-15",
            f"{LE2026} Most Recent Data - GVI": "3",
            f"{LE2026} Most Recent Data - Usual Party": "Labour",
            f"{LE2026} Most Recent Data - Postal Voter": "Y",
            f"{LE2026} Previous Data 1 - Date": "2026-Jan-10",
            f"{LE2026} Previous Data 1 - GVI": "2",
            f"{LE2026} Previous Data 1 - Usual Party": "Conservatives",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Shift", ElectorForename="Test",
            PostCode="NW10 3JU", Party="G", **{"1-5": "1"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_most_recent_has_new_data(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "1")
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Greens")
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Date"], TEST_DATE)

    def test_old_most_recent_shifted_to_previous1(self):
        """Previous visit record preserved in Previous 1."""
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - Date"], "2026-Feb-15")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - GVI"], "3")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - Usual Party"], "Labour")

    def test_old_previous1_shifted_to_previous2(self):
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 2 - Date"], "2026-Jan-10")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 2 - GVI"], "2")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 2 - Usual Party"], "Conservatives")

    def test_postal_voter_untouched(self):
        """Postal Voter has no Previous slots — stays on Most Recent."""
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Postal Voter"], "Y")


class TestLE2026PartialNewData(unittest.TestCase):
    """Register has GVI but no Party — shift still happens, Party blank in new Most Recent."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "Partial", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - Date": "2026-Feb-15",
            f"{LE2026} Most Recent Data - GVI": "3",
            f"{LE2026} Most Recent Data - Usual Party": "Labour",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Partial", ElectorForename="Test",
            PostCode="NW10 3JU", **{"1-5": "1"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_new_gvi(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "1")

    def test_party_blank_in_most_recent(self):
        """No party this visit — blank, not stale from previous visit."""
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "")

    def test_date_set_to_today(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Date"], TEST_DATE)

    def test_old_data_preserved_in_previous1(self):
        """Previous visit's full record preserved."""
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - GVI"], "3")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - Usual Party"], "Labour")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - Date"], "2026-Feb-15")


class TestLE2026NoVisitData(unittest.TestCase):
    """No GVI/Party in register → no shift, existing data preserved."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "NoVisit", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - GVI": "3",
            f"{LE2026} Most Recent Data - Usual Party": "Labour",
            f"{LE2026} Most Recent Data - Date": "2026-Feb-15",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="NoVisit", ElectorForename="Test",
            PostCode="NW10 3JU", GE24="Yes")]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_all_preserved(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "3")
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Labour")
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Date"], "2026-Feb-15")

    def test_previous1_still_empty(self):
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - GVI"], "")


class TestLE2026Overflow(unittest.TestCase):
    """All 5 slots full → Previous 4 lost with warning."""

    @classmethod
    def setUpClass(cls):
        overrides = {"First Name": "Test", "Surname": "Full", "Post Code": "NW10 3JU"}
        overrides[f"{LE2026} Most Recent Data - Date"] = "2026-Mar-01"
        overrides[f"{LE2026} Most Recent Data - GVI"] = "1"
        overrides[f"{LE2026} Most Recent Data - Usual Party"] = "Greens"
        for i in range(1, 5):
            overrides[f"{LE2026} Previous Data {i} - Date"] = f"2026-Jan-{i:02d}"
            overrides[f"{LE2026} Previous Data {i} - GVI"] = str(i + 1)
            overrides[f"{LE2026} Previous Data {i} - Usual Party"] = "Labour"
        cls.app_rows = [make_app_row(**overrides)]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Full", ElectorForename="Test",
            PostCode="NW10 3JU", Party="LD", **{"1-5": "4"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        run_update(cls.app_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p): os.unlink(p)

    def test_most_recent_has_new_data(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "4")
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Liberal Democrats")

    def test_previous1_has_old_most_recent(self):
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - GVI"], "1")
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - Usual Party"], "Greens")

    def test_overflow_warning(self):
        self.assertIn("Previous Data 4", self.report_text)


class TestTTWSentinelValues(unittest.TestCase):
    """TTW uses <NO RECORD> and <NO DATA RECORDED> as empty — should be treated as no data."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "Sentinel", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - Date": "<NO RECORD>",
            f"{LE2026} Most Recent Data - GVI": "<NO RECORD>",
            f"{LE2026} Most Recent Data - Usual Party": "<NO RECORD>",
            f"{LE2026} Most Recent Data - Postal Voter": "<NO DATA RECORDED>",
            f"{LE2026} Previous Data 4 - Date": "<NO RECORD>",
            f"{LE2026} Previous Data 4 - GVI": "<NO RECORD>",
            f"{LE2026} Previous Data 4 - Usual Party": "<NO RECORD>",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Sentinel", ElectorForename="Test",
            PostCode="NW10 3JU", Party="G", **{"1-5": "1"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        run_update(cls.app_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p): os.unlink(p)

    def test_new_values_written(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "1")
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_sentinels_shifted_to_previous1(self):
        """<NO RECORD> values shift verbatim — that's fine, they represent no prior data."""
        self.assertEqual(self.rows[0][f"{LE2026} Previous Data 1 - GVI"], "<NO RECORD>")

    def test_no_false_overflow_warning(self):
        """Previous 4 had <NO RECORD> — should NOT warn about data loss."""
        self.assertNotIn("content lost", self.report_text)


class TestChangedOnlyFlag(unittest.TestCase):
    """--changed-only outputs only rows that were actually modified."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "Priya", "Surname": "Patel", "Post Code": "NW10 3JU"}),
            make_app_row(**{"Voter Number": "HP1-5", "First Name": "Kenji", "Surname": "Tanaka", "Post Code": "NW10 3JU"}),
            make_app_row(**{"Voter Number": "HP1-6", "First Name": "Sean", "Surname": "Murphy", "Post Code": "NW2 4PJ"}),
        ]
        # Register only has data for Patel
        cls.reg_rows = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU", **{"1-5": "1"}),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)

        # Run with --changed-only
        fd, cls.out_changed = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_changed,
                   extra_args=["--changed-only"])
        _, cls.rows_changed = read_output_csv(cls.out_changed)

        # Run without --changed-only
        fd, cls.out_all = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_all)
        _, cls.rows_all = read_output_csv(cls.out_all)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_changed, cls.out_all]:
            if os.path.exists(p): os.unlink(p)

    def test_changed_only_has_one_row(self):
        self.assertEqual(len(self.rows_changed), 1)

    def test_changed_row_is_patel(self):
        self.assertEqual(self.rows_changed[0]["Surname"], "Patel")

    def test_all_rows_has_three(self):
        self.assertEqual(len(self.rows_all), 3)


class TestMatchedButNoNewData(unittest.TestCase):
    """Matched row with no updatable data should NOT count as changed."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "Priya", "Surname": "Patel", "Post Code": "NW10 3JU"}),
        ]
        # Register matches Patel but has no GVI, Party, Voted, DNK, Comments, etc.
        cls.reg_rows = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path, extra_args=["--changed-only"])
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_no_rows_in_output(self):
        """Matched but nothing to update — should not appear in --changed-only output."""
        self.assertEqual(len(self.rows), 0)


class TestChangedOnlyNoMatches(unittest.TestCase):
    """--changed-only with no matches produces header-only CSV."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "Priya", "Surname": "Patel", "Post Code": "NW10 3JU"}),
        ]
        cls.reg_rows = [
            make_register_row(ElectorSurname="Nobody", ElectorForename="John",
                              PostCode="SW1A 1AA"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path, extra_args=["--changed-only"])
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_zero_rows(self):
        self.assertEqual(len(self.rows), 0)

    def test_headers_present(self):
        self.assertEqual(self.headers, APP_EXPORT_HEADERS)


class TestGapFillSemantics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "GapFill", "Post Code": "NW10 3JU",
            f"{LE2026} Most Recent Data - GVI": "3",
            f"{LE2026} Most Recent Data - Usual Party": "Labour",
        })]
        cls.reg_rows = [make_register_row(
            ElectorSurname="GapFill", ElectorForename="Test",
            PostCode="NW10 3JU", Party="", **{"1-5": ""})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_gvi_preserved(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "3")

    def test_party_preserved(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Labour")


class TestQAReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "Priya", "Surname": "Patel", "Post Code": "NW10 3JU"}),
            make_app_row(**{"Voter Number": "HP1-5", "First Name": "Kenji", "Surname": "Tanaka", "Post Code": "NW10 3JU"}),
        ]
        cls.reg_rows = [
            make_register_row(ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU", Party="G", **{"1-5": "1"}),
            make_register_row(PDCode="XX", RollNo="99",
                              ElectorSurname="Nobody", ElectorForename="John",
                              PostCode="SW1A 1AA"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        run_update(cls.app_path, cls.reg_path, cls.out_path, report_file=cls.report_path)
        cls.report_text, cls.machine = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.report_path]:
            if os.path.exists(p): os.unlink(p)

    def test_report_created(self):
        self.assertTrue(os.path.exists(self.report_path))

    def test_report_has_summary(self):
        self.assertIn("Matched: 1", self.report_text)

    def test_report_has_field_updates(self):
        self.assertIn("LE2026 GVI", self.report_text)

    def test_machine_readable(self):
        matched = [l for l in self.machine if l.startswith("MATCHED")]
        self.assertEqual(len(matched), 1)
        self.assertIn("Patel", matched[0])

    def test_unmatched_in_report(self):
        unmatched = [l for l in self.machine if l.startswith("UNMATCHED")]
        self.assertTrue(len(unmatched) >= 1)


class TestOutputFormat(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row()]
        cls.reg_rows = [make_register_row()]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        cls.headers, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_all_columns_preserved(self):
        for h in APP_EXPORT_HEADERS:
            self.assertIn(h, self.headers, f"Missing column: {h}")

    def test_column_order_preserved(self):
        self.assertEqual(self.headers, APP_EXPORT_HEADERS)

    def test_utf8_bom(self):
        with open(self.out_path, "rb") as f:
            self.assertEqual(f.read(3), b"\xef\xbb\xbf")

    def test_row_count(self):
        self.assertEqual(len(self.rows), 1)


class TestOutputRowCount(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{"First Name": "A", "Surname": "One", "Post Code": "NW10 3JU"}),
            make_app_row(**{"First Name": "B", "Surname": "Two", "Post Code": "NW10 3JU"}),
            make_app_row(**{"First Name": "C", "Surname": "Three", "Post Code": "NW10 3JU"}),
        ]
        cls.reg_rows = [
            make_register_row(ElectorSurname="One", ElectorForename="A", PostCode="NW10 3JU"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_same_count_as_input(self):
        self.assertEqual(len(self.rows), 3)


class TestMultipleFieldsUpdated(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [make_app_row(**{
            "First Name": "Test", "Surname": "Multi", "Post Code": "NW10 3JU"})]
        cls.reg_rows = [make_register_row(
            ElectorSurname="Multi", ElectorForename="Test", PostCode="NW10 3JU",
            Party="LD", GE24="Yes", DNK="X", Comments="A note",
            **{"PostalVoter?": "Y", "1-5": "4", "P/PB": "P/PB"})]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        run_update(cls.app_path, cls.reg_path, cls.out_path)
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path]:
            if os.path.exists(p): os.unlink(p)

    def test_gvi(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - GVI"], "4")

    def test_party(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Usual Party"], "Liberal Democrats")

    def test_postal(self):
        self.assertEqual(self.rows[0][f"{LE2026} Most Recent Data - Postal Voter"], "Y")

    def test_voted(self):
        self.assertEqual(self.rows[0][f"{GE2024} Voted"], "Y")

    def test_dnk(self):
        self.assertEqual(self.rows[0]["Do Not Knock ticked"], "TRUE")

    def test_poster(self):
        self.assertEqual(self.rows[0]["Poster ticked"], "TRUE")

    def test_board(self):
        self.assertEqual(self.rows[0]["Board ticked"], "TRUE")

    def test_note(self):
        self.assertEqual(self.rows[0]["Text of Note 1"], "A note")


class TestPerfectScoreAutoResolve(unittest.TestCase):
    """A score=1.0 match against an imperfect runner-up is auto-resolved:
    data is applied to the perfect match, the runner-up appears in --changed-only
    output via force_include_indices, and the register row is logged to
    rejects2check with the 'Auto-resolved to perfect match' prefix so the user
    can spot-check."""

    @classmethod
    def setUpClass(cls):
        # Priya Patel (perfect match, score 1.0) + Priyanka Patel (spouse, ~0.89)
        # at the same postcode. Register row has Party+GVI to update.
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-1", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU", "Voter UUID": "uuid-priya",
            }),
            make_app_row(**{
                "Voter Number": "KG1-2", "First Name": "Priyanka", "Surname": "Patel",
                "Post Code": "NW10 3JU", "Voter UUID": "uuid-priyanka",
            }),
        ]
        # Use a PDCode+RollNo that matches NEITHER candidate's Voter Number,
        # so the voter-number tiebreaker falls through and the perfect-score
        # branch is exercised directly.
        cls.reg_rows = [
            make_register_row(
                PDCode="ZZ9", RollNo="999",
                ElectorSurname="Patel", ElectorForename="Priya",
                PostCode="NW10 3JU", Party="G", **{"1-5": "1"}),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        cls.rc, _, cls.stderr = run_update(
            cls.app_path, cls.reg_path, cls.out_path,
            extra_args=["--changed-only"], report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, cls.machine_lines = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path,
                  cls.report_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_exit_code(self):
        self.assertEqual(self.rc, 0, self.stderr)

    def test_perfect_match_row_updated(self):
        priya = [r for r in self.rows if r["First Name"] == "Priya"][0]
        self.assertEqual(priya[f"{LE2026} Most Recent Data - GVI"], "1")
        self.assertEqual(priya[f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_runner_up_in_changed_only_output(self):
        priyanka = [r for r in self.rows if r["First Name"] == "Priyanka"]
        self.assertEqual(len(priyanka), 1,
            "Runner-up should appear in --changed-only output via force_include_indices")

    def test_runner_up_unchanged(self):
        priyanka = [r for r in self.rows if r["First Name"] == "Priyanka"][0]
        self.assertEqual(priyanka[f"{LE2026} Most Recent Data - GVI"], "")
        self.assertEqual(priyanka[f"{LE2026} Most Recent Data - Usual Party"], "")

    def test_ambiguous_counter_not_incremented(self):
        """Auto-resolved rows are treated as matched, not ambiguous."""
        self.assertIn("Ambiguous: 0", self.report_text)
        self.assertIn("Matched: 1", self.report_text)
        # No AMBIGUOUS| entry in machine-readable section either
        ambiguous_lines = [l for l in self.machine_lines if l.startswith("AMBIGUOUS|")]
        self.assertEqual(ambiguous_lines, [])

    def test_rejects2check_contains_auto_resolve_entry(self):
        self.assertTrue(os.path.exists(self.rejects_path),
            "rejects2check.csv should be written")
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            rejects = list(csv.DictReader(f))
        self.assertEqual(len(rejects), 1)
        reason = rejects[0]["Reject_Reason"]
        self.assertTrue(reason.startswith("Auto-resolved to perfect match"),
            f"Unexpected reason prefix: {reason!r}")

    def test_reject_reason_includes_winner_and_runnerup_ids(self):
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reason = list(csv.DictReader(f))[0]["Reject_Reason"]
        self.assertIn("KG1-1", reason)      # winner Voter Number
        self.assertIn("uuid-priya", reason)  # winner UUID
        self.assertIn("KG1-2", reason)      # runner-up Voter Number
        self.assertIn("uuid-priyanka", reason)  # runner-up UUID


class TestVoterNumberAutoResolve(unittest.TestCase):
    """When an ambiguous match is resolved by the elector number (PDCode+RollNo)
    tiebreaker, the winner gets the data applied, runner-ups appear in
    --changed-only output, and the register row is logged to rejects2check with
    the 'Auto-resolved via elector number match' prefix for spot-checking."""

    @classmethod
    def setUpClass(cls):
        # Two similar-named candidates at same postcode with ambiguous name
        # scores (both forenames start with "Priya"). The register row has
        # PDCode+RollNo matching ONLY the second candidate's Voter Number,
        # so voter-number tiebreaker must pick the second candidate.
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-1", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU", "Voter UUID": "uuid-priya",
            }),
            make_app_row(**{
                "Voter Number": "KG1-2", "First Name": "Priyanka", "Surname": "Patel",
                "Post Code": "NW10 3JU", "Voter UUID": "uuid-priyanka",
            }),
        ]
        # Register row is for Priyanka (PDCode KG1, RollNo 2 → "KG1-2").
        # But name score best-matches "Priya" (1.0) with Priyanka slightly lower,
        # making the situation ambiguous — voter-number tiebreaker picks Priyanka.
        cls.reg_rows = [
            make_register_row(
                PDCode="KG1", RollNo="2",
                ElectorSurname="Patel", ElectorForename="Priyanka",
                PostCode="NW10 3JU", Party="G", **{"1-5": "1"}),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        cls.rc, _, cls.stderr = run_update(
            cls.app_path, cls.reg_path, cls.out_path,
            extra_args=["--changed-only"], report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path,
                  cls.report_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_exit_code(self):
        self.assertEqual(self.rc, 0, self.stderr)

    def test_voter_number_winner_gets_update(self):
        """Priyanka (KG1-2) must receive the GVI/Party update — not Priya."""
        priyanka = [r for r in self.rows if r["Voter Number"] == "KG1-2"][0]
        self.assertEqual(priyanka[f"{LE2026} Most Recent Data - GVI"], "1")
        self.assertEqual(priyanka[f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_runner_up_included_unchanged(self):
        priya = [r for r in self.rows if r["Voter Number"] == "KG1-1"]
        self.assertEqual(len(priya), 1,
            "Runner-up must appear in --changed-only output via force_include_indices")
        self.assertEqual(priya[0][f"{LE2026} Most Recent Data - GVI"], "")

    def test_not_counted_as_ambiguous(self):
        self.assertIn("Ambiguous: 0", self.report_text)
        self.assertIn("Matched: 1", self.report_text)

    def test_rejects2check_has_voter_number_entry(self):
        self.assertTrue(os.path.exists(self.rejects_path),
            "rejects2check.csv should be written")
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            rejects = list(csv.DictReader(f))
        self.assertEqual(len(rejects), 1)
        reason = rejects[0]["Reject_Reason"]
        self.assertTrue(reason.startswith("Auto-resolved via elector number match"),
            f"Unexpected reason prefix: {reason!r}")
        self.assertIn("KG1-2", reason)        # winner
        self.assertIn("uuid-priyanka", reason) # winner UUID
        self.assertIn("KG1-1", reason)        # runner-up
        self.assertIn("uuid-priya", reason)    # runner-up UUID


class TestVoterNumberSuffixMatch(unittest.TestCase):
    """App Voter Number is natively 'PDCode-RollNo-Suffix'; register only has
    PDCode+RollNo. The voter-number tiebreaker must match by prefix so a reg
    voter number 'KG1-2' matches an app voter number 'KG1-2-A'."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-1-A", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
            make_app_row(**{
                "Voter Number": "KG1-2-B", "First Name": "Priyanka", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
        ]
        cls.reg_rows = [
            make_register_row(
                PDCode="KG1", RollNo="2",
                ElectorSurname="Patel", ElectorForename="Priyanka",
                PostCode="NW10 3JU", Party="G"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        run_update(cls.app_path, cls.reg_path, cls.out_path,
                   extra_args=["--changed-only"])
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_suffixed_voter_number_is_matched(self):
        """Reg KG1-2 must resolve to app KG1-2-B despite the suffix."""
        winner = [r for r in self.rows if r["Voter Number"] == "KG1-2-B"][0]
        self.assertEqual(winner[f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_loser_unchanged(self):
        """The non-winning candidate must not receive any field update."""
        loser = [r for r in self.rows if r["Voter Number"] == "KG1-1-A"][0]
        self.assertEqual(loser[f"{LE2026} Most Recent Data - Usual Party"], "")

    def test_rejects2check_marks_elector_number_resolution(self):
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reason = list(csv.DictReader(f))[0]["Reject_Reason"]
        self.assertTrue(reason.startswith("Auto-resolved via elector number match"),
            f"Unexpected reason: {reason!r}")


class TestVoterNumberOverridesPerfectNameScore(unittest.TestCase):
    """When the register's PDCode+RollNo points to a candidate whose name
    scores <1.0, the voter-number match MUST win even if another candidate
    scored exactly 1.0 on name similarity. This guards against a data-entry
    mistake on the app where a neighbour has the same name as the register
    elector."""

    @classmethod
    def setUpClass(cls):
        # Register: Priya Patel, KG1-2
        # App row KG1-1: Priya Patel (name score 1.0 — but WRONG person)
        # App row KG1-2: Priyanka Patel (name score ~0.89 — correct person per VN)
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-1", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
            make_app_row(**{
                "Voter Number": "KG1-2", "First Name": "Priyanka", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
        ]
        cls.reg_rows = [
            make_register_row(
                PDCode="KG1", RollNo="2",
                ElectorSurname="Patel", ElectorForename="Priya",
                PostCode="NW10 3JU", Party="G"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        run_update(cls.app_path, cls.reg_path, cls.out_path,
                   extra_args=["--changed-only"], report_file=cls.report_path)
        _, cls.rows = read_output_csv(cls.out_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path,
                  cls.report_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_voter_number_winner_gets_update(self):
        priyanka = [r for r in self.rows if r["Voter Number"] == "KG1-2"][0]
        self.assertEqual(priyanka[f"{LE2026} Most Recent Data - Usual Party"], "Greens")

    def test_perfect_score_name_match_does_not_get_update(self):
        priya = [r for r in self.rows if r["Voter Number"] == "KG1-1"][0]
        self.assertEqual(priya[f"{LE2026} Most Recent Data - Usual Party"], "")

    def test_reason_is_elector_number_not_perfect_match(self):
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reason = list(csv.DictReader(f))[0]["Reject_Reason"]
        self.assertTrue(reason.startswith("Auto-resolved via elector number match"),
            f"VN should win over perfect name score, got: {reason!r}")

    def test_counted_as_matched_not_ambiguous(self):
        self.assertIn("Matched: 1", self.report_text)
        self.assertIn("Ambiguous: 0", self.report_text)


class TestTruePerfectTieFallsThroughToTiebreakers(unittest.TestCase):
    """Two candidates both at score=1.0 (e.g. duplicate app rows) must NOT
    trigger the perfect-match auto-resolve: the second_score < 1.0 guard means
    normal tiebreaker/ambiguous handling takes over."""

    @classmethod
    def setUpClass(cls):
        # Two app rows with identical name+postcode (data-entry duplicate)
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-1", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
            make_app_row(**{
                "Voter Number": "KG1-2", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
        ]
        # Override PDCode+RollNo so the voter-number tiebreaker matches NEITHER
        # app row; otherwise the default "KG1-1" would collide with app row 1
        # and mask the branch under test.
        cls.reg_rows = [
            make_register_row(PDCode="ZZ9", RollNo="999",
                              ElectorSurname="Patel", ElectorForename="Priya",
                              PostCode="NW10 3JU", Party="G"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        run_update(cls.app_path, cls.reg_path, cls.out_path,
                   report_file=cls.report_path)
        cls.report_text, _ = read_report(cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path,
                  cls.report_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_counted_as_ambiguous_not_matched(self):
        self.assertIn("Matched: 0", self.report_text)
        self.assertIn("Ambiguous: 1", self.report_text)

    def test_rejects_file_exists_with_plain_ambiguous_entry(self):
        """A genuine two-1.0 tie produces a plain 'Ambiguous:' reject — not
        an auto-resolved one — proving the second_score < 1.0 guard held."""
        self.assertTrue(os.path.exists(self.rejects_path),
            "Two-1.0-tie should produce an Ambiguous entry in rejects2check")
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reasons = [r["Reject_Reason"] for r in csv.DictReader(f)]
        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith("Ambiguous:"),
            f"Expected plain 'Ambiguous:' prefix, got: {reasons[0]!r}")
        self.assertFalse(reasons[0].startswith("Auto-resolved"),
            f"Auto-resolve should not fire on two-1.0 tie, got: {reasons[0]!r}")


class TestVoterNumberNoFalseMatchOnPrefix(unittest.TestCase):
    """Reg PDCode-RollNo 'KG1-2' must NOT match app Voter Number 'KG1-20-X'.
    Guards the trailing-dash in `vn_prefix` against future regression."""

    @classmethod
    def setUpClass(cls):
        # Only candidate is KG1-20 (extended roll number, different person).
        # Names are close-but-ambiguous to ensure we enter the ambiguity block.
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-20-X", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
            make_app_row(**{
                "Voter Number": "KG1-21-Y", "First Name": "Priyanka", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
        ]
        # Reg voter number "KG1-2" — must NOT prefix-match "KG1-20-X" or "KG1-21-Y"
        cls.reg_rows = [
            make_register_row(
                PDCode="KG1", RollNo="2",
                ElectorSurname="Patel", ElectorForename="Priya",
                PostCode="NW10 3JU", Party="G"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        run_update(cls.app_path, cls.reg_path, cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_not_resolved_via_voter_number(self):
        """No reject entry should claim 'Auto-resolved via elector number
        match' — KG1-2 is not a valid prefix of KG1-20 or KG1-21."""
        self.assertTrue(os.path.exists(self.rejects_path))
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reasons = [r["Reject_Reason"] for r in csv.DictReader(f)]
        for reason in reasons:
            self.assertFalse(
                reason.startswith("Auto-resolved via elector number match"),
                f"KG1-2 must not prefix-match KG1-20 / KG1-21: {reason!r}")

    def test_falls_through_to_perfect_score(self):
        """Positive confirmation the VN tiebreaker fell through: Priya scores
        1.0 against reg Priya so the perfect-score branch should resolve."""
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reasons = [r["Reject_Reason"] for r in csv.DictReader(f)]
        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith("Auto-resolved to perfect match"),
            f"Expected perfect-score to resolve after VN fell through, got: {reasons[0]!r}")


class TestEmptyVoterNumberFallsThroughToPerfectScore(unittest.TestCase):
    """When the register row has no PDCode/RollNo, the voter-number tiebreaker
    must be skipped and perfect-score auto-resolve should take over."""

    @classmethod
    def setUpClass(cls):
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-1", "First Name": "Priya", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
            make_app_row(**{
                "Voter Number": "KG1-2", "First Name": "Priyanka", "Surname": "Patel",
                "Post Code": "NW10 3JU",
            }),
        ]
        # Blank PDCode → reg_voter_num = "" → VN tiebreaker skipped.
        cls.reg_rows = [
            make_register_row(
                PDCode="", RollNo="",
                ElectorSurname="Patel", ElectorForename="Priya",
                PostCode="NW10 3JU", Party="G"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        run_update(cls.app_path, cls.reg_path, cls.out_path,
                   extra_args=["--changed-only"])
        _, cls.rows = read_output_csv(cls.out_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_resolved_via_perfect_score(self):
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reasons = [r["Reject_Reason"] for r in csv.DictReader(f)]
        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith("Auto-resolved to perfect match"),
            f"Expected perfect-match resolution, got: {reasons[0]!r}")

    def test_correct_winner_gets_update(self):
        """Priya (perfect name match) must receive the update — not Priyanka."""
        priya = [r for r in self.rows if r["Voter Number"] == "KG1-1"][0]
        self.assertEqual(priya[f"{LE2026} Most Recent Data - Usual Party"], "Greens")
        priyanka = [r for r in self.rows if r["Voter Number"] == "KG1-2"][0]
        self.assertEqual(priyanka[f"{LE2026} Most Recent Data - Usual Party"], "")


class TestTwoRegRowsClaimSameAppRowViaVN(unittest.TestCase):
    """If two register rows both resolve via VN to the same app row, only the
    first should get the match (and its 'Auto-resolved…' reject entry). The
    second should get a single 'Ambiguous (tiebreaker target claimed)' entry —
    NOT a contradictory pair of 'Auto-resolved' + 'Ambiguous'."""

    @classmethod
    def setUpClass(cls):
        # Single app row that both reg rows will target via VN KG1-5.
        cls.app_rows = [
            make_app_row(**{
                "Voter Number": "KG1-5", "First Name": "Chris", "Surname": "Jones",
                "Post Code": "NW10 3JU",
            }),
            # A decoy with a close name score so matching enters the ambiguity
            # block for both reg rows.
            make_app_row(**{
                "Voter Number": "KG1-6", "First Name": "Christine", "Surname": "Jones",
                "Post Code": "NW10 3JU",
            }),
        ]
        cls.reg_rows = [
            make_register_row(PDCode="KG1", RollNo="5",
                              ElectorSurname="Jones", ElectorForename="Chris",
                              PostCode="NW10 3JU", Party="G"),
            make_register_row(PDCode="KG1", RollNo="5",
                              ElectorSurname="Jones", ElectorForename="Chris",
                              PostCode="NW10 3JU", Party="Lab"),
        ]
        cls.app_path = write_temp_csv(cls.app_rows, APP_EXPORT_HEADERS)
        cls.reg_path = write_temp_csv(cls.reg_rows, REGISTER_HEADERS)
        fd, cls.out_path = tempfile.mkstemp(suffix=".csv"); os.close(fd)
        cls.report_path = cls.out_path + ".report.txt"
        cls.rejects_path = cls.out_path[:-4] + ".rejects2check.csv"
        run_update(cls.app_path, cls.reg_path, cls.out_path,
                   report_file=cls.report_path)

    @classmethod
    def tearDownClass(cls):
        for p in [cls.app_path, cls.reg_path, cls.out_path,
                  cls.report_path, cls.rejects_path]:
            if os.path.exists(p): os.unlink(p)

    def test_exactly_two_reject_entries(self):
        """One 'Auto-resolved' entry for the first reg row (successful match),
        one 'Ambiguous (tiebreaker target claimed)' for the second, in that
        order (input order of the register rows). A prior bug produced THREE
        entries — two for the second reg row."""
        with open(self.rejects_path, "r", encoding="utf-8-sig", newline="") as f:
            reasons = [r["Reject_Reason"] for r in csv.DictReader(f)]
        self.assertEqual(len(reasons), 2, f"Expected 2 entries, got {len(reasons)}: {reasons}")
        self.assertTrue(reasons[0].startswith("Auto-resolved"),
            f"First entry should be the successful auto-resolve: {reasons[0]!r}")
        self.assertTrue(reasons[1].startswith("Ambiguous (tiebreaker target claimed)"),
            f"Second entry should be the claim-collision: {reasons[1]!r}")


if __name__ == "__main__":
    unittest.main()
