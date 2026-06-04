"""
MichaeliBilling — Dr. J. Michaeli, Mount Sinai Fertility
Billing dashboard: import eIVF XLS → review/edit → export report → save to DB
"""

import eel
import sqlite3
import pandas as pd
import json
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DB_PATH    = BASE_DIR / "db"    / "billing.db"
EXPORT_DIR = BASE_DIR / "exports"
LOG_DIR    = BASE_DIR / "logs"
WEB_DIR    = BASE_DIR / "web"
ICON_PATH  = BASE_DIR / "Billing.ico"

for d in (DB_PATH.parent, EXPORT_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=LOG_DIR / f"billing_{datetime.now():%Y%m%d}.log",
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger("MichaeliBilling")

# ── Reference data ─────────────────────────────────────────────────────────────
BILLING_CODES = {
    "A205": {"desc": "Consultation*",                     "fee": 134.25},
    "A935": {"desc": "Special surgical consultation",     "fee": 194.65},
    "A206": {"desc": "Repeat consultation",               "fee":  71.45},
    "A203": {"desc": "Specific assessment",               "fee":  62.65},
    "A204": {"desc": "Partial assessment",                "fee":  40.50},
    "K013": {"desc": "Individual Counselling x3",         "fee":  80.00},
    "K033": {"desc": "Individual Counselling",            "fee":  56.30},
    "K040": {"desc": "Group counselling x3",              "fee":  80.00},
    "K041": {"desc": "Group counselling",                 "fee":  56.30},
    "A101": {"desc": "Limited Virtual Care by Video",     "fee":  20.00},
    "A102": {"desc": "Limited Virtual Care by Telephone", "fee":  15.00},
}

DX_CODES = {
    "628": "Other Disorders of Female Genital Tract: Infertility",
    "606": "Diseases of Male Genital Organs: Male infertility, oligospermia, azoospermia",
    "626": "Other Disorders of Female Genital Tract: Disorders of menstruation",
    "625": "Other Disorders of Female Genital Tract: Dyspareunia, dysmenorrhea, premenstrual tension, stress incontinence",
    "895": "Family Planning: Family planning, contraceptive advice, advice on surgical contraception or abortion",
    "758": "Chromosomal anomalies (e.g., Down's syndrome, other autosomal anomalies, Klinefelter's syndrome, Turner's syndrome)",
    "632": "Missed abortion",
    "633": "Ectopic pregnancy",
    "634": "Incomplete abortion, complete abortion",
    "635": "Therapeutic abortion",
    "640": "Threatened abortion, haemorrhage in early pregnancy",
    "650": "Normal delivery, uncomplicated pregnancy",
}

# ── DB setup ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            session_date  TEXT NOT NULL,
            imported_at   TEXT NOT NULL,
            source_file   TEXT,
            submitted     INTEGER DEFAULT 0,
            submitted_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS encounters (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id           INTEGER NOT NULL REFERENCES sessions(id),
            patient_id           TEXT,
            partner_id           TEXT,
            patient_name         TEXT NOT NULL,
            health_card          TEXT,
            visit_type           TEXT,
            facility             TEXT,
            status               TEXT,
            encounter_date       TEXT NOT NULL,
            start_time           TEXT,
            end_time             TEXT,
            duration_min         INTEGER,
            referring_md         TEXT,
            referring_md_license TEXT,
            schedule_notes       TEXT,
            last_encounter_date  TEXT,
            last_encounter_type  TEXT,
            months_since_last    INTEGER,
            provider_enc_count   INTEGER,
            billing_codes        TEXT,
            dx_codes             TEXT,
            sex                  TEXT,
            notes                TEXT,
            locked               INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS historical_imports (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            imported_at TEXT NOT NULL,
            source_file TEXT,
            row_count   INTEGER
        );
    """)
    con.commit()
    con.close()
    log.info("Database initialised at %s", DB_PATH)

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_sex(raw: str) -> str:
    """Normalise Ptn_Sex column value → 'F' | 'M' | 'U'."""
    v = str(raw or "").strip().upper()
    if v in ("F", "FEMALE"):
        return "F"
    if v in ("M", "MALE"):
        return "M"
    return "U"

def default_dx(sex: str) -> list:
    if sex == "F":
        return ["628"]
    if sex == "M":
        return ["606"]
    return ["628"]

def referring_license(row) -> str:
    for col in ("State_License_No", "Upin"):
        val = str(row.get(col, "") or "").strip()
        if val and val.lower() not in ("nan", ""):
            # strip float suffix e.g. "22827.0" → "22827"
            if "." in val:
                try:
                    val = str(int(float(val)))
                except ValueError:
                    pass
            return val
    return ""

def db_con():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def safe_int(val):
    try:
        return int(val) if pd.notna(val) else None
    except (ValueError, TypeError):
        return None

# ── Eel API ───────────────────────────────────────────────────────────────────

@eel.expose
def get_reference_data():
    return {"billing_codes": BILLING_CODES, "dx_codes": DX_CODES}

@eel.expose
def import_xls(file_path: str):
    """Parse XLS/XLSX daily billing export. Returns encounter list (unsaved)."""
    try:
        ext    = Path(file_path).suffix.lower()
        engine = "xlrd" if ext == ".xls" else "openpyxl"
        df     = pd.read_excel(file_path, engine=engine)

        required = {"Visit_Type", "Schedule_Date", "From_Time", "To_Time",
                    "Scheduled_EntityName", "Patient_ID"}
        missing  = required - set(df.columns)
        if missing:
            return {"ok": False, "error": f"Missing columns: {', '.join(sorted(missing))}"}

        has_sex_col = "Ptn_Sex" in df.columns
        encounters  = []

        for _, row in df.iterrows():
            row = row.to_dict()

            # Sex: prefer explicit column, fall back to Visit_Type inference
            if has_sex_col and pd.notna(row.get("Ptn_Sex")):
                sex = parse_sex(row["Ptn_Sex"])
            else:
                vt  = str(row.get("Visit_Type", "")).upper()
                sex = "F" if ("FEMALE" in vt or "NPF" in vt) else "M" if "MALE" in vt else "U"

            # Partner ID: convert float → int string
            partner_raw = row.get("Ptn_Partner_Id")
            partner_id  = str(int(partner_raw)) if pd.notna(partner_raw) else ""

            enc = {
                "patient_id":           str(row.get("Patient_ID", "") or ""),
                "partner_id":           partner_id,
                "patient_name":         str(row.get("Scheduled_EntityName", "")).strip(),
                "health_card":          str(row.get("Ptn_SSN", "") or "").strip(),
                "visit_type":           str(row.get("Visit_Type", "") or "").strip(),
                "facility":             str(row.get("Facility_Name", "") or "").strip(),
                "status":               str(row.get("Status", "") or "").strip(),
                "encounter_date":       str(row.get("Schedule_Date", ""))[:10],
                "start_time":           str(row.get("From_Time", "") or "").strip(),
                "end_time":             str(row.get("To_Time", "") or "").strip(),
                "duration_min":         safe_int(row.get("Duration")),
                "referring_md":         str(row.get("Ptn_Referring_MD_Name", "") or "").strip(),
                "referring_md_license": referring_license(row),
                "schedule_notes":       str(row.get("Schedule_Notes", "") or "").strip(),
                "last_encounter_date":  str(row.get("LatestNPEncounterDate", ""))[:10]
                                        if pd.notna(row.get("LatestNPEncounterDate")) else "",
                "last_encounter_type":  str(row.get("LatestNPEncounterVisitType", "") or "").strip(),
                "months_since_last":    safe_int(row.get("MonthsSinceLastEncounter")),
                "provider_enc_count":   safe_int(row.get("ProviderEncounterCount")),
                "sex":                  sex,
                "billing_codes":        ["A205"],
                "dx_codes":             default_dx(sex),
                "notes":                "",
            }
            encounters.append(enc)

        session_date = encounters[0]["encounter_date"] if encounters else ""
        log.info("Imported %d encounters from %s", len(encounters), file_path)
        return {
            "ok":           True,
            "session_date": session_date,
            "source_file":  Path(file_path).name,
            "has_sex_col":  has_sex_col,
            "encounters":   encounters,
        }

    except Exception as e:
        log.exception("import_xls failed")
        return {"ok": False, "error": str(e)}

@eel.expose
def save_session(session_date: str, source_file: str, encounters: list):
    """Commit a reviewed session to the database (explicit user action)."""
    try:
        con = db_con()
        cur = con.cursor()
        cur.execute(
            "INSERT INTO sessions (session_date, imported_at, source_file) VALUES (?,?,?)",
            (session_date, datetime.now().isoformat(), source_file)
        )
        session_id = cur.lastrowid

        for e in encounters:
            cur.execute("""
                INSERT INTO encounters
                  (session_id, patient_id, partner_id, patient_name, health_card,
                   visit_type, facility, status, encounter_date, start_time, end_time,
                   duration_min, referring_md, referring_md_license, schedule_notes,
                   last_encounter_date, last_encounter_type, months_since_last,
                   provider_enc_count, billing_codes, dx_codes, sex, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                session_id,
                e.get("patient_id",""),        e.get("partner_id",""),
                e.get("patient_name",""),       e.get("health_card",""),
                e.get("visit_type",""),         e.get("facility",""),
                e.get("status",""),             e.get("encounter_date",""),
                e.get("start_time",""),         e.get("end_time",""),
                e.get("duration_min"),
                e.get("referring_md",""),       e.get("referring_md_license",""),
                e.get("schedule_notes",""),
                e.get("last_encounter_date",""),e.get("last_encounter_type",""),
                e.get("months_since_last"),     e.get("provider_enc_count"),
                json.dumps(e.get("billing_codes", [])),
                json.dumps(e.get("dx_codes", [])),
                e.get("sex",""),                e.get("notes",""),
            ))

        con.commit()
        con.close()
        log.info("Saved session #%d (%s, %d encounters)", session_id, session_date, len(encounters))
        return {"ok": True, "session_id": session_id}

    except Exception as e:
        log.exception("save_session failed")
        return {"ok": False, "error": str(e)}

@eel.expose
def get_sessions():
    con  = db_con()
    rows = con.execute("""
        SELECT s.id, s.session_date, s.imported_at, s.source_file,
               s.submitted, s.submitted_at,
               COUNT(e.id) AS encounter_count
        FROM sessions s
        LEFT JOIN encounters e ON e.session_id = s.id
        GROUP BY s.id
        ORDER BY s.session_date DESC
    """).fetchall()
    con.close()
    return [dict(r) for r in rows]

@eel.expose
def get_session_encounters(session_id: int):
    con  = db_con()
    rows = con.execute(
        "SELECT * FROM encounters WHERE session_id=? ORDER BY start_time",
        (session_id,)
    ).fetchall()
    con.close()
    result = []
    for r in rows:
        d = dict(r)
        d["billing_codes"] = json.loads(d["billing_codes"] or "[]")
        d["dx_codes"]      = json.loads(d["dx_codes"]      or "[]")
        result.append(d)
    return result

@eel.expose
def update_encounter(encounter_id: int, billing_codes: list, dx_codes: list, notes: str):
    try:
        con = db_con()
        con.execute(
            "UPDATE encounters SET billing_codes=?, dx_codes=?, notes=? WHERE id=?",
            (json.dumps(billing_codes), json.dumps(dx_codes), notes, encounter_id)
        )
        con.commit()
        con.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@eel.expose
def mark_submitted(session_id: int):
    try:
        con = db_con()
        con.execute(
            "UPDATE sessions SET submitted=1, submitted_at=? WHERE id=?",
            (datetime.now().isoformat(), session_id)
        )
        con.commit()
        con.close()
        log.info("Session #%d marked submitted", session_id)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@eel.expose
def export_report(session_id: int, encounters: list, fmt: str):
    """Generate billing report. fmt: 'xlsx' | 'docx' | 'pdf'"""
    try:
        rows = []
        for e in encounters:
            codes    = e.get("billing_codes", [])
            dxs      = e.get("dx_codes", [])
            dx_str   = "; ".join([f"{d} – {DX_CODES.get(d, d)}" for d in dxs])
            code_str = "; ".join(
                [f"{c} – {BILLING_CODES[c]['desc']}" if c in BILLING_CODES else c for c in codes]
            )
            fee_total = sum(BILLING_CODES.get(c, {}).get("fee", 0) for c in codes)

            rows.append({
                "Date":                 e.get("encounter_date", ""),
                "Start Time":           e.get("start_time", ""),
                "End Time":             e.get("end_time", ""),
                "Patient Name":         e.get("patient_name", ""),
                "Health Card":          e.get("health_card", ""),
                "Sex":                  e.get("sex", ""),
                "Visit Type":           e.get("visit_type", ""),
                "Facility":             e.get("facility", ""),
                "Status":               e.get("status", ""),
                "Dx":                   dx_str,
                "Billing Code(s)":      code_str,
                "Fee ($)":              fee_total,
                "Referring Physician":  e.get("referring_md", ""),
                "Referring MD License": e.get("referring_md_license", ""),
                "Notes":                e.get("notes", ""),
            })

        df        = pd.DataFrame(rows)
        ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"BillingReport_Session{session_id}_{ts}"

        # ── XLSX ──────────────────────────────────────────────────────────────
        if fmt == "xlsx":
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.utils import get_column_letter

            out = EXPORT_DIR / f"{base_name}.xlsx"
            with pd.ExcelWriter(out, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Billing Report")
                ws = writer.sheets["Billing Report"]

                header_fill = PatternFill("solid", fgColor="1B3A5C")
                header_font = Font(bold=True, color="FFFFFF", size=10)
                for cell in ws[1]:
                    cell.fill      = header_fill
                    cell.font      = header_font
                    cell.alignment = Alignment(horizontal="center", wrap_text=True)

                for col_idx, col in enumerate(ws.columns, 1):
                    max_len = max((len(str(c.value or "")) for c in col), default=8)
                    ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 55)

                # Alternate row shading
                from openpyxl.styles import PatternFill as PF
                alt_fill = PF("solid", fgColor="EEF2F7")
                for row_idx, row in enumerate(ws.iter_rows(min_row=2), 2):
                    if row_idx % 2 == 0:
                        for cell in row:
                            cell.fill = alt_fill

            return {"ok": True, "path": str(out)}

        # ── DOCX ──────────────────────────────────────────────────────────────
        elif fmt == "docx":
            from docx import Document
            from docx.shared import Pt, RGBColor, Inches, Cm
            from docx.enum.text import WD_ALIGN_PARAGRAPH
            from docx.oxml.ns import qn
            from docx.oxml import OxmlElement

            doc = Document()

            # Page margins
            for section in doc.sections:
                section.left_margin   = Cm(1.5)
                section.right_margin  = Cm(1.5)
                section.top_margin    = Cm(1.8)
                section.bottom_margin = Cm(1.8)

            # Title
            title      = doc.add_heading("", 0)
            title_run  = title.add_run("MichaeliBilling — Billing Report")
            title_run.font.color.rgb = RGBColor(0x1B, 0x3A, 0x5C)
            title.alignment = WD_ALIGN_PARAGRAPH.CENTER

            # Subtitle
            sub = doc.add_paragraph()
            sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
            sub_run = sub.add_run(
                f"Dr. J. Michaeli  |  Date: {rows[0]['Date'] if rows else ''}  |  "
                f"Encounters: {len(rows)}  |  "
                f"Total: ${sum(r['Fee ($)'] for r in rows):.2f}"
            )
            sub_run.font.size  = Pt(10)
            sub_run.font.color.rgb = RGBColor(0x4A, 0x6A, 0x82)

            doc.add_paragraph("")

            # Table
            table = doc.add_table(rows=1, cols=len(df.columns))
            table.style = "Table Grid"

            # Header row
            for i, col_name in enumerate(df.columns):
                cell     = table.rows[0].cells[i]
                cell.text = col_name
                run       = cell.paragraphs[0].runs[0]
                run.bold  = True
                run.font.size = Pt(8)
                run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
                # Header cell shading
                tc_pr = cell._tc.get_or_add_tcPr()
                shd   = OxmlElement("w:shd")
                shd.set(qn("w:fill"), "1B3A5C")
                shd.set(qn("w:color"), "auto")
                shd.set(qn("w:val"), "clear")
                tc_pr.append(shd)

            for row_data in df.itertuples(index=False):
                cells = table.add_row().cells
                for i, val in enumerate(row_data):
                    cells[i].text                         = str(val)
                    cells[i].paragraphs[0].runs[0].font.size = Pt(8)

            out = EXPORT_DIR / f"{base_name}.docx"
            doc.save(out)
            return {"ok": True, "path": str(out)}

        # ── PDF ───────────────────────────────────────────────────────────────
        elif fmt == "pdf":
            from reportlab.lib.pagesizes import landscape, A4
            from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                            Paragraph, Spacer)
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import cm
            from reportlab.lib.enums import TA_CENTER

            out     = EXPORT_DIR / f"{base_name}.pdf"
            doc_pdf = SimpleDocTemplate(
                str(out), pagesize=landscape(A4),
                leftMargin=1*cm, rightMargin=1*cm,
                topMargin=1.5*cm, bottomMargin=1.5*cm
            )

            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                "MBTitle", parent=styles["Title"],
                fontSize=14, alignment=TA_CENTER,
                textColor=colors.HexColor("#1B3A5C")
            )
            sub_style = ParagraphStyle(
                "MBSub", parent=styles["Normal"],
                fontSize=9, alignment=TA_CENTER,
                textColor=colors.HexColor("#4A6A82")
            )

            story = [
                Paragraph("MichaeliBilling — Billing Report", title_style),
                Spacer(1, 0.2*cm),
                Paragraph(
                    f"Dr. J. Michaeli  &nbsp;|&nbsp;  Date: {rows[0]['Date'] if rows else ''}  "
                    f"&nbsp;|&nbsp;  Encounters: {len(rows)}  "
                    f"&nbsp;|&nbsp;  Total: ${sum(r['Fee ($)'] for r in rows):.2f}",
                    sub_style
                ),
                Spacer(1, 0.5*cm),
            ]

            header = list(df.columns)
            data   = [header] + [[str(v) for v in row] for _, row in df.iterrows()]

            t = Table(data, repeatRows=1)
            t.setStyle(TableStyle([
                ("BACKGROUND",     (0, 0), (-1, 0), colors.HexColor("#1B3A5C")),
                ("TEXTCOLOR",      (0, 0), (-1, 0), colors.white),
                ("FONTNAME",       (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",       (0, 0), (-1, -1), 7),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                    [colors.white, colors.HexColor("#EEF2F7")]),
                ("GRID",           (0, 0), (-1, -1), 0.3, colors.HexColor("#CCCCCC")),
                ("VALIGN",         (0, 0), (-1, -1), "TOP"),
                ("PADDING",        (0, 0), (-1, -1), 3),
            ]))
            story.append(t)

            doc_pdf.build(story)
            return {"ok": True, "path": str(out)}

        return {"ok": False, "error": f"Unknown format: {fmt}"}

    except Exception as e:
        log.exception("export_report failed")
        import traceback
        return {"ok": False, "error": traceback.format_exc()}

@eel.expose
def import_historical_csv(file_path: str):
    """Stub — format TBD."""
    return {"ok": False, "error": "Historical CSV import not yet implemented. Format TBD."}

# ── Launch ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    eel.init(str(WEB_DIR))

    icon = str(ICON_PATH) if ICON_PATH.exists() else None

    eel.start(
        "index.html",
        size=(1440, 900),
        port=8765,
        block=True,
        **({"icon": icon} if icon else {}),
    )
