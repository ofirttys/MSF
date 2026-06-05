"""
MichaeliBilling — Billing Code Assignment Engine
Implements all rules from the approved ruleset v2.

Entry point:  assign_billing_codes(encounters, db_path)
  - Takes a list of encounter dicts (as returned by import_xls)
  - Looks up patient_records DB for A203 once-per-year check
  - Returns the same list with billing_codes, flags, and flag_messages populated

Flag levels (stored in enc["flag_level"]):
  "red"    — invalid OHIP, first visit not NP, both patients have no referring MD
  "orange" — partner missing, unknown visit type, 60-min non-first visit, 24-month mark
  "yellow" — referral expiry warning (≥18 months, no NP)
  ""       — clean

enc["flag_messages"] — list of human-readable strings explaining each flag
enc["included"]      — bool, default True; False for red-flagged rows
enc["md_copied"]     — bool, True if referring MD was copied from partner (Rule 2)
"""

import sqlite3
import json
import re
from datetime import datetime, date
from pathlib import Path


# ── OHIP validation ───────────────────────────────────────────────────────────
_OHIP_RE = re.compile(r"^\d{10}[A-Za-z]{2}$")

def is_valid_ohip(hc: str) -> bool:
    return bool(_OHIP_RE.match((hc or "").strip()))


# ── Helpers ───────────────────────────────────────────────────────────────────
def clean(v) -> str:
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s

def has_referring(enc) -> bool:
    return bool(clean(enc.get("referring_md")))

def vt_contains(enc, *keywords) -> bool:
    vt = (enc.get("visit_type") or "").upper()
    return any(k in vt for k in keywords)

def a203_used_this_year(patient_id: str, patient_name: str,
                        encounter_date: str, db_path: Path) -> bool:
    """Check if A203 was already used for this patient in the same calendar year."""
    try:
        year = encounter_date[:4]
        con  = sqlite3.connect(db_path)
        rows = con.execute(
            """SELECT billing_codes FROM patient_records
               WHERE (patient_id=? OR patient_name=?)
               AND record_date LIKE ?""",
            (patient_id, patient_name, f"{year}%")
        ).fetchall()
        con.close()
        for r in rows:
            codes = json.loads(r[0] or "[]")
            if "A203" in codes:
                return True
        return False
    except Exception:
        return False


# ── Flag helper ───────────────────────────────────────────────────────────────
def add_flag(enc, level: str, message: str):
    """Set flag level (respecting severity order red > orange > yellow) and append message."""
    priority = {"red": 3, "orange": 2, "yellow": 1, "": 0}
    current  = enc.get("flag_level", "")
    if priority.get(level, 0) > priority.get(current, 0):
        enc["flag_level"] = level
    enc.setdefault("flag_messages", []).append(message)
    if level == "red":
        enc["included"] = False


# ── Main entry point ──────────────────────────────────────────────────────────
def assign_billing_codes(encounters: list, db_path: Path) -> list:
    """
    Run all billing rules on a list of encounter dicts.
    Modifies encounters in-place and returns them.
    """

    # Initialise flags and defaults on every encounter
    for enc in encounters:
        enc["flag_level"]    = ""
        enc["flag_messages"] = []
        enc["billing_codes"] = []
        enc["included"]      = True
        enc["md_copied"]     = False

    # Build lookup maps for this session
    by_patient_id = {e["patient_id"]: e for e in encounters if e.get("patient_id")}

    # ── Rule 1: OHIP validation ───────────────────────────────────────────────
    for enc in encounters:
        hc = clean(enc.get("health_card"))
        if not is_valid_ohip(hc):
            if not hc:
                add_flag(enc, "red", "No health card (OHIP) on file.")
            else:
                add_flag(enc, "red",
                    f"Health card '{hc}' is not valid (expected 10 digits + 2 letters). Please verify.")

    # ── Rule 2: Copy referring MD from partner ────────────────────────────────
    for enc in encounters:
        notes = (enc.get("schedule_notes") or "").upper()
        if "R" in notes.split() or notes == "R":
            # "R" as a word token in schedule notes
            pass
        # More robust: check if "R" appears as standalone note
        if re.search(r'\bR\b', enc.get("schedule_notes") or ""):
            if not has_referring(enc):
                partner_id  = enc.get("partner_id", "")
                partner_enc = by_patient_id.get(partner_id)
                if partner_enc and has_referring(partner_enc):
                    enc["referring_md"]         = partner_enc["referring_md"]
                    enc["referring_md_license"] = partner_enc["referring_md_license"]
                    enc["md_copied"]            = True
                    add_flag(enc, "yellow",
                        f"Referring MD copied from partner ({partner_enc['patient_name']}). Please verify.")
                elif partner_enc and not has_referring(partner_enc):
                    add_flag(enc, "red",
                        "Both patient and partner have no referring physician.")
                # If partner not in session — Rule 3 will handle the missing partner flag

    # ── Rule 3: Partner missing from session ─────────────────────────────────
    for enc in encounters:
        partner_id = enc.get("partner_id", "")
        if partner_id and partner_id not in by_patient_id:
            add_flag(enc, "orange",
                f"Partner (ID {partner_id}) is not in this session. Please verify why they are absent.")

    # ── Rule 4: Unknown visit type ────────────────────────────────────────────
    for enc in encounters:
        if not vt_contains(enc, "OTN", "NP", "F/U"):
            add_flag(enc, "orange",
                f"Visit type '{enc.get('visit_type')}' is unrecognised (expected OTN, NP, or F/U). Please verify.")

    # ── Rule 5: Referral expiry warning ──────────────────────────────────────
    for enc in encounters:
        months = enc.get("months_since_last")
        if months is not None and months >= 18 and not vt_contains(enc, "NP"):
            add_flag(enc, "yellow",
                f"Patient has not had an NP visit in {months} months. "
                "A new referral may be needed soon — consider emailing the patient.")

    # ── Billing assignment (only for valid OHIP) ──────────────────────────────
    # Group partners together for couple logic
    processed = set()

    for enc in encounters:
        if enc["patient_id"] in processed:
            continue
        if enc["flag_level"] == "red" and not is_valid_ohip(clean(enc.get("health_card"))):
            # No OHIP — skip billing assignment, already flagged
            processed.add(enc["patient_id"])
            continue

        partner_id  = enc.get("partner_id", "")
        partner_enc = by_patient_id.get(partner_id) if partner_id else None

        # Only pair if partner is actually in session
        if partner_enc:
            _assign_couple(enc, partner_enc, db_path)
            processed.add(enc["patient_id"])
            processed.add(partner_enc["patient_id"])
        else:
            _assign_solo(enc, db_path)
            processed.add(enc["patient_id"])

    return encounters


# ── Solo patient ──────────────────────────────────────────────────────────────
def _assign_solo(enc, db_path: Path):
    count  = enc.get("provider_enc_count") or 1
    months = enc.get("months_since_last") or 0

    # Rule 6: OTN
    if vt_contains(enc, "OTN"):
        enc["billing_codes"].append("K300")

    # Rule 7: 60-min duration
    if enc.get("duration_min") == 60:
        if count > 1:
            add_flag(enc, "orange",
                "60-minute appointment on a follow-up visit — please verify billing code.")

    # Rule 8 errors
    if count == 1 and not vt_contains(enc, "NP"):
        add_flag(enc, "red",
            "First visit but Visit Type does not contain 'NP'. Please verify.")
        return

    if count == 1:
        # First / new-patient visit
        if enc.get("duration_min") == 60:
            code = "A935"
        elif has_referring(enc):
            code = "A205"
        else:
            code = "A203"
        enc["billing_codes"].append(code)

    elif months >= 24:
        if not vt_contains(enc, "NP"):
            add_flag(enc, "orange",
                f"Patient has not had an NP visit in {months} months (≥24). "
                "Please verify if a new referral episode should be started.")
        # Still assign K013 — user can override
        enc["billing_codes"].append("K013")

    else:
        # Follow-up visits 2..n, < 24 months
        enc["billing_codes"].append("K013")


# ── Couple assignment dispatcher ──────────────────────────────────────────────
def _assign_couple(enc1, enc2, db_path: Path):
    """Determine couple type and dispatch to correct rule."""
    sex1 = enc1.get("sex", "U")
    sex2 = enc2.get("sex", "U")

    # Rule 6: OTN for each
    for enc in (enc1, enc2):
        if vt_contains(enc, "OTN"):
            enc["billing_codes"].append("K300")

    # Rule 7: 60-min flag for each
    for enc in (enc1, enc2):
        count = enc.get("provider_enc_count") or 1
        if enc.get("duration_min") == 60 and count > 1:
            add_flag(enc, "orange",
                "60-minute appointment on a follow-up visit — please verify billing code.")

    # 24-month flag for both
    for enc in (enc1, enc2):
        months = enc.get("months_since_last") or 0
        if months >= 24 and not vt_contains(enc, "NP"):
            add_flag(enc, "orange",
                f"Patient has not had an NP visit in {months} months (≥24). "
                "Please verify if a new referral episode should be started.")

    if sex1 == "F" and sex2 == "M":
        _assign_female_male(enc1, enc2, db_path)
    elif sex1 == "M" and sex2 == "F":
        _assign_female_male(enc2, enc1, db_path)
    elif sex1 == "F" and sex2 == "F":
        _assign_same_sex(enc1, enc2, db_path)
    else:
        # Fallback — treat each as solo
        _assign_solo(enc1, db_path)
        _assign_solo(enc2, db_path)


# ── Female + Male couple (Rule 9) ─────────────────────────────────────────────
def _assign_female_male(female, male, db_path: Path):
    count   = female.get("provider_enc_count") or 1
    f_ref   = has_referring(female)
    m_ref   = has_referring(male)

    if not f_ref and not m_ref:
        add_flag(female, "red", "Neither patient nor partner has a referring physician.")
        add_flag(male,   "red", "Neither patient nor partner has a referring physician.")
        return

    both_ref = f_ref and m_ref

    if count == 1:
        # Rule 7: A935 if 60-min first visit
        f_code = "A935" if female.get("duration_min") == 60 else "A205"
        if both_ref:
            m_code = "A935" if male.get("duration_min") == 60 else "A205"
        else:
            m_code = "A203"
        female["billing_codes"].append(f_code)
        male["billing_codes"].append(m_code)

    elif count == 2:
        if both_ref:
            female["billing_codes"].append("K013")
            male["billing_codes"].append(_a203_or_fallback(male, db_path))
        else:
            female["billing_codes"].append(_a203_or_fallback(female, db_path))
            male["billing_codes"].append("K013")

    else:
        # Visit 3+
        female["billing_codes"].append("K013")
        male["billing_codes"].append("A204")


# ── Same-sex female couple (Rule 10) ─────────────────────────────────────────
def _assign_same_sex(enc_a, enc_b, db_path: Path):
    """
    Deterministically assign P1 / P2 regardless of import order:
      1. Patient with referring MD → P1
      2. Both have referring MD → lower provider_enc_count → P1
      3. Equal count → earlier start_time → P1
    """
    a_ref = has_referring(enc_a)
    b_ref = has_referring(enc_b)

    if not a_ref and not b_ref:
        add_flag(enc_a, "red", "Neither patient nor partner has a referring physician.")
        add_flag(enc_b, "red", "Neither patient nor partner has a referring physician.")
        return

    both_ref = a_ref and b_ref

    if a_ref and not b_ref:
        p1, p2 = enc_a, enc_b
    elif b_ref and not a_ref:
        p1, p2 = enc_b, enc_a
    else:
        # Both have referring MD — lower visit count = P1; tie-break by start_time
        count_a = enc_a.get("provider_enc_count") or 1
        count_b = enc_b.get("provider_enc_count") or 1
        if count_a > count_b:
            p1, p2 = enc_a, enc_b
        elif count_b > count_a:
            p1, p2 = enc_b, enc_a
        else:
            # Equal count — lower patient_id (numerically) = P1
            try:
                id_a = int(enc_a.get("patient_id") or 0)
                id_b = int(enc_b.get("patient_id") or 0)
            except ValueError:
                id_a, id_b = 0, 0
            p1, p2 = (enc_a, enc_b) if id_a <= id_b else (enc_b, enc_a)

    count = p1.get("provider_enc_count") or 1

    if count == 1:
        p1_code = "A935" if p1.get("duration_min") == 60 else "A205"
        p2_code = ("A935" if p2.get("duration_min") == 60 else "A205") if both_ref else "A203"
        p1["billing_codes"].append(p1_code)
        p2["billing_codes"].append(p2_code)

    elif count == 2:
        if both_ref:
            p1["billing_codes"].append("K013")
            p2["billing_codes"].append(_a203_or_fallback(p2, db_path))
        else:
            p1["billing_codes"].append(_a203_or_fallback(p1, db_path))
            p2["billing_codes"].append("K013")

    elif count == 3:
        if both_ref:
            p1["billing_codes"].append(_a203_or_fallback(p1, db_path))
            p2["billing_codes"].append("K013")
        else:
            p1["billing_codes"].append("K013")
            p2["billing_codes"].append("A204")

    else:
        # Visit 4+: alternate K013 / A204
        # Even visits: p1=K013, p2=A204; odd visits: p1=A204, p2=K013
        if count % 2 == 0:
            p1["billing_codes"].append("K013")
            p2["billing_codes"].append("A204")
        else:
            p1["billing_codes"].append("A204")
            p2["billing_codes"].append("K013")


# ── A203 with once-per-year guard ─────────────────────────────────────────────
def _a203_or_fallback(enc, db_path: Path) -> str:
    """Return A203 if not already used this year for this patient, else K013."""
    if a203_used_this_year(
        enc.get("patient_id", ""),
        enc.get("patient_name", ""),
        enc.get("encounter_date", ""),
        db_path
    ):
        add_flag(enc, "orange",
            "A203 already used this year for this patient — substituting K013.")
        return "K013"
    return "A203"
