// ── State ─────────────────────────────────────────────────────────────────────
let REF = { billing_codes: {}, dx_codes: {} };

let currentSession = {
  session_date: "",
  source_file:  "",
  encounters:   [],   // working copy (unsaved)
  session_id:   null, // set after DB save
};

let editingIndex = null; // index into currentSession.encounters

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  setupDropZone();

  try {
    REF = await eel.get_reference_data()();
    setDbStatus(true);
  } catch (e) {
    setDbStatus(false);
  }

  loadHistory();
});

function setDbStatus(ok) {
  const dot  = document.getElementById("dbStatus");
  const text = document.getElementById("dbStatusText");
  dot.className  = "status-dot " + (ok ? "ok" : "err");
  text.textContent = ok ? "DB connected" : "DB error";
}

// ── View routing ──────────────────────────────────────────────────────────────
function showView(name) {
  document.querySelectorAll(".view").forEach(v => v.classList.remove("active"));
  document.querySelectorAll(".nav-btn").forEach(b => b.classList.remove("active"));
  document.getElementById("view-" + name).classList.add("active");
  document.querySelector(`[data-view="${name}"]`).classList.add("active");

  if (name === "history") loadHistory();
}

// ── File import ───────────────────────────────────────────────────────────────
function setupDropZone() {
  const zone = document.getElementById("dropZone");
  zone.addEventListener("dragover",  e => { e.preventDefault(); zone.classList.add("drag-over"); });
  zone.addEventListener("dragleave", () => zone.classList.remove("drag-over"));
  zone.addEventListener("drop", e => {
    e.preventDefault();
    zone.classList.remove("drag-over");
    const file = e.dataTransfer.files[0];
    if (file) processFile(file);
  });
}

function handleFileSelect(evt) {
  const file = evt.target.files[0];
  if (file) processFile(file);
}

async function processFile(file) {
  const statusEl = document.getElementById("importStatus");
  statusEl.className = "import-status hidden";
  statusEl.textContent = "";

  showStatus("Parsing " + file.name + "…", "ok", false);

  try {
    // Eel needs a filesystem path — on desktop this is the real path
    // In dev we use the file.path property (Electron / pywebview / eel all expose it)
    const filePath = file.path || file.name;
    const result   = await eel.import_xls(filePath)();

    if (!result.ok) {
      showStatus("❌ " + result.error, "err", true);
      return;
    }

    currentSession.session_date = result.session_date;
    currentSession.source_file  = result.source_file;
    currentSession.encounters   = result.encounters;
    currentSession.session_id   = null;

    showStatus(
      `✓ Loaded ${result.encounters.length} encounters for ${result.session_date} — review and edit before saving.`,
      "ok", true
    );

    renderReviewTable();
    showView("review");

  } catch (e) {
    showStatus("❌ " + e.toString(), "err", true);
  }
}

function showStatus(msg, cls, visible) {
  const el = document.getElementById("importStatus");
  el.textContent = msg;
  el.className   = "import-status " + cls + (visible ? "" : " hidden");
}

// ── Review table ──────────────────────────────────────────────────────────────
function renderReviewTable() {
  const enc = currentSession.encounters;
  if (!enc || enc.length === 0) return;

  document.getElementById("reviewEmpty").style.display       = "none";
  document.getElementById("encounterTableWrap").style.display = "block";
  document.getElementById("summaryBar").style.display        = "flex";
  document.getElementById("reviewSubtitle").textContent =
    `${currentSession.session_date} — ${currentSession.source_file}`;

  updateSummary(enc);

  const tbody = document.getElementById("encounterTbody");
  tbody.innerHTML = "";

  enc.forEach((e, i) => {
    const tr = document.createElement("tr");
    tr.dataset.index = i;
    tr.addEventListener("click", () => openEdit(i));
    tr.innerHTML = `
      <td style="color:var(--text-dim);font-size:11px">${i + 1}</td>
      <td style="white-space:nowrap;font-family:var(--font);font-size:12px">
        ${e.start_time}<br><span style="color:var(--text-dim)">${e.end_time}</span>
      </td>
      <td style="font-weight:600">${escHtml(e.patient_name)}</td>
      <td style="font-family:var(--font);font-size:11px;color:var(--text-sub)">${escHtml(e.health_card)}</td>
      <td><span class="sex-badge ${e.sex}">${e.sex}</span></td>
      <td style="font-size:11px;color:var(--text-sub)">${escHtml(e.visit_type)}</td>
      <td style="font-size:11px;color:var(--text-sub)">${escHtml(e.facility)}</td>
      <td>${statusBadge(e.status)}</td>
      <td style="font-size:11px">${escHtml(e.referring_md)}<br>
        <span style="color:var(--text-dim);font-family:var(--font);font-size:10px">${escHtml(e.referring_md_license)}</span>
      </td>
      <td style="font-size:10px;color:var(--text-dim);font-family:var(--font)">${escHtml(e.referring_md_license)}</td>
      <td id="cell-billing-${i}">${renderCodeChips(e.billing_codes, false)}</td>
      <td id="cell-dx-${i}">${renderCodeChips(e.dx_codes, true)}</td>
      <td style="font-size:11px;color:var(--text-sub)">${escHtml(e.notes)}</td>
      <td style="font-weight:600;color:var(--success);white-space:nowrap" id="cell-fee-${i}">
        $${calcFee(e.billing_codes).toFixed(2)}
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function updateSummary(enc) {
  document.getElementById("sumTotal").textContent       = enc.length;
  document.getElementById("sumFemale").textContent      = enc.filter(e => e.sex === "F").length;
  document.getElementById("sumMale").textContent        = enc.filter(e => e.sex === "M").length;
  document.getElementById("sumVirtual").textContent     = enc.filter(e => e.facility === "Virtual").length;
  document.getElementById("sumCheckedOut").textContent  = enc.filter(e => e.status === "Checked-Out").length;
  const total = enc.reduce((s, e) => s + calcFee(e.billing_codes), 0);
  document.getElementById("sumFee").textContent = "$" + total.toFixed(2);
}

function renderCodeChips(codes, isDx) {
  if (!codes || codes.length === 0) return '<span style="color:var(--text-dim)">—</span>';
  return codes.map(c => `<span class="chip${isDx ? ' dx' : ''}">${escHtml(c)}</span>`).join("");
}

function statusBadge(status) {
  const cls = status === "Checked-Out" ? "checked-out" : "confirmed";
  return `<span class="status-badge ${cls}">${escHtml(status)}</span>`;
}

function calcFee(codes) {
  if (!codes) return 0;
  return codes.reduce((s, c) => s + (REF.billing_codes[c]?.fee || 0), 0);
}

// ── Edit modal ────────────────────────────────────────────────────────────────
function openEdit(index) {
  editingIndex = index;
  const e = currentSession.encounters[index];

  document.getElementById("modalTitle").textContent =
    `Edit — ${e.patient_name}`;

  // Info grid
  const infoGrid = document.getElementById("modalInfoGrid");
  const infoFields = [
    ["Date",           e.encounter_date],
    ["Time",           `${e.start_time} – ${e.end_time}`],
    ["Facility",       e.facility],
    ["Visit Type",     e.visit_type],
    ["Status",         e.status],
    ["Sex",            e.sex === "F" ? "Female" : e.sex === "M" ? "Male" : "Unknown"],
    ["Health Card",    e.health_card],
    ["Patient ID",     e.patient_id],
    ["Partner ID",     e.partner_id || "—"],
    ["Referring MD",   e.referring_md || "—"],
    ["MD License",     e.referring_md_license || "—"],
    ["Prior Visits",   e.provider_enc_count ?? "—"],
    ["Last Encounter", e.last_encounter_date || "—"],
    ["Months Since",   e.months_since_last ?? "—"],
    ["Notes Flag",     e.schedule_notes || "—"],
  ];
  infoGrid.innerHTML = infoFields.map(([lbl, val]) => `
    <div class="info-cell">
      <div class="info-cell-label">${lbl}</div>
      <div class="info-cell-value">${escHtml(String(val))}</div>
    </div>
  `).join("");

  // Billing code chips
  const bcGrid = document.getElementById("billingCodeGrid");
  bcGrid.innerHTML = Object.entries(REF.billing_codes).map(([code, info]) => {
    const sel = e.billing_codes.includes(code) ? "selected" : "";
    return `
      <div class="code-chip ${sel}" data-code="${code}" onclick="toggleCode(this,'billing')">
        <span class="code-chip-code">${code}</span>
        <span class="code-chip-desc">${escHtml(info.desc)}</span>
        <span class="code-chip-fee">$${info.fee.toFixed(2)}</span>
      </div>
    `;
  }).join("");

  // Dx code chips
  const dxGrid = document.getElementById("dxCodeGrid");
  dxGrid.innerHTML = Object.entries(REF.dx_codes).map(([code, desc]) => {
    const sel = e.dx_codes.includes(code) ? "selected dx-chip" : "";
    return `
      <div class="code-chip ${sel} dx-chip" data-code="${code}" onclick="toggleCode(this,'dx')">
        <span class="code-chip-code">${code}</span>
        <span class="code-chip-desc">${escHtml(desc)}</span>
      </div>
    `;
  }).join("");

  document.getElementById("modalNotes").value = e.notes || "";
  updateModalFee();

  document.getElementById("editModal").classList.add("open");
}

function toggleCode(el, type) {
  el.classList.toggle("selected");
  if (type === "dx") el.classList.toggle("dx-chip", el.classList.contains("selected"));
  updateModalFee();
}

function updateModalFee() {
  const selected = [...document.querySelectorAll("#billingCodeGrid .code-chip.selected")]
    .map(el => el.dataset.code);
  const fee = calcFee(selected);
  document.getElementById("modalFeeDisplay").textContent =
    selected.length > 0 ? `Total: $${fee.toFixed(2)}` : "";
}

function applyEdit() {
  const billingCodes = [...document.querySelectorAll("#billingCodeGrid .code-chip.selected")]
    .map(el => el.dataset.code);
  const dxCodes = [...document.querySelectorAll("#dxCodeGrid .code-chip.selected")]
    .map(el => el.dataset.code);
  const notes = document.getElementById("modalNotes").value.trim();

  if (billingCodes.length === 0) {
    toast("Select at least one billing code.", "warn");
    return;
  }
  if (dxCodes.length === 0) {
    toast("Select at least one Dx code.", "warn");
    return;
  }

  const e = currentSession.encounters[editingIndex];
  e.billing_codes = billingCodes;
  e.dx_codes      = dxCodes;
  e.notes         = notes;

  // Update row in table
  document.getElementById(`cell-billing-${editingIndex}`).innerHTML = renderCodeChips(billingCodes, false);
  document.getElementById(`cell-dx-${editingIndex}`).innerHTML      = renderCodeChips(dxCodes, true);
  document.getElementById(`cell-fee-${editingIndex}`).textContent   = "$" + calcFee(billingCodes).toFixed(2);

  // Update summary totals
  updateSummary(currentSession.encounters);

  // If session is already saved, update DB record
  if (e.id) {
    eel.update_encounter(e.id, billingCodes, dxCodes, notes)().then(r => {
      if (!r.ok) toast("DB update failed: " + r.error, "err");
    });
  }

  closeModal();
  toast("Encounter updated.", "ok");
}

function closeModal(evt) {
  if (evt && evt.target !== document.getElementById("editModal")) return;
  document.getElementById("editModal").classList.remove("open");
  editingIndex = null;
}

// ── Save session ──────────────────────────────────────────────────────────────
async function saveSession() {
  if (!currentSession.encounters.length) {
    toast("No session loaded.", "warn");
    return;
  }

  if (currentSession.session_id !== null) {
    toast("Session already saved to database.", "warn");
    return;
  }

  const result = await eel.save_session(
    currentSession.session_date,
    currentSession.source_file,
    currentSession.encounters
  )();

  if (!result.ok) {
    toast("Save failed: " + result.error, "err");
    return;
  }

  currentSession.session_id = result.session_id;
  toast(`✓ Session saved (ID ${result.session_id}).`, "ok");
}

// ── Export ────────────────────────────────────────────────────────────────────
async function exportReport(fmt) {
  if (!currentSession.encounters.length) {
    toast("No session loaded.", "warn");
    return;
  }

  const sid = currentSession.session_id ?? 0;
  toast("Generating " + fmt.toUpperCase() + "…", "ok");

  const result = await eel.export_report(sid, currentSession.encounters, fmt)();
  if (!result.ok) {
    toast("Export failed: " + result.error, "err");
  } else {
    toast(`✓ Saved: ${result.path}`, "ok");
  }
}

// ── History ───────────────────────────────────────────────────────────────────
async function loadHistory() {
  const sessions = await eel.get_sessions()();
  const tbody = document.getElementById("historyTbody");
  tbody.innerHTML = "";

  if (!sessions || sessions.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;color:var(--text-dim);padding:24px">No sessions saved yet.</td></tr>`;
    return;
  }

  sessions.forEach(s => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td style="font-family:var(--font);color:var(--text-dim)">#${s.id}</td>
      <td style="font-weight:600">${s.session_date}</td>
      <td>${s.encounter_count}</td>
      <td style="font-size:11px;color:var(--text-sub)">${escHtml(s.source_file || "")}</td>
      <td style="font-size:11px;color:var(--text-dim)">${s.imported_at?.slice(0,16).replace("T"," ") || ""}</td>
      <td>${s.submitted
        ? `<span class="status-badge checked-out">Submitted ${s.submitted_at?.slice(0,10)}</span>`
        : `<span class="status-badge confirmed">Pending</span>`
      }</td>
      <td style="display:flex;gap:6px">
        <button class="btn btn-outline btn-sm" onclick="loadSessionFromHistory(${s.id})">Load</button>
        ${!s.submitted ? `<button class="btn btn-success btn-sm" onclick="markSubmitted(${s.id})">Mark Submitted</button>` : ""}
      </td>
    `;
    tbody.appendChild(tr);
  });
}

async function loadSessionFromHistory(sessionId) {
  const encs = await eel.get_session_encounters(sessionId)();
  if (!encs || encs.length === 0) {
    toast("No encounters found for session " + sessionId, "warn");
    return;
  }

  currentSession.session_date = encs[0].encounter_date;
  currentSession.source_file  = "DB Session #" + sessionId;
  currentSession.encounters   = encs;
  currentSession.session_id   = sessionId;

  renderReviewTable();
  showView("review");
  toast(`✓ Loaded session #${sessionId} from database.`, "ok");
}

async function markSubmitted(sessionId) {
  const result = await eel.mark_submitted(sessionId)();
  if (!result.ok) {
    toast("Failed: " + result.error, "err");
  } else {
    toast("Session marked as submitted.", "ok");
    loadHistory();
  }
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function escHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g,"&amp;")
    .replace(/</g,"&lt;")
    .replace(/>/g,"&gt;")
    .replace(/"/g,"&quot;");
}

let _toastTimer = null;
function toast(msg, cls = "ok") {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className   = "toast " + cls + " show";
  if (_toastTimer) clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove("show"), 4000);
}
