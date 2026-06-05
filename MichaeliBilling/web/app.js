// ── State ─────────────────────────────────────────────────────────────────────
let REF = { billing_codes: {}, dx_codes: {} };

let currentSession = {
  session_date: "",
  source_file:  "",
  encounters:   [],
  session_id:   null,
};

let editingIndex = null;

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  setupDropZone();
  applyStoredTheme();

  try {
    REF = await eel.get_reference_data()();
    setDbStatus(true);
  } catch (e) {
    setDbStatus(false);
  }

  loadHistory();
});

// ── Theme ─────────────────────────────────────────────────────────────────────
function applyStoredTheme() {
  const stored = localStorage.getItem("mb_theme") || "dark";
  if (stored === "light") {
    document.body.classList.add("light");
    document.getElementById("themeToggle").textContent = "🌙";
  } else {
    document.body.classList.remove("light");
    document.getElementById("themeToggle").textContent = "☀";
  }
}

function toggleTheme() {
  const isLight = document.body.classList.toggle("light");
  document.getElementById("themeToggle").textContent = isLight ? "🌙" : "☀";
  localStorage.setItem("mb_theme", isLight ? "light" : "dark");
}

// ── DB status ─────────────────────────────────────────────────────────────────
function setDbStatus(ok) {
  document.getElementById("dbStatus").className    = "status-dot " + (ok ? "ok" : "err");
  document.getElementById("dbStatusText").textContent = ok ? "DB connected" : "DB error";
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
  document.getElementById("importStatus").className = "import-status hidden";
  showStatus("Parsing " + file.name + "…", "ok", false);
  try {
    const arrayBuffer = await file.arrayBuffer();
    const uint8Array  = Array.from(new Uint8Array(arrayBuffer));
    const result      = await eel.import_xls_bytes(uint8Array, file.name)();

    if (!result.ok) { showStatus("❌ " + result.error, "err", true); return; }

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
  const el     = document.getElementById("importStatus");
  el.textContent = msg;
  el.className   = "import-status " + cls + (visible ? "" : " hidden");
}

// ── Review table ──────────────────────────────────────────────────────────────
// Columns: # | Time | Patient | HC# | Sex | Status | Referring MD / Billing# | Billing Code(s) | Dx | Notes | Fee
function renderReviewTable() {
  const enc = currentSession.encounters;
  if (!enc || enc.length === 0) return;

  document.getElementById("reviewEmpty").style.display        = "none";
  document.getElementById("encounterTableWrap").style.display = "block";
  document.getElementById("summaryBar").style.display         = "flex";
  document.getElementById("reviewSubtitle").textContent =
    `${currentSession.session_date} — ${currentSession.source_file}`;

  updateSummary(enc);

  const tbody = document.getElementById("encounterTbody");
  tbody.innerHTML = "";

  enc.forEach((e, i) => {
    const tr = document.createElement("tr");
    tr.dataset.index = i;
    tr.addEventListener("click", () => openEdit(i));

    // Referring MD: name on top, billing# below (empty if nan/blank)
    const mdName    = cleanVal(e.referring_md);
    const mdLicense = cleanVal(e.referring_md_license);
    const mdCell    = mdName
      ? `<div class="md-cell">${escHtml(mdName)}${mdLicense ? `<div class="md-license">${escHtml(mdLicense)}</div>` : ""}</div>`
      : (mdLicense ? `<div class="md-license" style="margin-top:0">${escHtml(mdLicense)}</div>` : `<span style="color:var(--text-dim)">—</span>`);

    tr.innerHTML = `
      <td style="color:var(--text-dim);font-size:11px">${i + 1}</td>
      <td class="time-cell" id="cell-time-${i}">
        ${escHtml(e.start_time)}<br>
        <span class="time-end">${escHtml(e.end_time)}</span>
      </td>
      <td style="font-weight:600">${escHtml(e.patient_name)}</td>
      <td style="font-family:'Courier New',monospace;font-size:11px;color:var(--text-sub)">${escHtml(e.health_card)}</td>
      <td><span class="sex-badge ${e.sex}">${e.sex}</span></td>
      <td>${statusBadge(e.status)}</td>
      <td>${mdCell}</td>
      <td id="cell-billing-${i}">${renderCodeChips(e.billing_codes, false)}</td>
      <td id="cell-dx-${i}">${renderCodeChips(e.dx_codes, true)}</td>
      <td style="font-size:11px;color:var(--text-sub)" id="cell-notes-${i}">${escHtml(e.notes)}</td>
      <td style="font-weight:600;color:var(--success);white-space:nowrap" id="cell-fee-${i}">
        $${calcFee(e.billing_codes).toFixed(2)}
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function cleanVal(v) {
  if (!v) return "";
  const s = String(v).trim();
  return (s === "" || s.toLowerCase() === "nan") ? "" : s;
}

function updateSummary(enc) {
  document.getElementById("sumTotal").textContent      = enc.length;
  document.getElementById("sumFemale").textContent     = enc.filter(e => e.sex === "F").length;
  document.getElementById("sumMale").textContent       = enc.filter(e => e.sex === "M").length;
  document.getElementById("sumVirtual").textContent    = enc.filter(e => e.facility === "Virtual").length;
  document.getElementById("sumCheckedOut").textContent = enc.filter(e => e.status === "Checked-Out").length;
  const total = enc.reduce((s, e) => s + calcFee(e.billing_codes), 0);
  document.getElementById("sumFee").textContent = "$" + total.toFixed(2);
}

function renderCodeChips(codes, isDx) {
  if (!codes || codes.length === 0)
    return '<span style="color:var(--text-dim)">—</span>';
  return codes.map(c =>
    `<span class="chip${isDx ? " dx" : ""}">${escHtml(c)}</span>`
  ).join("");
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

  document.getElementById("modalTitle").textContent = `Edit — ${e.patient_name}`;

  // Info grid (read-only context fields)
  const infoFields = [
    ["Date",           e.encounter_date],
    ["Facility",       e.facility],
    ["Visit Type",     e.visit_type],
    ["Status",         e.status],
    ["Sex",            e.sex === "F" ? "Female" : e.sex === "M" ? "Male" : "Unknown"],
    ["Health Card",    e.health_card],
    ["Patient ID",     e.patient_id],
    ["Partner ID",     e.partner_id || "—"],
    ["Referring MD",   cleanVal(e.referring_md) || "—"],
    ["MD Billing#",    cleanVal(e.referring_md_license) || "—"],
    ["Prior Visits",   e.provider_enc_count ?? "—"],
    ["Last Encounter", e.last_encounter_date || "—"],
    ["Months Since",   e.months_since_last ?? "—"],
    ["Notes Flag",     e.schedule_notes || "—"],
  ];
  document.getElementById("modalInfoGrid").innerHTML = infoFields.map(([lbl, val]) => `
    <div class="info-cell">
      <div class="info-cell-label">${lbl}</div>
      <div class="info-cell-value">${escHtml(String(val))}</div>
    </div>
  `).join("");

  // Editable time fields
  document.getElementById("modalStartTime").value = e.start_time || "";
  document.getElementById("modalEndTime").value   = e.end_time   || "";

  // Billing code chips
  document.getElementById("billingCodeGrid").innerHTML =
    Object.entries(REF.billing_codes).map(([code, info]) => {
      const sel = e.billing_codes.includes(code) ? "selected" : "";
      return `
        <div class="code-chip ${sel}" data-code="${code}" onclick="toggleCode(this,'billing')">
          <span class="code-chip-code">${code}</span>
          <span class="code-chip-desc">${escHtml(info.desc)}</span>
          <span class="code-chip-fee">$${info.fee.toFixed(2)}</span>
        </div>`;
    }).join("");

  // Dx code chips
  document.getElementById("dxCodeGrid").innerHTML =
    Object.entries(REF.dx_codes).map(([code, desc]) => {
      const sel = e.dx_codes.includes(code) ? "selected dx-chip" : "";
      return `
        <div class="code-chip ${sel} dx-chip" data-code="${code}" onclick="toggleCode(this,'dx')">
          <span class="code-chip-code">${code}</span>
          <span class="code-chip-desc">${escHtml(desc)}</span>
        </div>`;
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
  const notes     = document.getElementById("modalNotes").value.trim();
  const startTime = document.getElementById("modalStartTime").value.trim();
  const endTime   = document.getElementById("modalEndTime").value.trim();

  if (billingCodes.length === 0) { toast("Select at least one billing code.", "warn"); return; }
  if (dxCodes.length === 0)      { toast("Select at least one Dx code.", "warn");      return; }

  const e = currentSession.encounters[editingIndex];
  e.billing_codes = billingCodes;
  e.dx_codes      = dxCodes;
  e.notes         = notes;
  e.start_time    = startTime;
  e.end_time      = endTime;

  // Update table cells
  document.getElementById(`cell-time-${editingIndex}`).innerHTML =
    `${escHtml(startTime)}<br><span class="time-end">${escHtml(endTime)}</span>`;
  document.getElementById(`cell-billing-${editingIndex}`).innerHTML = renderCodeChips(billingCodes, false);
  document.getElementById(`cell-dx-${editingIndex}`).innerHTML      = renderCodeChips(dxCodes, true);
  document.getElementById(`cell-notes-${editingIndex}`).textContent = notes;
  document.getElementById(`cell-fee-${editingIndex}`).textContent   = "$" + calcFee(billingCodes).toFixed(2);

  updateSummary(currentSession.encounters);

  // Persist immediately if already saved to DB
  if (e.id) {
    eel.update_encounter(e.id, billingCodes, dxCodes, notes, startTime, endTime)().then(r => {
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
  if (!currentSession.encounters.length) { toast("No session loaded.", "warn"); return; }
  if (currentSession.session_id !== null) { toast("Session already saved to database.", "warn"); return; }

  const result = await eel.save_session(
    currentSession.session_date, currentSession.source_file, currentSession.encounters
  )();

  if (!result.ok) { toast("Save failed: " + result.error, "err"); return; }
  currentSession.session_id = result.session_id;
  toast(`✓ Session saved (ID ${result.session_id}).`, "ok");
}

// ── Export ────────────────────────────────────────────────────────────────────
async function exportReport(fmt) {
  if (!currentSession.encounters.length) { toast("No session loaded.", "warn"); return; }
  toast("Generating " + fmt.toUpperCase() + "…", "ok");
  const result = await eel.export_report(
    currentSession.session_id ?? 0, currentSession.encounters, fmt
  )();
  if (!result.ok) toast("Export failed: " + result.error, "err");
  else            toast(`✓ Saved: ${result.path}`, "ok");
}

// ── History ───────────────────────────────────────────────────────────────────
async function loadHistory() {
  const sessions = await eel.get_sessions()();
  const tbody    = document.getElementById("historyTbody");
  tbody.innerHTML = "";

  if (!sessions || sessions.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;color:var(--text-dim);padding:24px">No sessions saved yet.</td></tr>`;
    return;
  }

  sessions.forEach(s => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td style="font-family:'Courier New',monospace;color:var(--text-dim)">#${s.id}</td>
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
        ${!s.submitted
          ? `<button class="btn btn-success btn-sm" onclick="markSubmitted(${s.id})">Mark Submitted</button>`
          : ""}
      </td>
    `;
    tbody.appendChild(tr);
  });
}

async function loadSessionFromHistory(sessionId) {
  const encs = await eel.get_session_encounters(sessionId)();
  if (!encs || encs.length === 0) { toast("No encounters found for session " + sessionId, "warn"); return; }

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
  if (!result.ok) toast("Failed: " + result.error, "err");
  else { toast("Session marked as submitted.", "ok"); loadHistory(); }
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function escHtml(str) {
  if (!str) return "";
  return String(str).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

let _toastTimer = null;
function toast(msg, cls = "ok") {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className   = "toast " + cls + " show";
  if (_toastTimer) clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove("show"), 4000);
}

// ── Patient Records ───────────────────────────────────────────────────────────
let recordsPage  = 1;
const PAGE_SIZE  = 50;
let _searchTimer = null;

function debounceSearch() {
  if (_searchTimer) clearTimeout(_searchTimer);
  _searchTimer = setTimeout(searchRecords, 350);
}

function clearSearch() {
  document.getElementById("searchQuery").value = "";
  document.getElementById("searchDate").value  = "";
  recordsPage = 1;
  searchRecords();
}

async function searchRecords(page) {
  if (page) recordsPage = page;
  const query = document.getElementById("searchQuery").value.trim();
  const date  = document.getElementById("searchDate").value.trim();

  const result = await eel.search_patient_records(query, date, recordsPage, PAGE_SIZE)();

  if (!result.ok) { toast("Search failed: " + result.error, "err"); return; }

  renderRecordsTable(result.records, result.total);
  renderPagination(result.total);
}

function renderRecordsTable(records, total) {
  const tbody  = document.getElementById("recordsTbody");
  const empty  = document.getElementById("recordsEmpty");
  const wrap   = document.getElementById("recordsTableWrap");
  const count  = document.getElementById("recordsCount");

  count.textContent = total > 0 ? `${total.toLocaleString()} record${total !== 1 ? "s" : ""}` : "";

  if (!records || records.length === 0) {
    empty.style.display = "flex";
    wrap.style.display  = "none";
    return;
  }

  empty.style.display = "none";
  wrap.style.display  = "block";
  tbody.innerHTML     = "";

  records.forEach(r => {
    const tr = document.createElement("tr");
    const src = r.source === "csv_import" ? "csv_import" : "session";
    tr.innerHTML = `
      <td style="font-family:'Courier New',monospace;font-size:12px;white-space:nowrap">${escHtml(r.record_date)}</td>
      <td style="font-weight:600">${escHtml(r.patient_name)}</td>
      <td style="font-family:'Courier New',monospace;font-size:11px;color:var(--text-sub)">${escHtml(r.patient_id || "—")}</td>
      <td>${renderCodeChips(r.billing_codes, false)}</td>
      <td>${renderCodeChips(r.dx_codes, true)}</td>
      <td><span class="source-badge ${src}">${src === "csv_import" ? "CSV" : "Session"}</span></td>
    `;
    tbody.appendChild(tr);
  });
}

function renderPagination(total) {
  const container = document.getElementById("recordsPagination");
  const totalPages = Math.ceil(total / PAGE_SIZE);
  container.innerHTML = "";

  if (totalPages <= 1) return;

  const prev = document.createElement("button");
  prev.className = "page-btn";
  prev.textContent = "← Prev";
  prev.disabled = recordsPage <= 1;
  prev.onclick = () => searchRecords(recordsPage - 1);
  container.appendChild(prev);

  // Page number buttons (show up to 7 around current)
  const start = Math.max(1, recordsPage - 3);
  const end   = Math.min(totalPages, recordsPage + 3);
  for (let p = start; p <= end; p++) {
    const btn = document.createElement("button");
    btn.className = "page-btn" + (p === recordsPage ? " active" : "");
    btn.textContent = p;
    btn.onclick = () => searchRecords(p);
    container.appendChild(btn);
  }

  const next = document.createElement("button");
  next.className = "page-btn";
  next.textContent = "Next →";
  next.disabled = recordsPage >= totalPages;
  next.onclick = () => searchRecords(recordsPage + 1);
  container.appendChild(next);
}

// ── CSV Import ────────────────────────────────────────────────────────────────
async function importCsv(evt) {
  const file = evt.target.files[0];
  evt.target.value = "";   // reset so same file can be re-selected
  if (!file) return;

  const statusEl = document.getElementById("csvStatus");
  statusEl.className   = "import-status ok";
  statusEl.textContent = `Importing ${file.name}…`;

  try {
    const arrayBuffer = await file.arrayBuffer();
    const uint8Array  = Array.from(new Uint8Array(arrayBuffer));
    const result      = await eel.import_historical_csv_bytes(uint8Array, file.name)();

    if (!result.ok) {
      statusEl.className   = "import-status err";
      statusEl.textContent = "❌ " + result.error;
      return;
    }

    let msg = `✓ Imported ${result.inserted} record(s) from ${file.name}`;
    if (result.skipped > 0) msg += ` (${result.skipped} skipped)`;
    statusEl.className   = "import-status ok";
    statusEl.textContent = msg;

    if (result.errors && result.errors.length > 0) {
      console.warn("CSV import warnings:", result.errors);
    }

    // Refresh the table
    searchRecords(1);

  } catch (e) {
    statusEl.className   = "import-status err";
    statusEl.textContent = "❌ " + e.toString();
  }
}

// Load records when switching to that view
const _origShowView = showView;
window.showView = function(name) {
  _origShowView(name);
  if (name === "records" && document.getElementById("recordsTbody").innerHTML === "") {
    searchRecords(1);
  }
};
