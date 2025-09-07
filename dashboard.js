// dashboard.js — full version with OpenAlex profile links and FWCI in publications
(function(){
  // ==== Defaults ====
  const DEFAULT_START_YEAR = 2021;
  const DEFAULT_END_YEAR = 2025;

  document.addEventListener('DOMContentLoaded', () => {
    // Paths used by index.html
    const rosterPath = 'data/roster_with_metrics.csv';
    const pubsPath = 'data/openalex_all_authors_last5y_key_fields_dedup.csv';
    const authorshipsPath = 'data/openalex_all_authors_last5y_key_fields.csv'; // pre-dedup, optional
    
    // In-memory data
    let rosterData = [];   // faculty roster + metrics
    let pubData = [];      // publications (last 5y)
    let yearBounds = { min: DEFAULT_START_YEAR, max: DEFAULT_END_YEAR };
    let authorshipData = null;

    // Focus (single author) state
    let focusedAuthorID = null;
    let focusedAuthorName = '';
    let lastSelectedPubs = []; // holds the most recent filtered publications

    // Load both CSVs, then initialize
    Promise.all([
      fetchCSV(rosterPath),
      fetchCSV(pubsPath),
      fetchCSVIfExists(authorshipsPath)])
      .then(([rosterCSV, pubsCSV, authCSV]) => {
      rosterData = parseCSV(rosterCSV);
      pubData = parseCSV(pubsCSV);
      authorshipData = authCSV ? parseCSV(authCSV) : [];

      normalizeRoster();
      normalizePubs();
      // Optional: normalize authorship, if you want to coerce types/columns fancily
      // (Not strictly necessary; we normalize at use-time.)

     // Ensure the network can draw immediately on first render:
      ensureNetworkPanel();     // NEW: create the panel (no-op if it exists)
      initFilters();
      initYearInputs();
      bindEvents();
      update();                 // IMPORTANT: forces initial render (fixes “needs a filter change”)
      }).catch(err => console.error('Failed to load CSVs', err));


    // ============ Core helpers ============
    function fetchCSVIfExists(path){
      return fetch(path).then(r => r.ok ? r.text() : null).catch(() => null);
      }
    
    function toInt(x) {
      const n = Number(x);
      return Number.isFinite(n) ? Math.round(n) : 0;
    }
    function toFloat(x) {
      const n = Number(x);
      return Number.isFinite(n) ? n : NaN;
    }
    function clampYear(y){
      y = toInt(y);
      if (!y) return DEFAULT_START_YEAR; // avoid 0 showing up
      if (y < DEFAULT_START_YEAR) return DEFAULT_START_YEAR;
      if (y > DEFAULT_END_YEAR) return DEFAULT_END_YEAR;
      return y;
    }
    function normalizeID(id) {
      // Accept raw OpenAlex IDs or full URLs
      return String(id || '')
        .replace(/^https?:\/\/openalex\.org\/authors\//i, '')
        .replace(/^https?:\/\/openalex\.org\//i, '')
        .trim();
    }
    function fetchCSV(path) { return fetch(path).then(resp => resp.text()); }

    // CSV parser that handles quoted commas and quotes
    function parseCSV(text) {
      const lines = text.replace(/\r/g, '').split('\n').filter(Boolean);
      if (!lines.length) return [];
      const headers = splitCSVLine(lines.shift());
      return lines.map(line => {
        const values = splitCSVLine(line);
        const row = {};
        headers.forEach((h, i) => {
          const key = h.trim();
          let v = values[i] !== undefined ? values[i] : '';
          // Strip surrounding quotes if present
          v = v.replace(/^"|"$/g, '').replace(/""/g, '"');
          row[key] = v;
        });
        return row;
      });
    }
    function splitCSVLine(line) {
      const out = [];
      let cur = '';
      let inQuotes = false;
      for (let i=0;i<line.length;i++){
        const ch = line[i];
        if (ch === '"'){
          if (inQuotes && line[i+1] === '"'){ cur += '"'; i++; }
          else { inQuotes = !inQuotes; }
        } else if (ch === ',' && !inQuotes) {
          out.push(cur);
          cur = '';
        } else cur += ch;
      }
      out.push(cur);
      return out;
    }

    // ============ Normalization ============
    function normalizeRoster(){
      // Normalize OpenAlexID, collect research groups and numeric metrics
      rosterData.forEach(r => {
        r.OpenAlexID = normalizeID(r.OpenAlexID);
        r.Name = r.Name || '';
        // combine RG1..RG4 into a single array for filtering
        r._RGs = ['RG1','RG2','RG3','RG4']
          .map(k => (r[k] || '').trim())
          .filter(v => v);
        r.H_index = toInt(r.H_index);
        r.I10_index = toInt(r.I10_index);
        r.Works_count = toInt(r.Works_count);
        r.Total_citations = toInt(r.Total_citations);
      });
    }

    function normalizePubs(){
      pubData.forEach(p => {
        // numeric
        p.publication_year = clampYear(p.publication_year);
        p.cited_by_count = toInt(p.cited_by_count);

        // author ID (already provided as 'author_openalex_id' per your CSV)
        p.author_openalex_id = normalizeID(p.author_openalex_id);

        // DOI: prefer full URL if present; otherwise leave empty
        if (p.doi && !/^https?:\/\//i.test(p.doi)) {
          // Some CSVs store DOIs like "10.1234/abcd"; we can link via dx.doi.org
          p.doi = 'https://doi.org/' + p.doi;
        }

        // FWCI (accept 'fwci' case from your CSV)
        p._fwci = toFloat(p.fwci);

        // Pre-compute a lowercase haystack for topic search:
        // concepts_list + subfield + primary topic + title (display_name)
        const c = [
          p.concepts_list || '',
          p.primary_topic__subfield__display_name || '',
          p.primary_topic__display_name || '',
          p.display_name || ''
        ].join(' ').toLowerCase();
        p._topic_haystack = normalizeText(c);
      });
    }

    // ============ UI: filters, years, events ============
    function initFilters(){
      // Populate multiselects for Level, Category, Appointment, Research Group
      const levelSel = document.getElementById('level');
      const catSel = document.getElementById('category');
      const apptSel = document.getElementById('appointment');
      const rgSel = document.getElementById('research-group');

      fillSelect(levelSel, uniqueNonEmpty(rosterData.map(r => r.Level || '')));
      fillSelect(catSel, uniqueNonEmpty(rosterData.map(r => r.Category || '')));
      fillSelect(apptSel, uniqueNonEmpty(rosterData.map(r => r.Appointment || '')));

      // Default Appointment selection: ONLY Full‑time selected
      setDefaultAppointmentSelection();

      // Research groups across RG1..RG4
      const allRGs = new Set();
      rosterData.forEach(r => r._RGs.forEach(g => allRGs.add(g)));
      fillSelect(rgSel, Array.from(allRGs).sort());
    }

    // Select only "Full‑time" in the Appointment multi-select, deselect others.
    // Tolerant to case and optional hyphen/space (e.g., Full time, Full-time).
    function setDefaultAppointmentSelection(){
      const sel = document.getElementById('appointment');
      if (!sel) return;
      const isFullTime = (s) => /full\s*-?\s*time/i.test(String(s || ''));
      for (const opt of sel.options) {
        opt.selected = isFullTime(opt.value) || isFullTime(opt.text);
      }
    }

    function fillSelect(sel, options){
      sel.innerHTML = '';
      options.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        sel.appendChild(opt);
      });
    }

    function initYearInputs(){
      const ymin = document.getElementById('year-min');
      const ymax = document.getElementById('year-max');
      ymin.value = DEFAULT_START_YEAR;
      ymax.value = DEFAULT_END_YEAR;
      yearBounds = { min: DEFAULT_START_YEAR, max: DEFAULT_END_YEAR };
    }

    function bindEvents(){
      // Multi-select changes
      document.querySelectorAll('#filters select').forEach(sel => sel.addEventListener('change', update));

      // Per-select Clear buttons
      document.querySelectorAll('.clear-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          const id = btn.getAttribute('data-target');
          const el = document.getElementById(id);
          Array.from(el.options).forEach(o => { o.selected = false; });
          if (id === 'appointment') setDefaultAppointmentSelection();
        update();
        });
      });

        // Export button: wire up click + set initial count
      const exportBtn = document.getElementById('export-selection');
        if (exportBtn) {
          exportBtn.addEventListener('click', () => exportCurrentSelectionCSV(lastSelectedPubs));
          setExportButtonCount(0);
        }

      });

    

      // Year inputs
      document.getElementById('year-min').addEventListener('input', update);
      document.getElementById('year-max').addEventListener('input', update);

      // Topic search (concepts + subfields + primary topic + title)
      const topic = document.getElementById('topic-search');
      if (topic) topic.addEventListener('input', debounce(update, 200));

      // Global reset
      document.getElementById('reset-filters').addEventListener('click', () => {
        document.querySelectorAll('#filters select').forEach(sel => {
          Array.from(sel.options).forEach(o => o.selected = false);
        });
        // Restore default: only Full‑time selected
        setDefaultAppointmentSelection();

        focusedAuthorID = null;
        focusedAuthorName = '';
        initYearInputs();
        if (topic) topic.value = '';
        update();
      });
    }

    // Export current selection as CSV
    const exportBtn = document.getElementById('export-selection');
    if (exportBtn) {
        exportBtn.addEventListener('click', () => {
          exportCurrentSelectionCSV(lastSelectedPubs);
        });
      }
    // ============ Filtering logic ============
    function applyFilters(){
      // Years
      const ymin = clampYear(document.getElementById('year-min').value);
      const ymax = clampYear(document.getElementById('year-max').value);
      yearBounds = { min: Math.min(ymin, ymax), max: Math.max(ymin, ymax) };

      // Topic query
      const qEl = document.getElementById('topic-search');
      const topicQ = (qEl && qEl.value) ? qEl.value.trim().toLowerCase() : '';

      // Roster attribute filters (ignored if focusing on one author)
      const selectedLevels = getMulti('level');
      const selectedCats = getMulti('category');
      const selectedAppt = getMulti('appointment');
      const selectedRGs = getMulti('research-group');

      // Filter pubs by year
      let pubs = pubData.filter(p => p.publication_year >= yearBounds.min && p.publication_year <= yearBounds.max);

      // Topic filter (robust token logic)
      if (topicQ) {
        pubs = pubs.filter(p => fuzzyQueryMatch(topicQ, p._topic_haystack));
      }

      let contributingRoster;
      if (focusedAuthorID) {
        // Focus: only pubs of that author
        const fa = normalizeID(focusedAuthorID);
        pubs = pubs.filter(p => normalizeID(p.author_openalex_id) === fa);
        // Roster: just that one entry, if present
        contributingRoster = rosterData.filter(r => normalizeID(r.OpenAlexID) === fa);
      } else {
        // Apply roster attribute filters to roster first
        const filteredRoster = rosterData.filter(r => {
          if (selectedLevels.length && !selectedLevels.includes(r.Level || '')) return false;
          if (selectedCats.length && !selectedCats.includes(r.Category || '')) return false;
          if (selectedAppt.length && !selectedAppt.includes(r.Appointment || '')) return false;
          if (selectedRGs.length && !r._RGs.some(g => selectedRGs.includes(g))) return false;
          return true;
        });

        // Then limit pubs to those whose author is in filteredRoster
        const allowedIDs = new Set(filteredRoster.map(r => normalizeID(r.OpenAlexID)));
        pubs = pubs.filter(p => allowedIDs.has(normalizeID(p.author_openalex_id)));

        // Contributing roster = filtered roster members who actually have pubs after all filters
        const havePubIDs = new Set(pubs.map(p => normalizeID(p.author_openalex_id)));
        contributingRoster = filteredRoster.filter(r => havePubIDs.has(normalizeID(r.OpenAlexID)));
      }

      return { contributingRoster, selectedPubs: pubs };
    }

    function getMulti(id){
      const el = document.getElementById(id);
      return Array.from(el.selectedOptions).map(o => o.value);
    }

    // ============ Rendering ============
    function update(){
      const { contributingRoster, selectedPubs } = applyFilters();
      lastSelectedPubs = selectedPubs.slice();  // shallow copy
      setExportButtonCount(selectedPubs.length);
      drawBarChart(selectedPubs);
      drawFacultyTable(contributingRoster);
      drawPublicationList(selectedPubs);
      ensureNetworkPanel();                                  // create panel once if missing
      updateCoauthorPanels(contributingRoster, selectedPubs); // network + pairs table
      const fc = document.getElementById('faculty-count');
      if (fc) {
        const base = `Faculty contributing: ${contributingRoster.length}`;
        fc.textContent = focusedAuthorID ? `${base} (Focused: ${focusedAuthorName}. Use Reset to clear)` : base;
      }
    }

    function drawBarChart(pubs){
      // Count by year x type
      const counts = new Map(); // key: `${year}::${type}` -> count
      const years = new Set();
      const types = new Set();

      pubs.forEach(p => {
        const y = p.publication_year;
        const t = (p.type || 'other').toLowerCase();
        years.add(y); types.add(t);
        const k = `${y}::${t}`;
        counts.set(k, (counts.get(k) || 0) + 1);
      });

      const sortedYears = Array.from(years).sort((a,b)=>a-b);
      const sortedTypes = Array.from(types).sort();

      // Build Plotly series (stacked bars)
      const traces = sortedTypes.map(t => {
        const yvals = sortedYears.map(y => counts.get(`${y}::${t}`) || 0);
        return {
          x: sortedYears,
          y: yvals,
          name: t,
          type: 'bar'
        };
      });

      const layout = {
        barmode: 'stack',
        xaxis: { title: 'Year', dtick: 1, range: [yearBounds.min - 0.5, yearBounds.max + 0.5] },
        yaxis: { title: 'Publications' },
        margin: { t: 20, r: 10, b: 40, l: 50 },
        height: 300
      };

      Plotly.react('pub-chart', traces, layout, {displayModeBar:false});
    }

    function drawFacultyTable(faculty){
      const body = document.querySelector('#faculty-table tbody');
      body.innerHTML = '';
      // Sort by H-index desc
      faculty.sort((a,b) => b.H_index - a.H_index);
      faculty.forEach(f => {
        const openAlexId = String(f.OpenAlexID || '').toLowerCase();
        const openAlexURL = `https://openalex.org/authors/${openAlexId}`;
        const row = `<tr>
          <td>
            <a href="#" class="author-link" data-id="${escapeHTML(f.OpenAlexID)}" data-name="${escapeHTML(f.Name)}">${escapeHTML(f.Name)}</a>
            &nbsp;·&nbsp;
            <a href="${openAlexURL}" target="_blank" rel="noopener">OpenAlex profile</a>
          </td>
          <td>${toInt(f.H_index)}</td>
          <td>${toInt(f.I10_index)}</td>
          <td>${toInt(f.Works_count)}</td>
          <td>${toInt(f.Total_citations)}</td>
        </tr>`;
        body.insertAdjacentHTML('beforeend', row);
      });

      // Click to focus on one author (local link)
      body.querySelectorAll('.author-link').forEach(a => {
        a.addEventListener('click', (e) => {
          e.preventDefault();
          focusedAuthorID = a.getAttribute('data-id');
          focusedAuthorName = a.getAttribute('data-name');
          update();
        });
      });
    }

function drawPublicationList(pubs) {
  const ul = document.getElementById('publications-list');
  if (!ul) return;

  ul.innerHTML = '';

  // Guard: no data or empty after filtering
  if (!Array.isArray(pubs) || pubs.length === 0) {
    const li = document.createElement('li');
    li.className = 'muted';
    li.textContent = 'No publications match the current filters.';
    ul.appendChild(li);
    // Optional: update a visible counter if you have one
    const countEl = document.getElementById('publications-count');
    if (countEl) countEl.textContent = '0';
    return;
  }

  // Sort newest first, then by citations (desc)
  pubs.sort((a, b) =>
    (b.publication_year - a.publication_year) ||
    (b.cited_by_count - a.cited_by_count)
  );

  const frag = document.createDocumentFragment();

  pubs.forEach(p => {
    const year = toInt(p.publication_year);
    const safeTitle = allowItalicsOnly(p.display_name || '');
    const type = escapeHTML(p.type || '');

    // Normalize DOI field: expect full URL; otherwise omit
    const doi = (p.doi && /^https?:\/\//i.test(p.doi)) ? p.doi : '';

    // Normalize FWCI to a finite number if present on record (e.g., as string)
    let fwci = (p && p._fwci != null) ? Number(p._fwci) : NaN;
    fwci = Number.isFinite(fwci) ? fwci : NaN;

    const li = document.createElement('li');

    // Main line + meta chips
    li.innerHTML = `
      <div><strong>${year}</strong> — <em>${safeTitle}</em> <span class="muted">(${type})</span></div>
      <div class="pub-meta">
        <span class="chip"><span class="mono">Citations:</span> ${toInt(p.cited_by_count)}</span>
        ${Number.isFinite(fwci) ? `<span class="chip secondary"><span class="mono">FWCI:</span> ${fwci.toFixed(2)}</span>` : ''}
        ${doi ? `<a class="chip" href="${doi}" target="_blank" rel="noopener">DOI</a>` : ''}
      </div>
    `;

    frag.appendChild(li);
  });

  ul.appendChild(frag);

  // Optional: update a visible counter if your HTML has one
  const countEl = document.getElementById('publications-count');
  if (countEl) countEl.textContent = String(pubs.length);
}


    // ============ Text helpers ============
    function escapeHTML(s){
      return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    // Allow <i> and </i> only (for italic species names)
    function allowItalicsOnly(s){
      const escaped = escapeHTML(String(s||''));
      // Restore *only* <i> and </i> tags if they existed literally
      // (assuming input titles may include <i>...</i>)
      return escaped
        .replace(/&lt;i&gt;/g, '<i>')
        .replace(/&lt;\/i&gt;/g, '</i>');
    }

  function stripDiacritics(s){ return String(s||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,''); }

function canonName(raw){
  let s = stripDiacritics(raw).toLowerCase().trim();
  const m = s.match(/^([^,]+),\s*(.+)$/); if (m) s = `${m[2]} ${m[1]}`;
  s = s.replace(/[.'’-]/g, ' ').replace(/[^a-z\s]/g,' ').replace(/\s+/g,' ').trim();
  const parts = s.split(' ').filter(Boolean);
  if (parts.length === 1) return parts[0];
  const suff = new Set(['jr','sr','iii','ii']); while (parts.length>1 && suff.has(parts.at(-1))) parts.pop();
  const particles = new Set(['de','del','della','der','den','van','von','da','di','dos','la','le','mac','mc','bin','al','ibn','st','st.']);
  const first = parts[0]; let last = parts.at(-1); const pen = parts.at(-2);
  if (parts.length>=3 && particles.has(pen)) last = `${pen} ${last}`;
  return `${first[0]} ${last}`;
}

function splitAuthorsList(s){ return s ? String(s).split(/\s*;\s*/).map(t=>t.trim()).filter(Boolean) : []; }

    
// ============ Fuzzy search helpers (STRICT) ============
// Goal: high precision. No prefix or edit-distance fuzziness.
// Match = exact token equality after conservative stemming.

function normalizeText(t) {
  if (t == null) return "";
  // Unicode normalize, strip diacritics, lowercase, collapse punctuation/whitespace
  let s = String(t)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")  // remove combining marks
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")      // map non-alphanumerics to spaces
    .replace(/\s+/g, " ")             // collapse spaces
    .trim();
  return s;
}

// Conservative stemmer: plurals and common verb endings only.
// Intentionally avoids aggressive derivational stemming to preserve meaning.
function stem(w) {
  if (!w || w.length <= 3) return w;  // don't stem very short tokens

  let s = w;

  // plural -> singular
  if (s.endsWith("sses")) {
    s = s.slice(0, -2);                                // "classes" -> "class"
  } else if (s.endsWith("ies") && s.length > 4) {
    s = s.slice(0, -3) + "y";                          // "studies" -> "study"
  } else if (s.endsWith("s") && !s.endsWith("ss") && s.length > 3) {
    s = s.slice(0, -1);                                // "dogs" -> "dog"
  }

  // past/gerund
  if (s.endsWith("ing") && s.length > 5) {
    s = s.slice(0, -3);                                // "running" -> "runn"
    if (s.length > 3 && s[s.length - 1] === s[s.length - 2]) {
      s = s.slice(0, -1);                              // "runn" -> "run"
    }
  } else if (s.endsWith("ed") && s.length > 4) {
    s = s.slice(0, -2);                                // "jogged" -> "jogg"
    if (s.length > 3 && s[s.length - 1] === s[s.length - 2]) {
      s = s.slice(0, -1);                              // "jogg" -> "jog"
    }
  }

  return s;
}

function tokenize(text) {
  // Normalize -> split -> light stem -> deduplicate
  const base = normalizeText(text).split(/\s+/).filter(Boolean).map(stem);
  return Array.from(new Set(base));
}

// STRICT token match: exact equality only after stemming
function strongTokenMatch(qt, tt) {
  return tt === qt;
}

// AND semantics across query tokens; every query token must match some text token.
function fuzzyQueryMatch(query, text) {
  const qTokens = tokenize(query);
  if (!qTokens.length) return true;

  const tTokens = tokenize(text);

  // Precision guardrails:
  // - 1–2 char tokens: must match exactly (already enforced by equality)
  // - 3+ char tokens: equality after stemming only; no prefixes, no edit distance
  return qTokens.every(qt => tTokens.some(tt => strongTokenMatch(qt, tt)));
}

// ========== End Fuzzy search helpers ==========

function exportCurrentSelectionCSV(pubs) {
  // Desired column order and their source fields in your CSV rows
  // Your CSV already includes these columns (confirmed): 
  // publication_year, display_name, authors, host_venue__display_name, id, doi, fwci, cited_by_count, type, institutions, concepts_list
  const headers = [
    'publication_year',
    'title',
    'authors',
    'journal',
    'id',
    'DOI',
    'FWCI',
    'citations',
    'type',
    'institutions',
    'concepts_list'
  ];

  // map a pub row to the exact ordered values
  const rows = pubs.map(p => ([
    safeCsv(p.publication_year),
    safeCsv(p.display_name),                    // title
    safeCsv(p.authors),
    safeCsv(p.host_venue__display_name),        // journal
    safeCsv(p.id),
    safeCsv(p.doi),
    safeCsv((Number.isFinite(p._fwci) ? p._fwci : p.fwci)), // FWCI (use parsed _fwci if present)
    safeCsv(p.cited_by_count),                  // citations
    safeCsv(p.type),
    safeCsv(p.institutions),
    safeCsv(p.concepts_list)
  ]));

  // Build CSV (RFC4180-ish): quote fields, double internal quotes
  const headerLine = headers.map(csvEscape).join(',');
  const bodyLines = rows.map(r => r.map(csvEscape).join(',')).join('\n');
  const csv = headerLine + '\n' + bodyLines + '\n';

  // Download (with UTF-8 BOM so Excel opens cleanly)
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'publications_selection.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function csvEscape(v) {
  const s = String(v == null ? '' : v);
  // Escape CR/LF to keep rows intact; many CSV readers handle raw newlines but this is safer
  const cleaned = s.replace(/\r\n/g, ' ').replace(/\n/g, ' ').replace(/\r/g, ' ');
  // If field contains comma, quote, or leading/trailing space, wrap in quotes and double quotes inside
  if (/[",\s]/.test(cleaned[0] || '') || /[",\s]/.test(cleaned.slice(-1)) || /[",\n]/.test(cleaned)) {
    return `"${cleaned.replace(/"/g, '""')}"`;
  }
  return cleaned;
}

function safeCsv(v) {
  if (v == null) return '';
  // prefer numbers as-is; everything else to string
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return String(v);
}

function setExportButtonCount(n) {
  const btn = document.getElementById('export-selection');
  if (!btn) return;
  btn.textContent = `Export current selection (n=${n})`;
  btn.disabled = (n === 0);                   // optional UX: disable when empty
  btn.classList.toggle('is-disabled', n === 0);
}

// ==================== Co-authorship Network & Pairs Table ====================

// Ensure a panel exists (works without editing HTML)
function ensureNetworkPanel(){
  if (document.getElementById('network-panel')) return;

  // Insert after the Faculty+Publications section if present; else append to body
  const anchor = document.getElementById('faculty-publications') || document.body;
  const panel = document.createElement('section');
  panel.id = 'network-panel';
  panel.className = 'card';
  panel.innerHTML = `
    <div class="panel-head">
      <h2>Co-authorship network (filtered selection)</h2>
      <div id="network-meta" class="count"></div>
    </div>
    <div id="coauthor-network" class="chart"></div>
    <div id="pair-detail" class="muted" style="margin-top:8px;"></div>
    <div class="split-2">
      <section>
        <h3 style="margin-top:16px;">Author pairs</h3>
        <table id="coauthor-table">
          <thead><tr><th>Author A</th><th>Author B</th><th># joint pubs</th></tr></thead>
          <tbody></tbody>
        </table>
      </section>
    </div>
  `;
  anchor.insertAdjacentElement('afterend', panel); 
}

// Main updater
function updateCoauthorPanels(contributingRoster, selectedPubs){
  const graph = computeCoauthorGraph(contributingRoster, selectedPubs);
  drawCoauthorNetwork(graph);
  drawCoauthorPairsTable(graph);
  const meta = document.getElementById('network-meta');
  if (meta) {
    meta.textContent = `${graph.nodes.length} researchers · ${graph.edges.length} links`;
  }
}

// REPLACE your existing computeCoauthorGraph with this version
function computeCoauthorGraph(contributingRoster, selectedPubs){
  const idNorm = s => String(s||'').trim()
    .replace(/^https?:\/\/openalex\.org\/authors\//i,'')
    .replace(/^https?:\/\/openalex\.org\//i,'');
  const workNorm = s => String(s||'').trim()
    .replace(/^https?:\/\/openalex\.org\/works\//i,'')
    .replace(/^https?:\/\/openalex\.org\//i,'');

  // roster id -> display name
  const nameOf = new Map(contributingRoster.map(r => [idNorm(r.OpenAlexID), r.Name || r.OpenAlexID]));

  // Respect all active filters by limiting to the works in the current (dedup) selection
  const selectedWorkIDs = new Set(
    selectedPubs.map(p => workNorm(p.id || p.work_id || '')).filter(Boolean)
  );

  // === Preferred path: expand authorships from PRE-dedup ===
  if (Array.isArray(authorshipData) && authorshipData.length) {
    // Build work -> Set(roster_author_ids) for the selected works only
    const byWork = new Map();
    for (const row of authorshipData) {
      const wid = workNorm(row.id || row.work_id || '');
      if (!selectedWorkIDs.has(wid)) continue;     // only current selection
      const aid = idNorm(row.author_openalex_id);
      if (!nameOf.has(aid)) continue;              // only cohort authors
      if (!byWork.has(wid)) byWork.set(wid, new Set());
      byWork.get(wid).add(aid);
    }
    return buildGraphFromPairs(byWork, nameOf, selectedPubs);
  }

  // === Fallback: derive from semicolon-joined author strings in the dedup rows ===
  const rosterByCanon = new Map(); // canon -> [{id, name}]
  for (const r of contributingRoster) {
    const cid = idNorm(r.OpenAlexID);
    const cname = canonName(r.Name || '');
    if (!rosterByCanon.has(cname)) rosterByCanon.set(cname, []);
    rosterByCanon.get(cname).push({ id: cid, name: r.Name || cid });
  }

  const byWork = new Map();
  for (const p of selectedPubs) {
    const wid = workNorm(p.id || p.work_id || '');
    if (!wid) continue;
    const names = splitAuthorsList(p.authors);
    const idsOnWork = new Set();
    for (const n of names) {
      const hits = rosterByCanon.get(canonName(n));
      if (hits) for (const h of hits) idsOnWork.add(h.id);
    }
    if (idsOnWork.size >= 2) byWork.set(wid, idsOnWork);
  }

  return buildGraphFromPairs(byWork, nameOf, selectedPubs);

  // ---- helper: convert work->authors into nodes/edges, deduping by work id
  function buildGraphFromPairs(byWork, nameOf, selectedPubs){
    // representative pub row per work (for edge click lists)
    const widToPub = new Map();
    const widOf = p => workNorm(p?.id || p?.work_id || '');
    for (const p of selectedPubs) { const w = widOf(p); if (w && !widToPub.has(w)) widToPub.set(w, p); }

    const pairCounts = new Map(); // "a|b" -> count
    const pairPubs   = new Map(); // "a|b" -> [pubRows]

    for (const [wid, set] of byWork.entries()){
      const ids = Array.from(set).sort();
      for (let i=0;i<ids.length;i++){
        for (let j=i+1;j<ids.length;j++){
          const a=ids[i], b=ids[j], key=`${a}|${b}`;
          pairCounts.set(key, (pairCounts.get(key)||0) + 1);
          if (!pairPubs.has(key)) pairPubs.set(key, []);
          pairPubs.get(key).push(widToPub.get(wid)); // safe: may hold a row from selectedPubs
        }
      }
    }

    // nodes
    const nodeIDs = new Set();
    for (const key of pairCounts.keys()){ const [a,b]=key.split('|'); nodeIDs.add(a); nodeIDs.add(b); }
    const nodes = Array.from(nodeIDs).map(id => ({ id, name: nameOf.get(id) || id }));

    // edges (dedup the pub list by work id)
    const idxOf = new Map(nodes.map((n,i)=>[n.id,i]));
    const edges = [];
    for (const [key, count] of pairCounts.entries()){
      const [a,b] = key.split('|');
      if (!idxOf.has(a) || !idxOf.has(b)) continue;
      const seen = new Set(); const uniqPubs = [];
      for (const p of (pairPubs.get(key) || [])) {
        const wid = widOf(p); if (wid && !seen.has(wid)) { seen.add(wid); uniqPubs.push(p); }
      }
      edges.push({ a, b, ai: idxOf.get(a), bi: idxOf.get(b), count, pubs: uniqPubs });
    }

    // simple circular layout + degree for size
    const N = nodes.length, R = 1.0;
    nodes.forEach((n,i)=>{ const t=(i/Math.max(1,N))*2*Math.PI; n.x=R*Math.cos(t); n.y=R*Math.sin(t); });
    const degree = new Map(nodes.map(n => [n.id, 0]));
    edges.forEach(e => { degree.set(e.a,(degree.get(e.a)||0)+e.count); degree.set(e.b,(degree.get(e.b)||0)+e.count); });
    nodes.forEach(n => n.deg = degree.get(n.id) || 0);

    return { nodes, edges };
  }
}



function drawCoauthorNetwork(graph){
  const el = document.getElementById('coauthor-network');
  if (!el) return;

  const xs = graph.nodes.map(n => n.x);
  const ys = graph.nodes.map(n => n.y);
  const labels = graph.nodes.map(n => n.name);
  const degs = graph.nodes.map(n => n.deg);
  const minDeg = Math.min(...degs, 0);
  const maxDeg = Math.max(...degs, 1);

  // Node size scale: 10–28 px
  const size = degs.map(d => {
    const t = (d - minDeg) / (maxDeg - minDeg || 1);
    return 10 + t * 18;
  });

  // Build edge traces with variable thickness
  const edgeLineTraces = [];
  const edgeClickTargetsX = [];
  const edgeClickTargetsY = [];
  const edgeClickTargetsCustom = [];

  let minW = Infinity, maxW = 0;
  graph.edges.forEach(e => { minW = Math.min(minW, e.count); maxW = Math.max(maxW, e.count); });
  const lineWidth = (c) => {
    if (!isFinite(c)) return 1;
    if (minW === maxW) return 4;      // uniform
    const t = (c - minW) / (maxW - minW);
    return 1 + t * 8;                 // 1–9 px
  };

  graph.edges.forEach(e => {
    const x0 = graph.nodes[e.ai].x, y0 = graph.nodes[e.ai].y;
    const x1 = graph.nodes[e.bi].x, y1 = graph.nodes[e.bi].y;

    edgeLineTraces.push({
      type: 'scatter',
      mode: 'lines',
      x: [x0, x1],
      y: [y0, y1],
      hoverinfo: 'skip',
      line: { width: lineWidth(e.count), color: 'rgba(100,116,139,0.6)' },
      showlegend: false
    });


    // Invisible midpoint markers for click detection
    const mx = (x0 + x1)/2, my = (y0 + y1)/2;
    edgeClickTargetsX.push(mx);
    edgeClickTargetsY.push(my);
    edgeClickTargetsCustom.push(`${e.a}|${e.b}`);
  });

  // Node trace
  const nodeTrace = {
  type: 'scatter',
  mode: 'markers+text',
  x: xs, y: ys,
  text: labels,
  textposition: 'top center',
  hoverinfo: 'text',                       // only node hover shows
  // optionally: hovertext: yourPrecomputedAdjacencyText,
  marker: { size: size, line: { width: 1, color: '#fff' } },
  name: 'authors'
};

const layout = {
  xaxis: { visible: false },
  yaxis: { visible: false },
  margin: { t: 10, r: 10, b: 10, l: 10 },
  height: 520,
  hovermode: 'closest',                    // <— IMPORTANT
  plot_bgcolor: 'rgba(0,0,0,0)',
  paper_bgcolor: 'rgba(0,0,0,0)'
};


  // Build pair index for click → publications
  const pairIndex = {};
  graph.edges.forEach(e => { pairIndex[`${e.a}|${e.b}`] = e; });
  el.__pairIndex = pairIndex;

  // Invisible midpoint markers for click detection
  const edgeClickTrace = {
    type: 'scatter',
    mode: 'markers',
    x: edgeClickTargetsX,
    y: edgeClickTargetsY,
    customdata: edgeClickTargetsCustom,
    marker: { size: 12, opacity: 0.005 },
    name: 'edge-click-targets',
    hoverinfo: 'skip',
    showlegend: false
  };

  
  // drawCoauthorNetwork(...)
Plotly.react(el, [...edgeLineTraces, edgeClickTrace, nodeTrace], layout, { displayModeBar: false });

// Attach once, now that Plotly has initialized the element
if (!el.__clickBound && typeof el.on === 'function') {
  el.on('plotly_click', (ev) => {
    const pt = ev?.points?.[0];
    if (!pt) return;
    const trace = ev.event?.target?.__data?.[pt.curveNumber];
    const isEdgeClickTargets = trace && trace.name === 'edge-click-targets';
    const pairKey = pt?.customdata;
    if (isEdgeClickTargets && pairKey) {
      const ctx = el.__pairIndex || {};
      const rec = ctx[pairKey];
      if (rec) showPairPublications(rec.a, rec.b, rec.pubs);
    }
  });
  el.__clickBound = true;
}

  
}

function drawCoauthorPairsTable(graph){
  const body = document.querySelector('#coauthor-table tbody');
  if (!body) return;
  body.innerHTML = '';

  // Sort pairs by count desc, then alpha
  const rows = graph.edges
    .map(e => ({ a: graph.nodes[e.ai].name, b: graph.nodes[e.bi].name, key: `${e.a}|${e.b}`, count: e.count, pubs: e.pubs }))
    .sort((r1, r2) => (r2.count - r1.count) || (r1.a.localeCompare(r2.a)) || (r1.b.localeCompare(r2.b)));

  const frag = document.createDocumentFragment();
  rows.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${escapeHTML(r.a)}</td><td>${escapeHTML(r.b)}</td><td>${r.count}</td>`;
    tr.addEventListener('click', () => {
      showPairPublications(r.key.split('|')[0], r.key.split('|')[1], r.pubs);
      // Optional: scroll into view
      document.getElementById('pair-detail')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });
    frag.appendChild(tr);
  });
  body.appendChild(frag);
}

function showPairPublications(aID, bID, pubs){
  const box = document.getElementById('pair-detail');
  if (!box) return;

  // Name lookup via rosterData
  const norm = (s)=> String(s||'').replace(/^https?:\/\/openalex\.org\/authors\//i,'').replace(/^https?:\/\/openalex\.org\//i,'');
  const nameOf = new Map(rosterData.map(r => [norm(r.OpenAlexID), r.Name || r.OpenAlexID]));
  const nameA = nameOf.get(norm(aID)) || aID;
  const nameB = nameOf.get(norm(bID)) || bID;

  if (!pubs || !pubs.length) {
    box.innerHTML = `<div class="muted">No publications found for ${escapeHTML(nameA)} and ${escapeHTML(nameB)} in the current selection.</div>`;
    return;
  }

  // Dedup by work id again (safety)
  const widOf = (p) => String(p?.id || p?.work_id || '').replace(/^https?:\/\/openalex\.org\/works\//i,'').replace(/^https?:\/\/openalex\.org\//i,'');
  const seen = new Set();
  const list = [];
  pubs.forEach(p => {
    const wid = widOf(p);
    if (!wid || seen.has(wid)) return;
    seen.add(wid);
    list.push(p);
  });

  const items = list.map(p => {
    const year = toInt(p.publication_year);
    const title = allowItalicsOnly(p.display_name || '');
    const doi = (p.doi && /^https?:\/\//i.test(p.doi)) ? `<a href="${p.doi}" target="_blank" rel="noopener">DOI</a>` : '';
    const idLink = p.id ? `<a href="${p.id}" target="_blank" rel="noopener">OpenAlex</a>` : '';
    const journal = escapeHTML(p.host_venue__display_name || '');
    return `<li><strong>${year}</strong> — <em>${title}</em> <span class="muted">(${journal})</span> ${doi ? '· '+doi : ''} ${idLink ? '· '+idLink : ''}</li>`;
  }).join('');

  box.innerHTML = `
    <div class="chip">Papers co-authored by <strong>${escapeHTML(nameA)}</strong> and <strong>${escapeHTML(nameB)}</strong> (n=${list.length})</div>
    <ul class="pair-pubs">${items}</ul>
  `;
}

    
    // ============ Utilities ============
    function uniqueNonEmpty(arr){
      return Array.from(new Set(arr.filter(v => v && String(v).trim() !== ''))).sort();
    }

    function debounce(fn, ms){
      let t=null;
      return (...args) => {
        clearTimeout(t);
        t = setTimeout(() => fn.apply(null, args), ms);
      };
    }
  });
})();
