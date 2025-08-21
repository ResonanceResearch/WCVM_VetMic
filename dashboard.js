// dashboard.js — clean full version with year clamp, fuzzy search, and author focus
(function(){
  const DEFAULT_START_YEAR = 2021;
  const DEFAULT_END_YEAR = 2025;

  document.addEventListener('DOMContentLoaded', () => {
    const rosterPath = 'data/roster_with_metrics.csv';
    const pubsPath = 'data/openalex_all_authors_last5y_key_fields_dedup.csv';

    let rosterData = [];
    let pubData = [];
    let yearBounds = { min: DEFAULT_START_YEAR, max: DEFAULT_END_YEAR };
    let focusedAuthorID = null;
    let focusedAuthorName = '';

    Promise.all([fetchCSV(rosterPath), fetchCSV(pubsPath)])
      .then(([roster, pubs]) => {
        rosterData = parseCSV(roster);
        pubData = parseCSV(pubs);

        // Normalize per-row fields
        pubData.forEach(p => {
          p.author_openalex_id = normalizeID(p.author_openalex_id || p.author_name || '');
          p.publication_year = toInt(p.publication_year);
          p.cited_by_count = toInt(p.cited_by_count);
        });
        rosterData.forEach(r => {
          r.OpenAlexID = normalizeID(r.OpenAlexID);
          r.H_index = toInt(r.H_index);
          r.I10_index = toInt(r.I10_index);
          r.Works_count = toInt(r.Works_count);
          r.Total_citations = toInt(r.Total_citations);
        });

        // Always initialize to the fixed last-5-year window
        initYearInputs(yearBounds);

        populateFilters(rosterData);
        wireMultiSelectToggle();
        wireFilterEvents();

        update();
      })
      .catch(err => console.error('Failed to load CSVs', err));

    // ============ Core helpers ============
    function toInt(x) {
      const n = Number(x);
      return Number.isFinite(n) ? Math.round(n) : 0;
    }
    function clampYear(y){
      y = toInt(y);
      if (!y) return DEFAULT_START_YEAR; // defensively avoid 0
      if (y < DEFAULT_START_YEAR) return DEFAULT_START_YEAR;
      if (y > DEFAULT_END_YEAR) return DEFAULT_END_YEAR;
      return y;
    }
    function normalizeID(id) {
      return String(id || '').replace(/^https?:\/\/openalex\.org\//, '').trim();
    }
    function fetchCSV(path) { return fetch(path).then(resp => resp.text()); }

    // CSV parser that handles quoted commas
    function parseCSV(text) {
      const lines = text.replace(/\r/g, '').split('\n').filter(Boolean);
      const headers = splitCSVLine(lines.shift());
      return lines.map(line => {
        const values = splitCSVLine(line);
        const row = {};
        headers.forEach((h, i) => {
          const key = h.trim();
          let v = values[i] !== undefined ? values[i] : '';
          v = v.replace(/^"|"$/g, '');
          row[key] = v;
        });
        return row;
      });
    }
    function splitCSVLine(line) {
      const out = [];
      let cur = '', inQ = false;
      for (let i=0;i<line.length;i++){
        const ch = line[i];
        if (ch === '"'){
          if (inQ && line[i+1] === '"'){ cur += '"'; i++; }
          else { inQ = !inQ; }
        } else if (ch === ',' && !inQ){ out.push(cur); cur = ''; }
        else { cur += ch; }
      }
      out.push(cur);
      return out;
    }

    function populateFilters(roster){
      const levels=new Set(), categories=new Set(), appointments=new Set(), groups=new Set();
      roster.forEach(r=>{
        if (r.Level) levels.add(r.Level);
        if (r.Category) categories.add(r.Category);
        if (r.Appointment) appointments.add(r.Appointment);
        [r.RG1,r.RG2,r.RG3,r.RG4].forEach(g=> g && groups.add(g));
      });
      fillSelect('level', levels);
      fillSelect('category', categories);
      fillSelect('appointment', appointments);
      fillSelect('research-group', groups);
    }
    function fillSelect(id, options){
      const sel = document.getElementById(id);
      sel.innerHTML = '';
      Array.from(options).sort().forEach(opt => {
        const o = document.createElement('option');
        o.value = o.textContent = opt;
        sel.appendChild(o);
      });
    }

    function initYearInputs(){
      const ymin = document.getElementById('year-min');
      const ymax = document.getElementById('year-max');
      const start = DEFAULT_START_YEAR;
      const end = DEFAULT_END_YEAR;
      ymin.value = String(start);
      ymax.value = String(end);
      ymin.min = String(start); ymax.min = String(start);
      ymin.max = String(end);   ymax.max = String(end);
    }

    function wireFilterEvents(){
      // Multi-select changes
      document.querySelectorAll('#filters select').forEach(sel => sel.addEventListener('change', update));

      // Per-select Clear buttons
      document.querySelectorAll('.clear-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          const id = btn.getAttribute('data-target');
          const el = document.getElementById(id);
          Array.from(el.options).forEach(o => { o.selected = false; });
          update();
        });
      });

      // Year inputs
      document.getElementById('year-min').addEventListener('input', update);
      document.getElementById('year-max').addEventListener('input', update);

      // Topic search (combined concepts + subfields + primary topic)
      const topic = document.getElementById('topic-search');
      if (topic) topic.addEventListener('input', debounce(update, 200));

      // Global reset
      document.getElementById('reset-filters').addEventListener('click', () => {
        document.querySelectorAll('#filters select').forEach(sel => Array.from(sel.options).forEach(o => o.selected = false));
        focusedAuthorID = null; focusedAuthorName = '';
        initYearInputs();
        if (topic) topic.value = '';
        update();
      });
    }

    // Allow deselection in multi-selects without Cmd/Ctrl
    function wireMultiSelectToggle(){
      document.querySelectorAll('select.multi').forEach(sel => {
        sel.addEventListener('mousedown', (e) => {
          if (e.target.tagName.toLowerCase() === 'option'){
            e.preventDefault();
            const opt = e.target;
            opt.selected = !opt.selected;
            sel.dispatchEvent(new Event('change', { bubbles: true }));
          }
        });
      });
    }

    function getSelected(id){
      return Array.from(document.getElementById(id).selectedOptions).map(o => o.value);
    }

    function applyFilters(){
      const levels = getSelected('level');
      const categories = getSelected('category');
      const appointments = getSelected('appointment');
      const groups = getSelected('research-group');

      let yrMin = clampYear(document.getElementById('year-min').value || yearBounds.min);
      let yrMax = clampYear(document.getElementById('year-max').value || yearBounds.max);
      if (yrMin > yrMax) { const t = yrMin; yrMin = yrMax; yrMax = t; }

      const topicQ = (document.getElementById('topic-search')?.value || '').trim();

      let selectedRoster;
      if (focusedAuthorID) {
        // When focusing on one author, ignore roster attribute filters
        selectedRoster = rosterData.filter(r => r.OpenAlexID === focusedAuthorID);
      } else {
        selectedRoster = rosterData.filter(r => {
          const groupMatch = [r.RG1, r.RG2, r.RG3, r.RG4].some(g => !groups.length || groups.includes(g));
          return (!levels.length || levels.includes(r.Level)) &&
                 (!categories.length || categories.includes(r.Category)) &&
                 (!appointments.length || appointments.includes(r.Appointment)) &&
                 groupMatch;
        });
      }

      const authorIDs = new Set(selectedRoster.map(r => r.OpenAlexID));

      const selectedPubs = pubData.filter(p => {
        const y = clampYear(p.publication_year);
        if (!(y >= yrMin && y <= yrMax)) return false;
        // Author match across ALL authors on the paper
        const authorMatch = hasAnyAuthor(p, authorIDs);
        if (!authorMatch) return false;
        const haystack = `${p.concepts_list || ''} ${p.primary_topic__subfield__display_name || ''} ${p.primary_topic__display_name || ''}`;
        const topicMatch = !topicQ || fuzzyQueryMatch(topicQ, haystack);
        return topicMatch;
      });

      const contributingIDs = new Set(selectedPubs.flatMap(p => Array.from(extractAllAuthorIDs(p))));
      const contributingRoster = selectedRoster.filter(r => contributingIDs.has(r.OpenAlexID));
      return { contributingRoster, selectedPubs };
    }

    function update(){
      const { contributingRoster, selectedPubs } = applyFilters();
      drawBarChart(selectedPubs);
      drawFacultyTable(contributingRoster);
      drawPublicationList(selectedPubs);
      const fc = document.getElementById('faculty-count');
      fc.textContent = `Faculty contributing: ${contributingRoster.length}` + (focusedAuthorID ? ` (Focused: ${focusedAuthorName}. Use Reset to clear)` : '');
    }

    function drawBarChart(pubs){
      const countsByYearType = new Map();
      const yearsSet = new Set();
      const typesSet = new Set();

      pubs.forEach(p => {
        const y = clampYear(p.publication_year);
        const t = p.type || 'unknown';
        if (!(y >= DEFAULT_START_YEAR && y <= DEFAULT_END_YEAR)) return;
        yearsSet.add(y); typesSet.add(t);
        if (!countsByYearType.has(y)) countsByYearType.set(y, new Map());
        const m = countsByYearType.get(y);
        m.set(t, (m.get(t) || 0) + 1);
      });

      const years = Array.from(yearsSet).sort((a,b)=>a-b);
      const types = Array.from(typesSet).sort();
      const yearsStr = years.map(String); // categorical axis

      const traces = types.map(t => ({
        x: yearsStr,
        y: years.map(y => (countsByYearType.get(y)?.get(t)) || 0),
        name: t,
        type: 'bar'
      }));

      const layout = {
        barmode: 'stack',
        title: 'Publications per Year by Type',
        xaxis: { type: 'category' },
        yaxis: { rangemode: 'tozero' },
        margin: { t: 40 }
      };
      Plotly.newPlot('pub-chart', traces, layout, { displayModeBar: false });
    }

    function drawFacultyTable(faculty){
      const body = document.querySelector('#faculty-table tbody');
      body.innerHTML = '';
      faculty.sort((a,b) => b.H_index - a.H_index);
      faculty.forEach(f => {
        const row = `<tr>
          <td><a href="#" class="author-link" data-id="${escapeHTML(f.OpenAlexID)}" data-name="${escapeHTML(f.Name)}">${escapeHTML(f.Name)}</a></td>
          <td>${toInt(f.H_index)}</td>
          <td>${toInt(f.I10_index)}</td>
          <td>${toInt(f.Works_count)}</td>
          <td>${toInt(f.Total_citations)}</td>
        </tr>`;
        body.insertAdjacentHTML('beforeend', row);
      });
      // Click to focus on one author
      body.querySelectorAll('.author-link').forEach(a => {
        a.addEventListener('click', (e) => {
          e.preventDefault();
          const id = a.getAttribute('data-id');
          const name = a.getAttribute('data-name');
          if (focusedAuthorID === id) { focusedAuthorID = null; focusedAuthorName = ''; }
          else { focusedAuthorID = id; focusedAuthorName = name; }
          update();
        });
      });
    }

    function drawPublicationList(pubs){
      const ul = document.getElementById('publications-list');
      ul.innerHTML = '';
      pubs.sort((a,b) => (b.publication_year - a.publication_year) || (b.cited_by_count - a.cited_by_count));
      pubs.forEach(p => {
        const doi = p.doi && p.doi.startsWith('http') ? p.doi : '';
        const safeTitle = allowItalicsOnly(p.display_name);
        const li = document.createElement('li');
        li.innerHTML = `<strong>${toInt(p.publication_year)}</strong> - <em>${safeTitle}</em> (${escapeHTML(p.type||'')}) [Citations: ${toInt(p.cited_by_count)}]` + (doi ? ` - <a href="${doi}" target="_blank" rel="noopener">DOI</a>` : '');
        ul.appendChild(li);
      });
    }

    function escapeHTML(s){
      return String(s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
    }
    function allowItalicsOnly(s){
      const esc = escapeHTML(s);
      return esc.replace(/&lt;i&gt;/g, '<i>').replace(/&lt;\/i&gt;/g, '</i>');
    }
    function debounce(fn, delay){ let t; return (...args)=>{ clearTimeout(t); t = setTimeout(()=>fn.apply(null,args), delay); }; }

    // ============ Fuzzy search helpers ============
    function normalizeText(t){
      return String(t||'')
        .toLowerCase()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '') // strip diacritics
        .replace(/[^a-z0-9\s]/g, ' ');
    }
    function stem(w){
      let s = w;
      if (s.endsWith('ies') && s.length > 4) return s.slice(0,-3) + 'y';
      if (s.endsWith('sses')) s = s.slice(0,-2);
      else if (s.endsWith('es') && s.length > 4) s = s.slice(0,-2);
      else if (s.endsWith('s') && !s.endsWith('ss') && !s.endsWith('us') && s.length > 3) s = s.slice(0,-1);
      if (s.endsWith('ing') && s.length > 5) s = s.slice(0,-3);
      if (s.endsWith('ed')  && s.length > 4) s = s.slice(0,-2);
      if (s.endsWith('er')  && s.length > 4) s = s.slice(0,-2);
      return s;
    }
    function tokenize(text){
      return normalizeText(text).split(/\s+/).filter(Boolean).map(stem);
    }
    function editDistanceLe1(a,b){
      if (a === b) return true;
      if (a.length > 2 && (b.startsWith(a) || a.startsWith(b))) return true;
      const la=a.length, lb=b.length;
      if (Math.abs(la - lb) > 1) return false;
      let i=0, j=0, edits=0;
      while (i<la && j<lb){
        if (a[i] === b[j]) { i++; j++; continue; }
        if (++edits > 1) return false;
        if (la > lb) i++; else if (lb > la) j++; else { i++; j++; }
      }
      return true;
    }
    function fuzzyQueryMatch(query, text){
      const qTokens = tokenize(query);
      if (!qTokens.length) return true;
      const tTokens = tokenize(text);
      return qTokens.every(qt => tTokens.some(tt => tt.includes(qt) || qt.includes(tt) || editDistanceLe1(qt, tt)));
    }

    // ============ Authorship helpers ============
    function parseIDList(raw){
      if (!raw) return [];
      const s = String(raw).trim();
      // Try JSON first
      try {
        const arr = JSON.parse(s);
        if (Array.isArray(arr)) return arr.map(x => normalizeID(x));
      } catch(_){/* ignore */}
      // Fallback split by common delimiters
      return s.split(/[;\,\|\s]+/).map(x => normalizeID(x)).filter(Boolean);
    }
    function extractAllAuthorIDs(p){
      const candidates = [
        p.authorships_author_ids,
        p.all_author_ids,
        p.author_ids,
        p.authors_ids,
        p.authors_full_ids,
        p.full_author_ids
      ].filter(Boolean);
      if (candidates.length){
        return new Set(parseIDList(candidates[0]));
      }
      const single = normalizeID(p.author_openalex_id || '');
      return new Set(single ? [single] : []);
    }
    function hasAnyAuthor(p, authorSet){
      if (!authorSet || authorSet.size === 0) return false;
      const ids = extractAllAuthorIDs(p);
      for (const id of ids){ if (authorSet.has(id)) return true; }
      return false;
    }
  });
})();
