document.addEventListener("DOMContentLoaded", () => {
  const rosterPath = "data/roster_with_metrics.csv";
  const pubsPath = "data/openalex_all_authors_last5y_key_fields_dedup.csv";

  let rosterData = [];
  let pubData = [];
  let yearBounds = { min: null, max: null };

  Promise.all([fetchCSV(rosterPath), fetchCSV(pubsPath)])
    .then(([roster, pubs]) => {
      rosterData = parseCSV(roster);
      pubData = parseCSV(pubs);

      // Normalize IDs and numeric fields
      pubData.forEach(p => {
        p.author_openalex_id = normalizeID(p.author_openalex_id || p.author_name || "");
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

      // Year bounds
      const years = pubData.map(p => p.publication_year).filter(isFinite);
      yearBounds.min = Math.min(...years);
      yearBounds.max = Math.max(...years);
      initYearInputs(yearBounds);

      populateFilters(rosterData);
      wireMultiSelectToggle();
      wireFilterEvents();

      update(); // initial render
    })
    .catch(err => console.error("Failed to load CSVs", err));

  // ---------- helpers ----------
  function toInt(x) {
    const n = Number(x);
    return Number.isFinite(n) ? Math.round(n) : 0;
  }

  function normalizeID(id) {
    return String(id || "").replace(/^https?:\/\/openalex\.org\//, "").trim();
  }

  function fetchCSV(path) { return fetch(path).then(resp => resp.text()); }

  // CSV parser that handles quoted commas and quotes
  function parseCSV(text) {
    const lines = text.replace(/\r/g, "").split("\n").filter(Boolean);
    const headers = splitCSVLine(lines.shift());
    return lines.map(line => {
      const values = splitCSVLine(line);
      const row = {};
      headers.forEach((h, i) => {
        const key = h.trim();
        let v = values[i] !== undefined ? values[i] : "";
        // trim surrounding quotes
        v = v.replace(/^"|"$/g, "");
        row[key] = v;
      });
      return row;
    });
  }

  function splitCSVLine(line) {
    const result = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
        else { inQuotes = !inQuotes; }
      } else if (ch === "," && !inQuotes) {
        result.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
    result.push(current);
    return result;
  }

  function populateFilters(roster) {
    const levels = new Set(), categories = new Set(), appointments = new Set(), groups = new Set();
    roster.forEach(r => {
      if (r.Level) levels.add(r.Level);
      if (r.Category) categories.add(r.Category);
      if (r.Appointment) appointments.add(r.Appointment);
      [r.RG1, r.RG2, r.RG3, r.RG4].forEach(g => g && groups.add(g));
    });
    fillSelect("level", levels);
    fillSelect("category", categories);
    fillSelect("appointment", appointments);
    fillSelect("research-group", groups);
  }

  function fillSelect(id, options) {
    const select = document.getElementById(id);
    select.innerHTML = "";
    Array.from(options).sort().forEach(opt => {
      const o = document.createElement("option");
      o.value = o.textContent = opt;
      select.appendChild(o);
    });
  }

  function initYearInputs({ min, max }) {
    const ymin = document.getElementById("year-min");
    const ymax = document.getElementById("year-max");
    ymin.value = String(min);
    ymax.value = String(max);
    ymin.min = String(min); ymin.max = String(max);
    ymax.min = String(min); ymax.max = String(max);
  }

  function wireFilterEvents() {
    // Multi selects
    document.querySelectorAll("#filters select").forEach(sel => sel.addEventListener("change", update));

    // Clear buttons per-select
    document.querySelectorAll(".clear-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        const id = btn.getAttribute("data-target");
        const el = document.getElementById(id);
        Array.from(el.options).forEach(o => { o.selected = false; });
        update();
      });
    });

    // Year inputs
    document.getElementById("year-min").addEventListener("input", update);
    document.getElementById("year-max").addEventListener("input", update);

    // Combined topic search (concepts_list + primary_topic__subfield__display_name + primary_topic__display_name)
    const topic = document.getElementById("topic-search");
    if (topic) topic.addEventListener("input", debounce(update, 200));

    // Global reset
    document.getElementById("reset-filters").addEventListener("click", () => {
      document.querySelectorAll("#filters select").forEach(sel => {
        Array.from(sel.options).forEach(o => o.selected = false);
      });
      initYearInputs(yearBounds);
      if (topic) topic.value = "";
      update();
    });
  }

  // Allow deselect by simple clicking (no Ctrl/Cmd) by toggling on mousedown
  function wireMultiSelectToggle() {
    document.querySelectorAll("select.multi").forEach(sel => {
      sel.addEventListener("mousedown", (e) => {
        if (e.target.tagName.toLowerCase() === "option") {
          e.preventDefault();
          const opt = e.target;
          opt.selected = !opt.selected;
          sel.dispatchEvent(new Event("change", { bubbles: true }));
        }
      });
    });
  }

  function getSelected(id) {
    return Array.from(document.getElementById(id).selectedOptions).map(o => o.value);
  }

  function applyFilters() {
    const levels = getSelected("level");
    const categories = getSelected("category");
    const appointments = getSelected("appointment");
    const groups = getSelected("research-group");
    const yrMin = toInt(document.getElementById("year-min").value || yearBounds.min);
    const yrMax = toInt(document.getElementById("year-max").value || yearBounds.max);
    const topicQ = (document.getElementById("topic-search")?.value || "").trim().toLowerCase();

    const selectedRoster = rosterData.filter(row => {
      const groupMatch = [row.RG1, row.RG2, row.RG3, row.RG4].some(g => !groups.length || groups.includes(g));
      return (!levels.length || levels.includes(row.Level)) &&
             (!categories.length || categories.includes(row.Category)) &&
             (!appointments.length || categories.includes ? !appointments.length || appointments.includes(row.Appointment) : !appointments.length || appointments.includes(row.Appointment)) &&
             groupMatch;
    });

    const authorIDs = new Set(selectedRoster.map(r => r.OpenAlexID));

    const selectedPubs = pubData.filter(p => {
      const inYear = p.publication_year >= yrMin && p.publication_year <= yrMax;
      const authorMatch = authorIDs.has(p.author_openalex_id);
      const topicMatch = !topicQ || (
        (p.concepts_list || "").toLowerCase().includes(topicQ) ||
        (p.primary_topic__subfield__display_name || "").toLowerCase().includes(topicQ) ||
        (p.primary_topic__display_name || "").toLowerCase().includes(topicQ)
      );
      return inYear && authorMatch && topicMatch;
    });

    // Keep only faculty with at least one pub after term filters
    const contributingIDs = new Set(selectedPubs.map(p => p.author_openalex_id));
    const contributingRoster = selectedRoster.filter(r => contributingIDs.has(r.OpenAlexID));

    return { contributingRoster, selectedPubs };
  }

  function update() {
    const { contributingRoster, selectedPubs } = applyFilters();

    drawBarChart(selectedPubs);
    drawFacultyTable(contributingRoster);
    drawPublicationList(selectedPubs);
    document.getElementById("faculty-count").textContent = `Faculty contributing: ${contributingRoster.length}`;
  }

  function drawBarChart(pubs) {
    // Aggregate counts
    const dataMap = new Map(); // year -> type -> count
    const yearsSet = new Set();
    const typesSet = new Set();

    pubs.forEach(p => {
      const y = toInt(p.publication_year);
      const t = p.type || "unknown";
      yearsSet.add(y); typesSet.add(t);
      if (!dataMap.has(y)) dataMap.set(y, new Map());
      const byType = dataMap.get(y);
      byType.set(t, (byType.get(t) || 0) + 1);
    });

    const years = Array.from(yearsSet).sort((a,b)=>a-b);
    const types = Array.from(typesSet).sort();

    // Use strings for x to force categorical axis (prevents decimal ticks)
    const yearsStr = years.map(String);

    const traces = types.map(t => ({
      x: yearsStr,
      y: years.map(y => (dataMap.get(y)?.get(t)) || 0),
      name: t,
      type: "bar"
    }));

    const layout = {
      barmode: "stack",
      title: "Publications per Year by Type",
      margin: { t: 40 },
      xaxis: { type: "category" }, // categorical axis => no decimal ticks
      yaxis: { rangemode: "tozero" }
    };
    Plotly.newPlot("pub-chart", traces, layout, { displayModeBar: false });
  }

  function drawFacultyTable(faculty) {
    const body = document.querySelector("#faculty-table tbody");
    body.innerHTML = "";
    faculty.sort((a,b) => (b.H_index - a.H_index));
    faculty.forEach(f => {
      const row = `<tr>
        <td>${escapeHTML(f.Name)}</td>
        <td>${toInt(f.H_index)}</td>
        <td>${toInt(f.I10_index)}</td>
        <td>${toInt(f.Works_count)}</td>
        <td>${toInt(f.Total_citations)}</td>
      </tr>`;
      body.insertAdjacentHTML("beforeend", row);
    });
  }

  function drawPublicationList(pubs) {
    const ul = document.getElementById("publications-list");
    ul.innerHTML = "";
    pubs.sort((a, b) => (b.publication_year - a.publication_year) || (b.cited_by_count - a.cited_by_count));
    pubs.forEach(p => {
      const doi = p.doi && p.doi.startsWith("http") ? p.doi : "";
      // Escape everything, then re-enable only <i>...</i> for titles
      const safeTitle = allowItalicsOnly(p.display_name);
      const li = document.createElement("li");
      li.innerHTML =
        `<strong>${toInt(p.publication_year)}</strong> - <em>${safeTitle}</em> (${escapeHTML(p.type||"")}) [Citations: ${toInt(p.cited_by_count)}]` +
        (doi ? ` - <a href="${doi}" target="_blank" rel="noopener">DOI</a>` : "");
      ul.appendChild(li);
    });
  }

  function escapeHTML(s){
    return String(s||"").replace(/[&<>\"']/g, c => (
      {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]
    ));
  }

  // Allow only <i>…</i> and escape everything else
  function allowItalicsOnly(s) {
    const escaped = escapeHTML(s);
    return escaped.replace(/&lt;i&gt;/g, "<i>").replace(/&lt;\/i&gt;/g, "</i>");
  }

  function debounce(fn, delay){ let t; return (...args)=>{ clearTimeout(t); t = setTimeout(()=>fn.apply(null,args), delay); }; }
});
