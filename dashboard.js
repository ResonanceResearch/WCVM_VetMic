document.addEventListener("DOMContentLoaded", () => {
  const rosterPath = "data/roster_with_metrics.csv";
  const pubsPath = "data/openalex_all_authors_last5y_key_fields_dedup.csv";

  Promise.all([fetchCSV(rosterPath), fetchCSV(pubsPath)]).then(([roster, pubs]) => {
    const rosterData = parseCSV(roster);
    const pubData = parseCSV(pubs);
    const openAlexToName = {};
    rosterData.forEach(row => { if (row.OpenAlexID) openAlexToName[row.OpenAlexID] = row.Name; });

    // Populate filters
    populateFilters(rosterData);
    document.querySelectorAll("#filters select").forEach(select => select.addEventListener("change", () => update(rosterData, pubData)));

    update(rosterData, pubData);
  });

  function fetchCSV(path) {
    return fetch(path).then(resp => resp.text());
  }

  function parseCSV(text) {
    const [headerLine, ...lines] = text.trim().split("\n");
    const headers = headerLine.split(",");
    return lines.map(line => {
      const values = line.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/);
      const row = {};
      headers.forEach((h, i) => row[h.trim()] = values[i] ? values[i].replace(/^"|"$/g, '') : "");
      return row;
    });
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
    options.forEach(opt => {
      const o = document.createElement("option");
      o.value = o.textContent = opt;
      select.appendChild(o);
    });
  }

  function update(roster, pubs) {
    const levels = getSelected("level");
    const categories = getSelected("category");
    const appointments = getSelected("appointment");
    const groups = getSelected("research-group");

    const selected = roster.filter(row => {
      const groupMatch = [row.RG1, row.RG2, row.RG3, row.RG4].some(g => !groups.length || groups.includes(g));
      return (!levels.length || levels.includes(row.Level)) &&
             (!categories.length || categories.includes(row.Category)) &&
             (!appointments.length || appointments.includes(row.Appointment)) &&
             groupMatch;
    });

    const authorIDs = new Set(selected.map(r => r.OpenAlexID));
    const selectedPubs = pubs.filter(p => authorIDs.has(p.author_openalex_id));

    drawBarChart(selectedPubs);
    drawFacultyTable(selected);
    drawPublicationList(selectedPubs);
    document.getElementById("faculty-count").textContent = `Faculty contributing: ${selected.length}`;
  }

  function getSelected(id) {
    return Array.from(document.getElementById(id).selectedOptions).map(o => o.value);
  }

  function drawBarChart(pubs) {
    const dataMap = {};
    pubs.forEach(p => {
      const year = p.publication_year;
      const type = p.type || "unknown";
      if (!dataMap[year]) dataMap[year] = {};
      if (!dataMap[year][type]) dataMap[year][type] = 0;
      dataMap[year][type]++;
    });
    const years = [...new Set(pubs.map(p => p.publication_year))].sort();
    const types = [...new Set(pubs.map(p => p.type))];
    const traces = types.map(t => ({
      x: years,
      y: years.map(y => (dataMap[y] && dataMap[y][t]) || 0),
      name: t,
      type: 'bar'
    }));
    Plotly.newPlot("pub-chart", traces, {barmode: 'stack', title: 'Publications per Year by Type'});
  }

  function drawFacultyTable(faculty) {
    const body = document.querySelector("#faculty-table tbody");
    body.innerHTML = "";
    faculty.forEach(f => {
      const row = `<tr><td>${f.Name}</td><td>${f.H_index}</td><td>${f.I10_index}</td><td>${f.Works_count}</td><td>${f.Total_citations}</td></tr>`;
      body.insertAdjacentHTML("beforeend", row);
    });
  }

  function drawPublicationList(pubs) {
    const ul = document.getElementById("publications-list");
    ul.innerHTML = "";
    pubs.sort((a, b) => b.publication_year - a.publication_year);
    pubs.forEach(p => {
      const li = document.createElement("li");
      li.innerHTML = `<strong>${p.publication_year}</strong> - <em>${p.display_name}</em> (${p.type}) [Citations: ${p.cited_by_count}]`;
      ul.appendChild(li);
    });
  }
});
