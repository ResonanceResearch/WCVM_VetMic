
// dashboard-logic.js

export async function initDashboard() {
  console.log("Auto-loading from GitHub...");
  const rosterURL = "https://raw.githubusercontent.com/Jeroendebuck/UCVM_Research/main/data/roster_with_metrics.csv";
  const worksURL = "https://raw.githubusercontent.com/Jeroendebuck/UCVM_Research/main/data/openalex_all_authors_last5y_key_fields_dedup.csv";

  const roster = await fetchCSV(rosterURL);
  console.log("Parsed roster:", roster);

  const works = await fetchCSV(worksURL);
  console.log("Parsed works:", works);

  renderKPIs(roster, works);
  renderChart(works);
  renderTable(roster, works);
}

async function fetchCSV(url) {
  console.log("Fetching:", url);
  const response = await fetch(url);
  const text = await response.text();
  return new Promise((resolve) => {
    Papa.parse(text, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => resolve(results.data),
    });
  });
}

function renderKPIs(roster, works) {
  const facultyCount = roster.length;
  const worksCount = works.length;
  const openAccess = works.filter(w => w.is_oa?.toLowerCase() === 'true').length;
  const oaPct = ((openAccess / worksCount) * 100).toFixed(1);

  const el = document.getElementById("validation");
  el.innerHTML = `${}facultyCount} faculty • ${}worksCount} works • ${}oaPct}% Open Access`;
}

function renderChart(works) {
  const years = {};
  works.forEach(w => {
    const y = parseInt(w.publication_year);
    if (!y || y < 2000) return;
    const type = w.type || "unknown";
    if (!years[y]) years[y] = {};
    years[y][type] = (years[y][type] || 0) + 1;
  });

  const labels = Object.keys(years).sort();
  const types = [...new Set(works.map(w => w.type))];
  const datasets = types.map(t => ({
    label: t,
    data: labels.map(y => years[y]?.[t] || 0),
    stack: "stack1"
  }));

  new Chart(document.getElementById("trendChart"), {
    type: "bar",
    data: { labels, datasets },
    options: { responsive: true, plugins: { legend: { position: 'bottom' } }, scales: { x: { stacked: true }, y: { stacked: true } } }
  });
}

function renderTable(roster, works) {
  const peopleTable = document.getElementById("peopleTable");
  const worksTable = document.getElementById("worksTable");

  peopleTable.querySelector("thead").innerHTML = "<tr><th>Name</th><th>Department</th><th>Works</th></tr>";
  peopleTable.querySelector("tbody").innerHTML = roster.map(p => {
    const count = works.filter(w => w.author_name?.includes(p.display_name)).length;
    return \`<tr><td>\${p.display_name}</td><td>\${p.department}</td><td>\${count}</td></tr>\`;
  }).join("");

  worksTable.querySelector("thead").innerHTML = "<tr><th>Title</th><th>Year</th><th>Type</th><th>Authors</th></tr>";
  worksTable.querySelector("tbody").innerHTML = works.map(w => {
    return \`<tr><td>\${w.title}</td><td>\${w.publication_year}</td><td>\${w.type}</td><td>\${w.author_name}</td></tr>\`;
  }).join("");
}
