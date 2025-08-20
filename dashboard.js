// --- dashboard.js with fuzzy search ---
document.addEventListener("DOMContentLoaded", () => {
  const rosterPath = "data/roster_with_metrics.csv";
  const pubsPath = "data/openalex_all_authors_last5y_key_fields_dedup.csv";

  let rosterData = [];
  let pubData = [];
  let yearBounds = { min: 2021, max: 2025 };
  let focusedAuthorID = null;
  let focusedAuthorName = "";

  Promise.all([fetchCSV(rosterPath), fetchCSV(pubsPath)])
    .then(([roster, pubs]) => {
      rosterData = parseCSV(roster);
      pubData = parseCSV(pubs);

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

      const allYears = pubData.map(p => p.publication_year).filter(y => Number.isFinite(y));
      const yearsValid = allYears.filter(y => y >= 2021 && y <= 2025);
      // Default to 2021–2025 regardless of sparse data; if dataset ends earlier, cap to observed max
      yearBounds.min = 2021;
      yearBounds.max = Math.max(2021, Math.min(2025, Math.max(...(yearsValid.length ? yearsValid : allYears.filter(y=>y>=1900)), 2025)));
      initYearInputs(yearBounds);

      populateFilters(rosterData);
      wireMultiSelectToggle();
      wireFilterEvents();

      update();
    });

  function toInt(x) {
    const n = Number(x);
    return Number.isFinite(n) ? Math.round(n) : 0;
  }
  function normalizeID(id) {
    return String(id || "").replace(/^https?:\/\/openalex\.org\//, "").trim();
  }
  function fetchCSV(path) { return fetch(path).then(resp => resp.text()); }
  function parseCSV(text) {
    const lines = text.replace(/\r/g, "").split("\n").filter(Boolean);
    const headers = splitCSVLine(lines.shift());
    return lines.map(line => {
      const values = splitCSVLine(line);
      const row = {};
      headers.forEach((h, i) => {
        let v = values[i] !== undefined ? values[i] : "";
        v = v.replace(/^"|"$/g, "");
        row[h.trim()] = v;
      });
      return row;
    });
  }
  function splitCSVLine(line) {
    const result = [];
    let current = "", inQuotes = false;
    for (let i=0;i<line.length;i++) {
      const ch=line[i];
      if(ch==='"'){
        if(inQuotes && line[i+1]==='"'){ current+='"'; i++; }
        else inQuotes=!inQuotes;
      } else if(ch===',' && !inQuotes){ result.push(current); current=''; }
      else current+=ch;
    }
    result.push(current);
    return result;
  }

  function populateFilters(roster){
    const levels=new Set(),categories=new Set(),appointments=new Set(),groups=new Set();
    roster.forEach(r=>{
      if(r.Level) levels.add(r.Level);
      if(r.Category) categories.add(r.Category);
      if(r.Appointment) appointments.add(r.Appointment);
      [r.RG1,r.RG2,r.RG3,r.RG4].forEach(g=>g&&groups.add(g));
    });
    fillSelect("level",levels);
    fillSelect("category",categories);
    fillSelect("appointment",appointments);
    fillSelect("research-group",groups);
  }
  function fillSelect(id,options){
    const select=document.getElementById(id);
    select.innerHTML="";
    Array.from(options).sort().forEach(opt=>{
      const o=document.createElement("option");
      o.value=o.textContent=opt;
      select.appendChild(o);
    });
  }
  function initYearInputs({min,max}){
    const ymin=document.getElementById("year-min"),ymax=document.getElementById("year-max");
    const start = 2021; const end = Math.max(start, max || 2025);
    ymin.value=String(start); ymax.value=String(end);
    ymin.min=String(start); ymax.min=String(start);
    ymin.max=String(end);   ymax.max=String(end);
  }

  function wireFilterEvents(){
    document.querySelectorAll("#filters select").forEach(sel=>sel.addEventListener("change",update));
    document.querySelectorAll(".clear-btn").forEach(btn=>{
      btn.addEventListener("click",()=>{
        const id=btn.getAttribute("data-target");
        Array.from(document.getElementById(id).options).forEach(o=>o.selected=false);
        update();
      });
    });
    document.getElementById("year-min").addEventListener("input",update);
    document.getElementById("year-max").addEventListener("input",update);
    const topic=document.getElementById("topic-search");
    if(topic) topic.addEventListener("input",debounce(update,200));
    document.getElementById("reset-filters").addEventListener("click",()=>{
      document.querySelectorAll("#filters select").forEach(sel=>Array.from(sel.options).forEach(o=>o.selected=false));
      focusedAuthorID = null; focusedAuthorName = "";
      initYearInputs(yearBounds);
      if(topic) topic.value="";
      update();
    });
  }
  function wireMultiSelectToggle(){
    document.querySelectorAll("select.multi").forEach(sel=>{
      sel.addEventListener("mousedown",e=>{
        if(e.target.tagName.toLowerCase()==="option"){
          e.preventDefault();
          e.target.selected=!e.target.selected;
          sel.dispatchEvent(new Event("change",{bubbles:true}));
        }
      });
    });
  }

  function getSelected(id){return Array.from(document.getElementById(id).selectedOptions).map(o=>o.value);}  

  function applyFilters(){
    const levels=getSelected("level"), categories=getSelected("category"), appointments=getSelected("appointment"), groups=getSelected("research-group");
    const yrMin=toInt(document.getElementById("year-min").value||yearBounds.min);
    const yrMax=toInt(document.getElementById("year-max").value||yearBounds.max);
    const topicQ=(document.getElementById("topic-search")?.value||"").trim();

    let selectedRoster;
    if (focusedAuthorID) {
      // When focusing an author, ignore roster attribute filters
      selectedRoster = rosterData.filter(r => r.OpenAlexID === focusedAuthorID);
    } else {
      selectedRoster = rosterData.filter(r=>{
        const groupMatch=[r.RG1,r.RG2,r.RG3,r.RG4].some(g=>!groups.length||groups.includes(g));
        return(!levels.length||levels.includes(r.Level))&&(!categories.length||categories.includes(r.Category))&&(!appointments.length||appointments.includes(r.Appointment))&&groupMatch;
      });
    }

    const authorIDs=new Set(selectedRoster.map(r=>r.OpenAlexID));

    const selectedPubs=pubData.filter(p=>{
      const y = toInt(p.publication_year);
      if (!(y>=yrMin && y<=yrMax)) return false;
      // Author match across ALL authors on the paper
      const authorMatch = hasAnyAuthor(p, authorIDs);
      if (!authorMatch) return false;
      const haystack=`${p.concepts_list||""} ${p.primary_topic__subfield__display_name||""} ${p.primary_topic__display_name||""}`;
      const topicMatch=!topicQ||fuzzyQueryMatch(topicQ,haystack);
      return topicMatch;
    });

    const contributingIDs=new Set(selectedPubs.flatMap(p => Array.from(extractAllAuthorIDs(p))));
    const contributingRoster=selectedRoster.filter(r=>contributingIDs.has(r.OpenAlexID));
    return{contributingRoster,selectedPubs};
  }

  function update(){
    const {contributingRoster,selectedPubs}=applyFilters();
    drawBarChart(selectedPubs);
    drawFacultyTable(contributingRoster);
    drawPublicationList(selectedPubs);
    const fc = document.getElementById("faculty-count");
    fc.textContent = `Faculty contributing: ${contributingRoster.length}` + (focusedAuthorID? ` (Focused: ${focusedAuthorName}. Use Reset to clear)` : "");
  }=applyFilters();
    drawBarChart(selectedPubs);
    drawFacultyTable(contributingRoster);
    drawPublicationList(selectedPubs);
    document.getElementById("faculty-count").textContent=`Faculty contributing: ${contributingRoster.length}`;
  }

  function drawBarChart(pubs){
    const dataMap=new Map(),yearsSet=new Set(),typesSet=new Set();
    pubs.forEach(p=>{
      const y=toInt(p.publication_year),t=p.type||"unknown";
      if (!(y>=2021 && y<=Math.max(2021, yearBounds.max))) return; // keep within default window
      yearsSet.add(y); typesSet.add(t);
      if(!dataMap.has(y)) dataMap.set(y,new Map());
      const byType=dataMap.get(y);
      byType.set(t,(byType.get(t)||0)+1);
    });
    const years=[...yearsSet].sort((a,b)=>a-b), types=[...typesSet].sort();
    const yearsStr=years.map(String);
    const traces=types.map(t=>({x:yearsStr,y:years.map(y=>(dataMap.get(y)?.get(t))||0),name:t,type:"bar"}));
    Plotly.newPlot("pub-chart",traces,{barmode:"stack",title:"Publications per Year by Type",xaxis:{type:"category"},yaxis:{rangemode:"tozero"}},{displayModeBar:false});
  }
  }

  function drawFacultyTable(faculty){
    const body=document.querySelector("#faculty-table tbody");
    body.innerHTML="";
    faculty.sort((a,b)=>b.H_index-a.H_index);
    faculty.forEach(f=>{
      const row=`<tr>
        <td><a href="#" class="author-link" data-id="${escapeHTML(f.OpenAlexID)}" data-name="${escapeHTML(f.Name)}">${escapeHTML(f.Name)}</a></td>
        <td>${toInt(f.H_index)}</td>
        <td>${toInt(f.I10_index)}</td>
        <td>${toInt(f.Works_count)}</td>
        <td>${toInt(f.Total_citations)}</td>
      </tr>`;
      body.insertAdjacentHTML("beforeend",row);
    });
    // Wire up clicks to focus on single author
    body.querySelectorAll('.author-link').forEach(a => {
      a.addEventListener('click', (e) => {
        e.preventDefault();
        const id = a.getAttribute('data-id');
        const name = a.getAttribute('data-name');
        if (focusedAuthorID === id) { focusedAuthorID = null; focusedAuthorName = ""; }
        else { focusedAuthorID = id; focusedAuthorName = name; }
        update();
      });
    });
  }

  function drawPublicationList(pubs){
    const ul=document.getElementById("publications-list");
    ul.innerHTML="";
    pubs.sort((a,b)=>b.publication_year-a.publication_year||b.cited_by_count-a.cited_by_count);
    pubs.forEach(p=>{
      const doi=p.doi&&p.doi.startsWith("http")?p.doi:"";
      const safeTitle=allowItalicsOnly(p.display_name);
      const li=document.createElement("li");
      li.innerHTML=`<strong>${toInt(p.publication_year)}</strong> - <em>${safeTitle}</em> (${escapeHTML(p.type||"")}) [Citations: ${toInt(p.cited_by_count)}]`+(doi?` - <a href="${doi}" target="_blank" rel="noopener">DOI</a>`:"");
      ul.appendChild(li);
    });
  }</strong> - <em>${safeTitle}</em> (${escapeHTML(p.type||"")}) [Citations: ${toInt(p.cited_by_count)}]`+(doi?` - <a href="${doi}" target="_blank" rel="noopener">DOI</a>`:"");
      ul.appendChild(li);
    });
  }

  function escapeHTML(s){return String(s||"").replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));}
  function allowItalicsOnly(s){return escapeHTML(s).replace(/&lt;i&gt;/g,"<i>").replace(/&lt;\/i&gt;/g,"</i>");}
  function debounce(fn,delay){let t;return(...args)=>{clearTimeout(t);t=setTimeout(()=>fn(...args),delay);};}

  // --- fuzzy helpers ---
  function normalizeText(t){return String(t||"").toLowerCase().normalize("NFD").replace(/\p{Diacritic}/gu,"").replace(/[^a-z0-9\s]/g," ");}
  function stem(w){if(w.endsWith("ies")&&w.length>4)return w.slice(0,-3)+"y";if(w.endsWith("sses"))w=w.slice(0,-2);else if(w.endsWith("es")&&w.length>4)w=w.slice(0,-2);else if(w.endsWith("s")&&!w.endsWith("ss")&&!w.endsWith("us")&&w.length>3)w=w.slice(0,-1);if(w.endsWith("ing")&&w.length>5)w=w.slice(0,-3);if(w.endsWith("ed")&&w.length>4)w=w.slice(0,-2);if(w.endsWith("er")&&w.length>4)w=w.slice(0,-2);return w;}
  function tokenize(text){return normalizeText(text).split(/\s+/).filter(Boolean).map(stem);}
  function editDistanceLe1(a,b){if(a===b)return true;if(a.length>2&&(b.startsWith(a)||a.startsWith(b)))return true;const la=a.length,lb=b.length;if(Math.abs(la-lb)>1)return false;let i=0,j=0,edits=0;while(i<la&&j<lb){if(a[i]===b[j]){i++;j++;continue;}if(++edits>1)return false;if(la>lb)i++;else if(lb>la)j++;else{i++;j++;}}return true;}
  function fuzzyQueryMatch(query,text){
    const qTokens=tokenize(query);
    if(!qTokens.length)return true;
    const tTokens=tokenize(text);
    return qTokens.every(qt=>tTokens.some(tt=>tt.includes(qt)||qt.includes(tt)||editDistanceLe1(qt,tt)));
  }

  // --- authorship helpers ---
  function extractAllAuthorIDs(p){
    const candidates = [
      p.all_author_ids, p.author_ids, p.authors_ids, p.authorships_author_ids, p.authorships_author_id_list,
      p.authors_full_ids, p.full_author_ids
    ].filter(Boolean);
    if (candidates.length){
      const raw = String(candidates[0]);
      return new Set(raw.split(/[;,\|]/).map(s => normalizeID(s.trim())).filter(Boolean));
    }
    // Fallback to single author field
    const single = normalizeID(p.author_openalex_id || "");
    return new Set(single ? [single] : []);
  }
  function hasAnyAuthor(p, authorIDSet){
    if (!authorIDSet || authorIDSet.size===0) return false;
    const ids = extractAllAuthorIDs(p);
    for (const id of ids){ if (authorIDSet.has(id)) return true; }
    return false;
  }
});
