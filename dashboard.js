// --- dashboard.js with fuzzy search ---
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

      const years = pubData.map(p => p.publication_year).filter(isFinite);
      yearBounds.min = Math.min(...years);
      yearBounds.max = Math.max(...years);
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
    ymin.value=String(min); ymax.value=String(max);
    ymin.min=ymin.min=String(min); ymax.min=ymax.min=String(min);
    ymin.max=ymax.max=String(max);
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

    const selectedRoster=rosterData.filter(r=>{
      const groupMatch=[r.RG1,r.RG2,r.RG3,r.RG4].some(g=>!groups.length||groups.includes(g));
      return(!levels.length||levels.includes(r.Level))&&(!categories.length||categories.includes(r.Category))&&(!appointments.length||appointments.includes(r.Appointment))&&groupMatch;
    });
    const authorIDs=new Set(selectedRoster.map(r=>r.OpenAlexID));
    const selectedPubs=pubData.filter(p=>{
      const inYear=p.publication_year>=yrMin&&p.publication_year<=yrMax;
      const authorMatch=authorIDs.has(p.author_openalex_id);
      const haystack=`${p.concepts_list||""} ${p.primary_topic__subfield__display_name||""} ${p.primary_topic__display_name||""}`;
      const topicMatch=!topicQ||fuzzyQueryMatch(topicQ,haystack);
      return inYear&&authorMatch&&topicMatch;
    });
    const contributingIDs=new Set(selectedPubs.map(p=>p.author_openalex_id));
    const contributingRoster=selectedRoster.filter(r=>contributingIDs.has(r.OpenAlexID));
    return{contributingRoster,selectedPubs};
  }

  function update(){
    const {contributingRoster,selectedPubs}=applyFilters();
    drawBarChart(selectedPubs);
    drawFacultyTable(contributingRoster);
    drawPublicationList(selectedPubs);
    document.getElementById("faculty-count").textContent=`Faculty contributing: ${contributingRoster.length}`;
  }

  function drawBarChart(pubs){
    const dataMap=new Map(),yearsSet=new Set(),typesSet=new Set();
    pubs.forEach(p=>{
      const y=toInt(p.publication_year),t=p.type||"unknown";
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

  function drawFacultyTable(faculty){
    const body=document.querySelector("#faculty-table tbody");
    body.innerHTML="";
    faculty.sort((a,b)=>b.H_index-a.H_index);
    faculty.forEach(f=>{
      const row=`<tr><td>${escapeHTML(f.Name)}</td><td>${toInt(f.H_index)}</td><td>${toInt(f.I10_index)}</td><td>${toInt(f.Works_count)}</td><td>${toInt(f.Total_citations)}</td></tr>`;
      body.insertAdjacentHTML("beforeend",row);
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
  }

  function escapeHTML(s){return String(s||"").replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));}
  function allowItalicsOnly(s){return escapeHTML(s).replace(/&lt;i&gt;/g,"<i>").replace(/&lt;\/i&gt;/g,"</i>");}
  function debounce(fn,delay){let t;return(...args)=>{clearTimeout(t);t=setTimeout(()=>fn(...args),delay);};}

  // --- fuzzy helpers ---
  function normalizeText(t){return String(t||"").toLowerCase().normalize("NFD").replace(/\p{Diacritic}/gu,"").replace(/[^a-z0-9\s]/g," ");}
  function stem(w){if(w.endsWith("ies")&&w.length>4)return w.slice(0,-3)+"y";if(w.endsWith("sses"))w=w.slice(0,-2);else if(w.endsWith("es")&&w.length>4)w=w.slice(0,-2);else if(w.endsWith("s")&&!w.endsWith("ss")&&!w.endsWith("us")&&w.length>3)w=w.slice(0,-1);if(w.endsWith("ing")&&w.length>5)w=w.slice(0,-3);if(w.endsWith("ed")&&w.length>4)w=w.slice(0,-2);if(w.endsWith("er")&&w.length>4)w=w.slice(0,-2);return w;}
  function tokenize(text){return normalizeText(text).split(/\s+/).filter(Boolean).map(stem);}
  function editDistanceLe1(a,b){if(a===b)return true;if(a.length>2&&(b.startsWith(a)||a.startsWith(b)))return true;const la=a.length,lb=b.length;if(Math.abs(la-lb)>1)return false;let i=0,j=0,edits=0;while(i<la&&j<lb){if(a[i]===b[j]){i++;j++;continue;}if(++edits>1)return false;if(la>lb)i++;else if(lb>la)j++;else{i++;j++;}}return true;}
  function fuzzyQueryMatch(query,text){const qTokens=tokenize(query);if(!qTokens.length)return true;const tTokens=tokenize(text);return qTokens.every(qt=>tTokens.some(tt=>tt.includes(qt)||qt.includes(tt)||editDistanceLe1(qt,tt)));}
});
