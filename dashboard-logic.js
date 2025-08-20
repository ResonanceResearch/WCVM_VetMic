export function initDashboard() {
  const $ = sel => document.querySelector(sel);
  const $$ = sel => Array.from(document.querySelectorAll(sel));
  const ALLOWED_TYPES = new Set(['article', 'book-chapter', 'review']);
  let roster = [];
  let worksRaw = [];
  let isDedup = false;
  let exploded = [];
  let personFilter = null;
  let chart;

  function normalizeToken(x) {
    if (!x) return '';
    const s = String(x).trim();
    const m = s.match(/A\d{6,}/);
    return m ? m[0] : s;
  }

  function rgArray(row) {
    const vals = [row.RG1, row.RG2, row.RG3, row.RG4].map(x => String(x || '').trim()).filter(Boolean);
    const seen = new Set();
    const out = [];
    for (const v of vals) {
      if (!seen.has(v)) {
        seen.add(v);
        out.push(v);
      }
    }
    return out;
  }

  function yearInt(y) {
    const n = parseInt(y, 10);
    return isFinite(n) ? n : null;
  }

  function isOA(s) {
    const t = String(s || '').toLowerCase();
    return t.includes('open') || t === 'gold';
  }

  function typeSimple(s) {
    const t = String(s || '').toLowerCase();
    if (t.includes('book-chapter')) return 'book-chapter';
    if (t.includes('review')) return 'review';
    if (t.includes('article')) return 'article';
    return 'other';
  }

  function parseCSV(file) {
    return new Promise((resolve, reject) => {
      if (window.Papa) {
        Papa.parse(file, { header: true, skipEmptyLines: true, complete: res => resolve(res.data), error: reject });
      } else {
        const fr = new FileReader();
        fr.onload = () => resolve(simpleCSV(fr.result));
        fr.onerror = reject;
        fr.readAsText(file);
      }
    });
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c]));
  }

  function validate() {
  const v = $('#validation');
  const needRoster = ['Name', 'OpenAlexID'];
  const rosterOK = roster.length && needRoster.every(k => k in roster[0]);
  const worksOK = worksRaw.length > 0;
  const kind = isDedup ? 'dedup' : 'non‑dedup';

  v.innerHTML = `<div class="badges"><span class="badge">Roster: ${rosterOK ? 'loaded' : 'missing'}</span><span class="badge">Works: ${worksOK ? 'loaded (' + kind + ')' : 'missing'}</span></div>`;
  if (rosterOK && worksOK) {
    $('#status').textContent = `${roster.length} roster rows • ${worksRaw.length} works rows (${kind})`;
  }
}

  async function autoLoadFromGitHub() {
    console.log("Auto-loading from GitHub...");
    if (!roster.length) {
      try {
        console.log("Fetching roster CSV...");
        const rosterResp = await fetch('https://raw.githubusercontent.com/Jeroendebuck/UCVM_Research/main/data/roster_with_metrics.csv');
        if (rosterResp.ok) {
          const text = await rosterResp.text();
          roster = Papa.parse(text, { header: true, skipEmptyLines: true }).data;
          roster.forEach(r => {
            r.OpenAlexID = normalizeToken(r.OpenAlexID);
            r.RGs = rgArray(r);
          });
        }
      } catch (e) { console.error('Roster auto-load failed', e); }
    }

    if (!worksRaw.length) {
      try {
        const worksResp = await fetch('https://raw.githubusercontent.com/Jeroendebuck/UCVM_Research/main/data/openalex_all_authors_last5y_key_fields_dedup.csv');
        if (worksResp.ok) {
          const text = await worksResp.text();
          worksRaw = Papa.parse(text, { header: true, skipEmptyLines: true }).data;
          isDedup = worksRaw.length > 0 && ('ucvm_openalex_ids' in worksRaw[0]);
          exploded = [];

          if (isDedup) {
            for (const w of worksRaw) {
              const ids = String(w.ucvm_openalex_ids || '').split(';').map(s => normalizeToken(s.trim())).filter(Boolean);
              if (!ids.length) {
                exploded.push({ ...w, __author: null });
                continue;
              }
              for (const id of ids) {
                exploded.push({ ...w, __author: id });
              }
            }
          } else {
            exploded = worksRaw.map(w => ({ ...w, __author: normalizeToken(w.author_openalex_id) }));
          }
          exploded.forEach(r => r.__typeSimple = typeSimple(r.type));
        }
      } catch (e) { console.error('Works auto-load failed', e); }
    }

    validate();
    populateRosterFilters();
    populateWorkFilters();
    renderAll();
  }

  // Entry point after definitions
  autoLoadFromGitHub();
}
