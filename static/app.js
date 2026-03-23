/* =====================================================================
   SERP Dashboard — Client-side logic
   ===================================================================== */

'use strict';

// -----------------------------------------------------------------------
// State
// -----------------------------------------------------------------------
let activeKeywordId   = null;
let activeKeywordName = '';
let activeWeekId      = null;
let currentResults    = [];
let activeFilter      = null;

// -----------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  // Default to first keyword + latest week
  const firstKwBtn = document.querySelector('.kw-btn');
  const weekSelect = document.getElementById('week-select');

  if (weekSelect.options.length) {
    activeWeekId = parseInt(weekSelect.options[0].value, 10);
  }

  weekSelect.addEventListener('change', () => {
    activeWeekId = parseInt(weekSelect.value, 10);
    if (activeKeywordId) loadResults();
  });

  document.querySelectorAll('.kw-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.kw-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeKeywordId   = parseInt(btn.dataset.id, 10);
      activeKeywordName = btn.dataset.name;
      document.getElementById('kw-title').textContent = activeKeywordName;
      document.getElementById('compare-toggle-btn').style.display = 'inline-flex';
      document.getElementById('export-btn').style.display = 'inline-flex';
      loadResults();
    });
  });

  // Export button
  document.getElementById('export-btn').addEventListener('click', () => {
    if (!activeWeekId) return;
    window.location.href = `/api/export?week_id=${activeWeekId}`;
  });

  // Filter buttons
  document.querySelectorAll('.stat-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const f = btn.dataset.filter;
      if (activeFilter === f) {
        // toggle off
        activeFilter = null;
        document.querySelectorAll('.stat-filter-btn').forEach(b => b.classList.remove('active'));
      } else {
        activeFilter = f;
        document.querySelectorAll('.stat-filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
      }
      renderTable(applyFilter(currentResults));
    });
  });

  // Auto-select first keyword
  if (firstKwBtn) firstKwBtn.click();

  // Compare
  setupCompare();

  // Upload modal
  setupUploadModal();

  // Fetch modal
  setupFetchModal();
});

// -----------------------------------------------------------------------
// Load results for active keyword + week
// -----------------------------------------------------------------------
function loadResults() {
  if (!activeKeywordId || !activeWeekId) return;

  const params = new URLSearchParams({ keyword_id: activeKeywordId, week_id: activeWeekId });

  Promise.all([
    fetch('/api/results?' + params).then(r => r.json()),
    fetch('/api/stats?'   + params).then(r => r.json()),
  ]).then(([results, stats]) => {
    currentResults = results;
    // Clear active filter when switching keywords/weeks
    activeFilter = null;
    document.querySelectorAll('.stat-filter-btn').forEach(b => b.classList.remove('active'));
    renderTable(applyFilter(currentResults));
    renderStats(stats);
  });
}

// -----------------------------------------------------------------------
// Render stats row
// -----------------------------------------------------------------------
function renderStats(stats) {
  setText('stat-total',    stats.total    ?? '—');
  setText('stat-positive', stats.positive ?? '—');   // green = positive + neutral
  setText('stat-negative', stats.negative ?? '—');   // red = negative only
  setText('stat-new',      stats.new      ?? '—');
  setText('stat-owned',    stats.owned    ?? '—');   // team-owned assets
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// -----------------------------------------------------------------------
// Filter helper
// -----------------------------------------------------------------------
function applyFilter(results) {
  if (!activeFilter) return results;
  return results.filter(r => {
    if (activeFilter === 'green')  return r.sentiment !== 'negative';
    if (activeFilter === 'red')    return r.sentiment === 'negative';
    if (activeFilter === 'new')    return r.movement === 'new';
    if (activeFilter === 'owned')  return r.owned === true;
    return true;
  });
}

// -----------------------------------------------------------------------
// Render table
// -----------------------------------------------------------------------
function renderTable(results) {
  const table  = document.getElementById('results-table');
  const tbody  = document.getElementById('results-body');
  const empty  = document.getElementById('empty-state');

  table.style.display = results.length ? 'table' : 'none';
  empty.style.display = results.length ? 'none'  : 'block';

  tbody.innerHTML = '';

  let lastPage = 0;
  let resultCount = 0;
  results.forEach(r => {
    resultCount++;
    const page = Math.ceil(resultCount / 10);
    if (page !== lastPage) {
      lastPage = page;
      const sep = document.createElement('tr');
      sep.className = 'page-separator';
      sep.innerHTML = `<td colspan="4">Page ${page}</td>`;
      tbody.appendChild(sep);
    }

    const tr = document.createElement('tr');
    tr.dataset.url = r.url;

    // neutral + positive = green, negative = red
    if (r.sentiment === 'negative') tr.classList.add('row-negative');
    else                            tr.classList.add('row-neutral');   // covers neutral + positive
    if (r.is_duplicate)             tr.classList.add('row-duplicate');

    tr.innerHTML = `
      <td class="col-pos">${r.position}</td>
      <td class="col-sentiment">${renderSentimentBtn(r.url, r.sentiment)}</td>
      <td class="col-movement">${renderMovement(r)}</td>
      <td class="col-url">
        <span class="url-title">
          <a class="url-link url-link-primary${r.owned ? ' url-owned' : ''}" href="${escHtml(r.url)}" target="_blank" rel="noopener">${escHtml(r.url)}</a>
          ${renderOwnedBtn(r.url, r.owned)}
        </span>
        <span class="url-subtitle">${escHtml(r.title || '(no title)')}</span>
        ${r.snippet ? `<span class="url-snippet">${escHtml(r.snippet)}</span>` : ''}
      </td>
    `;

    tr.querySelector('.sentiment-btn').addEventListener('click', function(e) {
      e.stopPropagation();
      cycleSentiment(this, tr);
    });

    tr.querySelector('.owned-btn').addEventListener('click', function(e) {
      e.stopPropagation();
      toggleOwned(this);
    });

    tbody.appendChild(tr);
  });
}

function renderOwnedBtn(url, owned) {
  const cls   = owned ? 'is-owned' : '';
  const icon  = owned ? '&#9733;' : '&#9734;';
  const title = owned ? 'Our Asset — click to remove' : 'Mark as Our Asset';
  return `<button class="owned-btn ${cls}" title="${title}" data-url="${escHtml(url)}">${icon}</button>`;
}

function toggleOwned(btn) {
  const isOwned = btn.classList.contains('is-owned');
  const next    = !isOwned;

  btn.classList.toggle('is-owned', next);
  btn.innerHTML = next ? '&#9733;' : '&#9734;';
  btn.title     = next ? 'Our Asset — click to remove' : 'Mark as Our Asset';

  // Color the URL link to match the star when owned
  const link = btn.closest('.url-title')?.querySelector('.url-link-primary');
  if (link) link.classList.toggle('url-owned', next);

  // Update the "Our Assets" stat card immediately
  const statEl = document.getElementById('stat-owned');
  if (statEl && statEl.textContent !== '—') {
    statEl.textContent = Math.max(0, parseInt(statEl.textContent, 10) + (next ? 1 : -1));
  }

  fetch('/api/tag', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ url: btn.dataset.url, owned: next }),
  });
}

function renderSentimentBtn(url, sentiment) {
  // Green = positive or neutral, Red = negative
  const isRed = sentiment === 'negative';
  const cls   = isRed ? 'negative' : 'positive';
  const icon  = isRed ? '✕' : '✓';
  return `<button class="sentiment-btn ${cls}" title="Click to toggle: green ↔ red" data-url="${escHtml(url)}">${icon}</button>`;
}

function cycleSentiment(btn, tr) {
  // Simple 2-state toggle: green (positive) ↔ red (negative)
  const isNowRed = btn.classList.contains('negative');
  const next     = isNowRed ? 'positive' : 'negative';

  btn.classList.remove('positive', 'negative');
  btn.classList.add(next);
  btn.textContent = next === 'negative' ? '✕' : '✓';

  tr.classList.remove('row-positive', 'row-negative', 'row-neutral');
  if (next === 'negative') tr.classList.add('row-negative');
  else                     tr.classList.add('row-neutral');

  fetch('/api/tag', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ url: btn.dataset.url, sentiment: next }),
  });
}

function renderMovement(r) {
  const movement = typeof r === 'string' ? r : r.movement;
  if (!movement) return '';
  if (movement === 'new')       return `<span class="mv mv-new">NEW</span>`;
  if (movement === 'duplicate') return `<span class="mv mv-duplicate">DUPE</span>`;
  if (movement === 'no_change') return `<span class="mv mv-nochange">—</span>`;

  if (movement === 'returned') {
    const pos  = r.last_seen_pos;
    const date = r.last_seen_date ? fmtDate(r.last_seen_date) : '';
    const tip  = `Last seen at #${pos} on ${r.last_seen_date}`;
    return `<span class="mv mv-returned" title="${tip}">↩ #${pos}${date ? ' · ' + date : ''}</span>`;
  }

  const upMatch   = movement.match(/^up_(\d+)$/);
  const downMatch = movement.match(/^down_(\d+)$/);
  if (upMatch)   return `<span class="mv mv-up">▲ ${upMatch[1]}</span>`;
  if (downMatch) return `<span class="mv mv-down">▼ ${downMatch[1]}</span>`;
  return `<span class="mv mv-nochange">${escHtml(movement)}</span>`;
}

function fmtDate(dateStr) {
  const d = new Date(dateStr + 'T00:00:00');
  const includeYear = d.getFullYear() < new Date().getFullYear();
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    ...(includeYear ? { year: 'numeric' } : {}),
  });
}


// -----------------------------------------------------------------------
// Compare feature
// -----------------------------------------------------------------------
function setupCompare() {
  const toggleBtn  = document.getElementById('compare-toggle-btn');
  const overlay    = document.getElementById('compare-overlay');
  const closeBtn   = document.getElementById('compare-close');
  const compareBtn = document.getElementById('compare-btn');
  const selectA    = document.getElementById('compare-week-a');
  const selectB    = document.getElementById('compare-week-b');
  const kwLabel    = document.getElementById('compare-kw-label');

  function populateWeekSelects() {
    const opts = [...document.getElementById('week-select').options];
    [selectA, selectB].forEach((sel, idx) => {
      sel.innerHTML = '';
      opts.forEach(o => {
        const opt = document.createElement('option');
        opt.value = o.value;
        opt.textContent = o.text;
        sel.appendChild(opt);
      });
      // Default: A = second-latest, B = latest
      if (opts.length > 1) sel.selectedIndex = idx === 0 ? 1 : 0;
    });
  }

  function openModal() {
    populateWeekSelects();
    kwLabel.textContent = activeKeywordName || '';
    document.getElementById('compare-empty').style.display = 'block';
    document.getElementById('compare-table').style.display = 'none';
    overlay.classList.remove('hidden');
    toggleBtn.classList.add('active');
  }

  function closeModal() {
    overlay.classList.add('hidden');
    toggleBtn.classList.remove('active');
  }

  toggleBtn.addEventListener('click', () => {
    overlay.classList.contains('hidden') ? openModal() : closeModal();
  });

  closeBtn.addEventListener('click', closeModal);
  overlay.addEventListener('click', e => { if (e.target === overlay) closeModal(); });

  compareBtn.addEventListener('click', () => {
    if (!activeKeywordId) return;
    const weekA = selectA.value;
    const weekB = selectB.value;
    if (weekA === weekB) { alert('Please select two different weeks.'); return; }

    compareBtn.textContent = 'Loading…';
    compareBtn.disabled = true;
    fetch(`/api/compare?keyword_id=${activeKeywordId}&week_a=${weekA}&week_b=${weekB}`)
      .then(r => r.json())
      .then(data => {
        renderCompare(data);
        compareBtn.textContent = 'Compare';
        compareBtn.disabled = false;
      });
  });
}

function renderCompare(data) {
  const table   = document.getElementById('compare-table');
  const tbody   = document.getElementById('compare-body');
  const empty   = document.getElementById('compare-empty');
  empty.style.display = 'none';

  document.getElementById('compare-head-a').textContent = data.week_a;
  document.getElementById('compare-head-b').textContent = data.week_b;

  tbody.innerHTML = '';
  let lastComparePage = 0;
  data.rows.forEach(r => {
    if (r.pos_b != null) {
      const page = Math.ceil(r.pos_b / 10);
      if (page !== lastComparePage) {
        lastComparePage = page;
        const sep = document.createElement('tr');
        sep.className = 'page-separator';
        sep.innerHTML = `<td colspan="5">Page ${page}</td>`;
        tbody.appendChild(sep);
      }
    }

    const tr = document.createElement('tr');
    if (r.sentiment === 'negative') tr.classList.add('row-negative');
    else tr.classList.add('row-neutral');
    if (!r.pos_b) tr.classList.add('row-dropped');

    const posA   = r.pos_a != null ? `#${r.pos_a}` : `<span class="pos-null">not ranked</span>`;
    const posB   = r.pos_b != null ? `#${r.pos_b}` : `<span class="pos-null">not ranked</span>`;
    const change = r.diff == null  ? '<span class="mv mv-new">NEW</span>'
                 : r.diff === 0    ? '<span class="mv mv-nochange">—</span>'
                 : r.diff > 0      ? `<span class="mv mv-up">▲ ${r.diff}</span>`
                 :                   `<span class="mv mv-down">▼ ${Math.abs(r.diff)}</span>`;

    tr.innerHTML = `
      <td class="col-sentiment">${renderSentimentBtn(r.url, r.sentiment)}</td>
      <td class="col-url">
        <span class="url-title">
          <a class="url-link url-link-primary" href="${escHtml(r.url)}" target="_blank" rel="noopener">${escHtml(r.url)}</a>
          ${renderOwnedBtn(r.url, r.owned)}
        </span>
        <span class="url-subtitle">${escHtml(r.title || '(no title)')}</span>
      </td>
      <td class="col-pos-num">${posA}</td>
      <td class="col-pos-num">${posB}</td>
      <td class="col-movement">${change}</td>
    `;

    tr.querySelector('.sentiment-btn').addEventListener('click', function(e) {
      e.stopPropagation();
      cycleSentiment(this, tr);
    });

    tr.querySelector('.owned-btn').addEventListener('click', function(e) {
      e.stopPropagation();
      toggleOwned(this);
    });

    tbody.appendChild(tr);
  });

  table.style.display = 'table';
}


// -----------------------------------------------------------------------
// Upload modal
// -----------------------------------------------------------------------
const KNOWN_STEMS = ['frank','melaleuca.com','melaleuca','products','reviews','riverbend ranch','the wellness company'];

function setupUploadModal() {
  const overlay   = document.getElementById('modal-overlay');
  const openBtn   = document.getElementById('upload-btn');
  const closeBtn  = document.getElementById('modal-close');
  const dropZone  = document.getElementById('drop-zone');
  const fileInput = document.getElementById('file-input');
  const submitBtn = document.getElementById('upload-submit');
  const fileList  = document.getElementById('file-list');
  const status    = document.getElementById('upload-status');
  let selectedFiles = [];

  openBtn.addEventListener('click', () => {
    overlay.classList.remove('hidden');
    status.className = '';
    status.style.display = 'none';
  });
  closeBtn.addEventListener('click',  () => overlay.classList.add('hidden'));
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.classList.add('hidden'); });

  dropZone.addEventListener('click', () => fileInput.click());
  dropZone.addEventListener('dragover',  e => { e.preventDefault(); dropZone.classList.add('dragover'); });
  dropZone.addEventListener('dragleave', ()  => dropZone.classList.remove('dragover'));
  dropZone.addEventListener('drop', e => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    handleFiles([...e.dataTransfer.files]);
  });
  fileInput.addEventListener('change', () => handleFiles([...fileInput.files]));

  function handleFiles(files) {
    selectedFiles = files;
    fileList.innerHTML = '';
    files.forEach(f => {
      const stem    = f.name.replace(/\.csv$/i, '').toLowerCase();
      const known   = KNOWN_STEMS.includes(stem);
      const li      = document.createElement('li');
      li.innerHTML  = `<span class="${known ? 'check' : 'skip'}">${known ? '✓' : '?'}</span> ${escHtml(f.name)}${known ? '' : ' <em>(unrecognized)</em>'}`;
      fileList.appendChild(li);
    });
  }

  submitBtn.addEventListener('click', () => {
    const weekDate = document.getElementById('upload-date').value;
    if (!weekDate) { showStatus('error', 'Please select a week date.'); return; }
    if (!selectedFiles.length) { showStatus('error', 'Please select CSV files.'); return; }

    submitBtn.disabled = true;
    submitBtn.textContent = 'Importing...';

    const fd = new FormData();
    fd.append('week_date', weekDate);
    selectedFiles.forEach(f => fd.append('csvfiles', f));

    fetch('/api/upload', { method: 'POST', body: fd })
      .then(r => r.json())
      .then(data => {
        if (data.ok) {
          showStatus('success', `Successfully imported ${data.imported} results. Refreshing...`);
          setTimeout(() => {
            overlay.classList.add('hidden');
            // Reload page to get new week in dropdown
            window.location.reload();
          }, 1800);
        } else {
          showStatus('error', data.error || 'Import failed.');
        }
      })
      .catch(() => showStatus('error', 'Network error.'))
      .finally(() => {
        submitBtn.disabled = false;
        submitBtn.textContent = 'Import Week';
      });
  });

  function showStatus(type, msg) {
    status.className     = type;
    status.textContent   = msg;
    status.style.display = 'block';
  }
}

// -----------------------------------------------------------------------
// Fetch Live Data modal
// -----------------------------------------------------------------------
function setupFetchModal() {
  const overlay   = document.getElementById('fetch-modal-overlay');
  const openBtn   = document.getElementById('fetch-btn');
  const closeBtn  = document.getElementById('fetch-modal-close');
  const submitBtn = document.getElementById('fetch-submit');
  const statusEl  = document.getElementById('fetch-status');
  const logEl     = document.getElementById('fetch-log');
  const dateInput = document.getElementById('fetch-date');

  // Default date to today
  dateInput.value = new Date().toISOString().slice(0, 10);

  openBtn.addEventListener('click', () => {
    overlay.classList.remove('hidden');
    logEl.innerHTML   = '';
    statusEl.className = '';
    statusEl.style.display = 'none';
    submitBtn.disabled = false;
    submitBtn.textContent = 'Fetch Now';

    // Warn if API key not configured
    fetch('/api/fetch_status')
      .then(r => r.json())
      .then(d => {
        if (!d.configured) {
          showFetchStatus('error', 'SCALESERP_API_KEY is not set. Add it as an environment variable.');
          submitBtn.disabled = true;
        }
      });
  });

  closeBtn.addEventListener('click',  () => overlay.classList.add('hidden'));
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.classList.add('hidden'); });

  submitBtn.addEventListener('click', () => {
    const weekDate = dateInput.value;
    if (!weekDate) { showFetchStatus('error', 'Please select a week date.'); return; }

    submitBtn.disabled    = true;
    submitBtn.textContent = 'Fetching…';
    logEl.innerHTML       = '';
    statusEl.style.display = 'none';

    fetch('/api/fetch', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ week_date: weekDate }),
    })
      .then(r => r.json())
      .then(data => {
        if (data.ok) {
          // Show per-keyword results
          Object.entries(data.results).forEach(([kw, v]) => {
            const li = document.createElement('div');
            li.className = 'fetch-log-row';
            if (v.error) {
              li.innerHTML = `<span class="skip">✕</span> ${escHtml(kw)} — <em>${escHtml(v.error)}</em>`;
            } else {
              li.innerHTML = `<span class="check">✓</span> ${escHtml(kw)} — ${v.count} results`;
            }
            logEl.appendChild(li);
          });

          const hasErrors = Object.values(data.results).some(v => v.error);
          if (hasErrors) {
            showFetchStatus('error', `Fetched ${data.imported} results (some keywords failed — see above).`);
          } else {
            showFetchStatus('success', `Successfully fetched ${data.imported} results. Refreshing…`);
            setTimeout(() => { overlay.classList.add('hidden'); window.location.reload(); }, 1800);
          }
        } else {
          showFetchStatus('error', data.error || 'Fetch failed.');
        }
      })
      .catch(() => showFetchStatus('error', 'Network error.'))
      .finally(() => {
        submitBtn.disabled    = false;
        submitBtn.textContent = 'Fetch Now';
      });
  });

  function showFetchStatus(type, msg) {
    statusEl.className    = type;
    statusEl.textContent  = msg;
    statusEl.style.display = 'block';
  }
}


// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------
function escHtml(str) {
  return String(str || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '…' : str;
}
