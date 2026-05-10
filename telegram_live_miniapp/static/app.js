const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
  try {
    tg.setHeaderColor('#111c24');
    tg.setBackgroundColor('#080f14');
  } catch (_) {}
}

const app = document.getElementById('app');

let state = {
  live: null,
  detail: null,
  filter: 'all',
  selectedId: null,
  loading: true,
};

const FAVORITES_KEY = 'live-miniapp-favorites-v1';

function getFavorites() {
  try {
    return new Set(JSON.parse(localStorage.getItem(FAVORITES_KEY) || '[]'));
  } catch (_) {
    return new Set();
  }
}

function saveFavorites(set) {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify([...set]));
}

function isFav(id) {
  return getFavorites().has(String(id));
}

function toggleFav(id) {
  const fav = getFavorites();
  id = String(id);
  if (fav.has(id)) fav.delete(id);
  else fav.add(id);
  saveFavorites(fav);
  render();
}

function escapeHtml(v) {
  return String(v ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[ch]));
}

function iconBack() {
  return `<svg viewBox="0 0 24 24" fill="none"><path d="M15 18l-6-6 6-6" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}

function iconFilter() {
  return `<svg viewBox="0 0 24 24" fill="none"><path d="M4 6h16l-6 7v5l-4 2v-7L4 6z" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/></svg>`;
}

function iconStar(fill = false) {
  return `<svg viewBox="0 0 24 24" fill="${fill ? 'currentColor' : 'none'}"><path d="M12 3.7l2.45 4.96 5.48.8-3.96 3.86.94 5.46L12 16.2l-4.9 2.58.93-5.46-3.96-3.86 5.48-.8L12 3.7z" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/></svg>`;
}

function flattenLive(live) {
  const out = [];
  for (const country of live?.countries || []) {
    for (const league of country.leagues || []) {
      for (const match of league.matches || []) out.push(match);
    }
  }
  return out;
}

function groupMatches(matches) {
  const countries = new Map();
  for (const m of matches) {
    const ckey = m.country || 'Без страны';
    const lkey = m.league || 'Без лиги';
    if (!countries.has(ckey)) {
      countries.set(ckey, { country: ckey, country_code: m.country_code || '', leagues: new Map(), match_count: 0 });
    }
    const c = countries.get(ckey);
    if (!c.leagues.has(lkey)) c.leagues.set(lkey, { league: lkey, matches: [], match_count: 0 });
    const l = c.leagues.get(lkey);
    l.matches.push(m);
    l.match_count++;
    c.match_count++;
  }
  return [...countries.values()].map(c => ({
    ...c,
    leagues: [...c.leagues.values()],
  }));
}

function filteredMatches() {
  const fav = getFavorites();
  let matches = flattenLive(state.live);
  if (state.filter === 'fav') matches = matches.filter(m => fav.has(String(m.id)));
  if (state.filter === '1t') matches = matches.filter(m => Number(m.minute || 0) <= 45);
  if (state.filter === '2t') matches = matches.filter(m => Number(m.minute || 0) > 45);
  if (state.filter === '00') matches = matches.filter(m => Number(m.score_home) === 0 && Number(m.score_away) === 0);
  return matches;
}

async function loadLive(force = false) {
  try {
    const res = await fetch(`/api/live${force ? '?force=1' : ''}`, { cache: 'no-store' });
    state.live = await res.json();
    state.loading = false;
    if (!state.selectedId) render();
  } catch (err) {
    state.loading = false;
    state.live = { ok: false, total: 0, countries: [], error: String(err) };
    render();
  }
}

async function loadDetail(id) {
  state.selectedId = String(id);
  state.detail = null;
  render();
  try {
    const res = await fetch(`/api/match?id=${encodeURIComponent(id)}`, { cache: 'no-store' });
    state.detail = await res.json();
    render();
  } catch (err) {
    state.detail = { ok: false, error: String(err) };
    render();
  }
}

function topbar(title, subtitle, back = false) {
  return `
    <div class="topbar">
      <button class="icon-btn" onclick="${back ? 'goBack()' : 'closeApp()'}" aria-label="${back ? 'Назад' : 'Закрыть'}">
        ${back ? iconBack() : iconBack()}
      </button>
      <div class="header-title">
        <h1>${escapeHtml(title)}</h1>
        <p>${escapeHtml(subtitle || '')}</p>
      </div>
      ${back ? '' : `<button class="icon-btn" onclick="loadLive(true)" aria-label="Фильтр">${iconFilter()}</button>`}
    </div>
  `;
}

function filterButton(id, label, extra = '') {
  const active = state.filter === id ? ' active' : '';
  return `<button class="pill${active}" onclick="setFilter('${id}')">${extra}${label}</button>`;
}

function renderList() {
  const total = state.live?.total || 0;
  const favCount = getFavorites().size;
  const matches = filteredMatches();
  const grouped = groupMatches(matches);
  let html = topbar('Live матчи', 'Только идущие сейчас');
  html += `
    <div class="filters">
      ${filterButton('all', `Все ${total}`)}
      ${filterButton('fav', `${favCount}`, '<span class="star">★</span>')}
      ${filterButton('1t', '1T')}
      ${filterButton('2t', '2T')}
      ${filterButton('00', '0:0')}
    </div>
  `;

  if (state.live?.error) {
    html += `<div class="error-banner">Источник: ${escapeHtml(state.live.source || 'demo')}. ${escapeHtml(state.live.error)}</div>`;
  }

  if (!matches.length) {
    html += `<div class="empty">Матчей по этому фильтру нет</div>`;
  } else {
    for (const country of grouped) {
      for (const league of country.leagues) {
        html += `<div class="section">
          <div class="league-title">${escapeHtml(country.country_code || '')} ${escapeHtml(country.country)} · ${escapeHtml(league.league)}</div>
          ${league.matches.map(renderMatchRow).join('')}
        </div>`;
      }
    }
  }

  html += `<div class="footer-note">Главный экран · список live-матчей</div>`;
  app.innerHTML = html;
}

function renderMatchRow(m) {
  const fav = isFav(m.id);
  return `
    <div class="match-row" onclick="loadDetail('${escapeHtml(m.id)}')">
      <div class="match-minute">${escapeHtml(m.minute_text || 'LIVE')}<small>${escapeHtml(m.period || '')}</small></div>
      <div class="teams">
        <div class="team-line">${escapeHtml(m.home)}</div>
        <div class="team-line">${escapeHtml(m.away)}</div>
      </div>
      <div class="score-col">
        <div>${Number(m.score_home ?? 0)}</div>
        <div>${Number(m.score_away ?? 0)}</div>
      </div>
      <button class="star-btn ${fav ? 'active' : ''}" onclick="event.stopPropagation(); toggleFav('${escapeHtml(m.id)}')">${iconStar(fav)}</button>
    </div>
  `;
}

function valuePair(stats, key, fallbackHome = 0, fallbackAway = 0) {
  const row = stats?.[key] || {};
  return {
    home: Number(row.home ?? fallbackHome),
    away: Number(row.away ?? fallbackAway),
  };
}

function statRow(stats, key, label, cls = '') {
  const v = valuePair(stats, key);
  const total = Math.max(1, v.home + v.away);
  const homePct = Math.max(0, Math.min(100, Math.round(v.home / total * 100)));
  const homeLabel = key === 'possession' ? `${v.home}%` : v.home;
  const awayLabel = key === 'possession' ? `${v.away}%` : v.away;
  return `
    <div class="stat-row ${cls}">
      <div class="stat-values">
        <div>${homeLabel}</div>
        <div class="bar"><span style="width:${homePct}%"></span><span style="width:${100-homePct}%"></span></div>
        <div style="text-align:right">${awayLabel}</div>
      </div>
      <div class="stat-label">${escapeHtml(label)}</div>
    </div>
  `;
}

function formatAvg(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return '—';
  return Number(v).toFixed(1);
}

function renderDetail() {
  if (!state.detail) {
    app.innerHTML = `<div class="detail">${topbar('Загрузка…', '', true)}<div class="loading-screen"><div class="loader"></div><div>Открываю карточку матча…</div></div></div>`;
    return;
  }

  if (!state.detail.ok) {
    app.innerHTML = `<div class="detail">${topbar('Ошибка', '', true)}<div class="empty">${escapeHtml(state.detail.error || 'Не удалось загрузить матч')}</div></div>`;
    return;
  }

  const m = state.detail.match;
  const stats = state.detail.stats || {};
  const avg = state.detail.avg || {};
  const fav = isFav(m.id);

  app.innerHTML = `
    <div class="detail">
      <div class="topbar">
        <button class="icon-btn" onclick="goBack()" aria-label="Назад">${iconBack()}</button>
        <div class="header-title">
          <div class="breadcrumb">${escapeHtml(m.country_code || '')} ${escapeHtml(m.country)} · ${escapeHtml(m.league)}</div>
        </div>
      </div>

      <div class="scoreboard">
        <div class="score-grid">
          <div class="score-team">${escapeHtml(m.home)}</div>
          <div class="big-score">${Number(m.score_home ?? 0)} : ${Number(m.score_away ?? 0)}</div>
          <div class="score-team">${escapeHtml(m.away)}</div>
        </div>
        <div class="live-pill"><span class="live-dot"></span>${escapeHtml(m.minute_text || 'LIVE')} · ${escapeHtml(m.period || '')}</div>
      </div>

      <div class="stats">
        <div class="block-title">Владение мячом</div>
        ${statRow(stats, 'possession', '', 'possession')}
        <div class="block-title">Статистика матча</div>
        ${statRow(stats, 'shots', 'Удары')}
        ${statRow(stats, 'on_target', 'В створ')}
        ${statRow(stats, 'dangerous', 'Опасные атаки', 'dangerous')}
        ${statRow(stats, 'corners', 'Угловые', 'corners')}
      </div>

      <div class="avg-wrap">
        <div class="avg-title">Средний тотал · 10 матчей</div>
        <div class="avg-cards">
          <div class="avg-card">
            <div class="name">${escapeHtml(m.home)}</div>
            <div class="num">${formatAvg(avg.home?.avg)}</div>
            <div class="meta">0:0 → ${Number(avg.home?.zero_zero || 0)}/${Number(avg.home?.count || 10)}</div>
          </div>
          <div class="avg-card">
            <div class="name">${escapeHtml(m.away)}</div>
            <div class="num">${formatAvg(avg.away?.avg)}</div>
            <div class="meta">0:0 → ${Number(avg.away?.zero_zero || 0)}/${Number(avg.away?.count || 10)}</div>
          </div>
        </div>
      </div>

      <div class="actions">
        <button class="primary-btn" onclick="toggleFav('${escapeHtml(m.id)}')">${fav ? '★ В избранном' : '☆ В избранное'}</button>
        <button class="secondary-btn" onclick="openIgscore('${escapeHtml(m.link || '')}')">↗ IGScore</button>
      </div>

      <div class="footer-note">Карточка матча · live-статистика</div>
    </div>
  `;
}

function setFilter(id) {
  state.filter = id;
  render();
}

function goBack() {
  state.selectedId = null;
  state.detail = null;
  if (tg?.BackButton) tg.BackButton.hide();
  render();
}

function openIgscore(link) {
  if (!link) return;
  if (tg?.openLink) tg.openLink(link);
  else window.open(link, '_blank', 'noopener');
}

function render() {
  if (state.selectedId) {
    if (tg?.BackButton) {
      tg.BackButton.show();
      tg.BackButton.onClick(goBack);
    }
    renderDetail();
    return;
  }
  if (tg?.BackButton) tg.BackButton.hide();

  if (state.loading) {
    app.innerHTML = `<div class="loading-screen"><div class="loader"></div><div>Загрузка live матчей…</div></div>`;
    return;
  }
  renderList();
}

loadLive();
setInterval(() => {
  if (!state.selectedId) loadLive(false);
}, 15000);

window.setFilter = setFilter;
window.loadLive = loadLive;
window.loadDetail = loadDetail;
window.goBack = goBack;
window.toggleFav = toggleFav;
function closeApp() {
  if (tg?.close) tg.close();
  else if (history.length > 1) history.back();
}

window.openIgscore = openIgscore;
window.closeApp = closeApp;
