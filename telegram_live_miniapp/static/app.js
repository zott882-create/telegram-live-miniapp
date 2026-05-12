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
  showFilters: false,
  country: 'all',
  league: 'all',
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

function getAllMatches() {
  return flattenLive(state.live);
}

function getAvailableCountries() {
  const map = new Map();
  for (const m of getAllMatches()) {
    const key = m.country || 'Без страны';
    if (!map.has(key)) map.set(key, { name: key, code: m.country_code || '', count: 0 });
    map.get(key).count += 1;
  }
  return [...map.values()].sort((a, b) => a.name.localeCompare(b.name, 'ru'));
}

function getAvailableLeagues(country = state.country) {
  const map = new Map();
  for (const m of getAllMatches()) {
    if (country !== 'all' && (m.country || 'Без страны') !== country) continue;
    const key = m.league || 'Без лиги';
    if (!map.has(key)) map.set(key, { name: key, count: 0 });
    map.get(key).count += 1;
  }
  return [...map.values()].sort((a, b) => a.name.localeCompare(b.name, 'ru'));
}

function filteredMatches() {
  const fav = getFavorites();
  let matches = getAllMatches();

  if (state.country !== 'all') matches = matches.filter(m => (m.country || 'Без страны') === state.country);
  if (state.league !== 'all') matches = matches.filter(m => (m.league || 'Без лиги') === state.league);

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
    const currentLeagues = new Set(getAvailableLeagues(state.country).map(x => x.name));
    if (state.league !== 'all' && !currentLeagues.has(state.league)) state.league = 'all';
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
  state.showFilters = false;
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
        ${iconBack()}
      </button>
      <div class="header-title">
        <h1>${escapeHtml(title)}</h1>
        <p>${escapeHtml(subtitle || '')}</p>
      </div>
      ${back ? '' : `<button class="icon-btn" onclick="openFilters()" aria-label="Фильтры">${iconFilter()}</button>`}
    </div>
  `;
}

function filterButton(id, label, extra = '') {
  const active = state.filter === id ? ' active' : '';
  return `<button class="pill${active}" onclick="setFilter('${id}')">${extra}${label}</button>`;
}

function initials(name) {
  const clean = String(name || '').trim();
  if (!clean) return 'FC';
  const parts = clean.replace(/[^\p{L}\p{N} ]/gu, ' ').split(/\s+/).filter(Boolean);
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[1][0]).toUpperCase();
}

function colorFromString(text) {
  const palette = ['#2a9d8f', '#3366ff', '#d97706', '#7c3aed', '#ef4444', '#0ea5e9', '#22c55e', '#db2777'];
  let hash = 0;
  const s = String(text || 'x');
  for (let i = 0; i < s.length; i += 1) hash = (hash * 31 + s.charCodeAt(i)) >>> 0;
  return palette[hash % palette.length];
}

function renderClubBadge(name, logo = '') {
  if (logo) return `<span class="club-badge is-image"><img src="${escapeHtml(logo)}" alt="${escapeHtml(name)}"></span>`;
  return `<span class="club-badge" style="background:${colorFromString(name)}">${escapeHtml(initials(name))}</span>`;
}

function flagUrl(code) {
  const cc = String(code || '').trim().toLowerCase();
  if (!cc || cc.length !== 2) return '';
  return `https://flagcdn.com/w40/${cc}.png`;
}

function renderCountryFlag(code, label = '') {
  const url = flagUrl(code);
  if (!url) return `<span class="country-flag-fallback">${escapeHtml((label || '').slice(0, 2).toUpperCase())}</span>`;
  return `<span class="country-flag-wrap"><img class="country-flag" src="${url}" alt="${escapeHtml(label || code)}"></span>`;
}

function activeFilterChips() {
  const chips = [];
  if (state.country !== 'all') chips.push(`<button class="active-chip" onclick="setCountry('all')">Страна: ${escapeHtml(state.country)} ✕</button>`);
  if (state.league !== 'all') chips.push(`<button class="active-chip" onclick="setLeague('all')">Лига: ${escapeHtml(state.league)} ✕</button>`);
  return chips.join('');
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

  const activeChips = activeFilterChips();
  if (activeChips) html += `<div class="active-filters">${activeChips}<button class="clear-link" onclick="clearAdvancedFilters()">Сбросить</button></div>`;

  if (state.live?.error) html += `<div class="error-banner">Источник: ${escapeHtml(state.live.source || 'demo')}. ${escapeHtml(state.live.error)}</div>`;

  if (!matches.length) {
    html += `<div class="empty">Матчей по этому фильтру нет</div>`;
  } else {
    for (const country of grouped) {
      for (const league of country.leagues) {
        html += `<div class="section">
          <div class="league-title-row">
            <div class="league-title-left">
              ${renderCountryFlag(country.country_code, country.country)}
              <div class="league-title-text">
                <div class="league-country">${escapeHtml(country.country)}</div>
                <div class="league-title">${escapeHtml(league.league)}</div>
              </div>
            </div>
            <div class="league-count">${league.matches.length}</div>
          </div>
          ${league.matches.map(renderMatchRow).join('')}
        </div>`;
      }
    }
  }

  html += `<div class="footer-note">Главный экран · список live-матчей</div>`;
  if (state.showFilters) html += renderFiltersScreen();
  app.innerHTML = html;
}

function renderMatchRow(m) {
  const fav = isFav(m.id);
  return `
    <div class="match-row" onclick="loadDetail('${escapeHtml(m.id)}')">
      <div class="match-minute">${escapeHtml(m.minute_text || 'LIVE')}<small>${escapeHtml(m.period || '')}</small></div>
      <div class="teams">
        <div class="team-line with-badge">${renderClubBadge(m.home, m.home_logo)}<span class="team-name">${escapeHtml(m.home)}</span></div>
        <div class="team-line with-badge">${renderClubBadge(m.away, m.away_logo)}<span class="team-name">${escapeHtml(m.away)}</span></div>
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
  return { home: Number(row.home ?? fallbackHome), away: Number(row.away ?? fallbackAway) };
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
          <div class="breadcrumb">${renderCountryFlag(m.country_code, m.country)} ${escapeHtml(m.country)} · ${escapeHtml(m.league)}</div>
          <h1>Матч live</h1>
        </div>
      </div>

      <div class="scoreboard">
        <div class="score-grid enhanced">
          <div class="score-side">${renderClubBadge(m.home, m.home_logo)}<div class="score-team">${escapeHtml(m.home)}</div></div>
          <div class="score-center"><div class="big-score">${Number(m.score_home ?? 0)} : ${Number(m.score_away ?? 0)}</div><div class="live-pill"><span class="live-dot"></span>${escapeHtml(m.minute_text || 'LIVE')} · ${escapeHtml(m.period || '')}</div></div>
          <div class="score-side">${renderClubBadge(m.away, m.away_logo)}<div class="score-team">${escapeHtml(m.away)}</div></div>
        </div>
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
          <div class="avg-card"><div class="name with-badge mini">${renderClubBadge(m.home, m.home_logo)} <span>${escapeHtml(m.home)}</span></div><div class="num">${formatAvg(avg.home?.avg)}</div><div class="meta">0:0 → ${Number(avg.home?.zero_zero || 0)}/${Number(avg.home?.count || 10)}</div></div>
          <div class="avg-card"><div class="name with-badge mini">${renderClubBadge(m.away, m.away_logo)} <span>${escapeHtml(m.away)}</span></div><div class="num">${formatAvg(avg.away?.avg)}</div><div class="meta">0:0 → ${Number(avg.away?.zero_zero || 0)}/${Number(avg.away?.count || 10)}</div></div>
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

function renderChoiceChip(active, label, onClick) {
  return `<button class="sheet-chip${active ? ' active' : ''}" onclick="${onClick}">${label}</button>`;
}

function renderFiltersScreen() {
  const countries = getAvailableCountries();
  const leagues = getAvailableLeagues(state.country);
  return `
    <div class="filters-overlay" onclick="closeFilters()">
      <div class="filters-sheet" onclick="event.stopPropagation()">
        <div class="sheet-head">
          <div>
            <div class="sheet-title">Фильтры</div>
            <div class="sheet-subtitle">Страны, лиги и быстрые отборы</div>
          </div>
          <button class="sheet-close" onclick="closeFilters()">✕</button>
        </div>

        <div class="sheet-block">
          <div class="sheet-label">Быстрые фильтры</div>
          <div class="sheet-chips">
            ${renderChoiceChip(state.filter === 'all', 'Все', `setFilter('all')`)}
            ${renderChoiceChip(state.filter === 'fav', '★ Избранное', `setFilter('fav')`)}
            ${renderChoiceChip(state.filter === '1t', '1 тайм', `setFilter('1t')`)}
            ${renderChoiceChip(state.filter === '2t', '2 тайм', `setFilter('2t')`)}
            ${renderChoiceChip(state.filter === '00', '0:0', `setFilter('00')`)}
          </div>
        </div>

        <div class="sheet-block">
          <div class="sheet-label">Страны</div>
          <div class="sheet-chips">
            ${renderChoiceChip(state.country === 'all', 'Все страны', `setCountry('all')`)}
            ${countries.map(item => renderChoiceChip(state.country === item.name, `${escapeHtml(item.name)} · ${item.count}`, `setCountry(${JSON.stringify(item.name)})`)).join('')}
          </div>
        </div>

        <div class="sheet-block">
          <div class="sheet-label">Лиги</div>
          <div class="sheet-chips">
            ${renderChoiceChip(state.league === 'all', 'Все лиги', `setLeague('all')`)}
            ${leagues.map(item => renderChoiceChip(state.league === item.name, `${escapeHtml(item.name)} · ${item.count}`, `setLeague(${JSON.stringify(item.name)})`)).join('')}
          </div>
        </div>

        <div class="sheet-actions">
          <button class="secondary-wide" onclick="clearAdvancedFilters()">Сбросить</button>
          <button class="primary-wide" onclick="closeFilters()">Применить</button>
        </div>
      </div>
    </div>
  `;
}

function setFilter(id) { state.filter = id; render(); }

function setCountry(country) {
  state.country = country;
  if (country === 'all') state.league = 'all';
  const available = new Set(getAvailableLeagues(country).map(x => x.name));
  if (state.league !== 'all' && !available.has(state.league)) state.league = 'all';
  render();
}

function setLeague(league) { state.league = league; render(); }
function openFilters() { state.showFilters = true; render(); }
function closeFilters() { state.showFilters = false; render(); }
function clearAdvancedFilters() { state.country = 'all'; state.league = 'all'; state.filter = 'all'; state.showFilters = false; render(); }

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

function closeApp() {
  if (tg?.close) tg.close();
  else if (history.length > 1) history.back();
}

function render() {
  if (!app) return;
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

window.addEventListener('error', (event) => {
  if (!app) return;
  app.innerHTML = `<div class="empty"><b>Ошибка интерфейса</b><br>${escapeHtml(event.message || 'Неизвестная ошибка')}</div>`;
});

loadLive();
setInterval(() => { if (!state.selectedId) loadLive(false); }, 15000);

window.setFilter = setFilter;
window.loadLive = loadLive;
window.loadDetail = loadDetail;
window.goBack = goBack;
window.toggleFav = toggleFav;
window.openFilters = openFilters;
window.closeFilters = closeFilters;
window.setCountry = setCountry;
window.setLeague = setLeague;
window.clearAdvancedFilters = clearAdvancedFilters;
window.openIgscore = openIgscore;
window.closeApp = closeApp;
