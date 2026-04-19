/**
 * app.js — Heatr shared JavaScript
 *
 * Loaded on every page (except index.html which loads it too but handles auth
 * differently). Provides:
 *   - Supabase client init (from window.HEATR_CONFIG)
 *   - requireAuth()
 *   - apiCall()
 *   - formatScore(), formatDate(), formatEuro(), formatRelativeTime()
 *   - showToast()
 *   - renderSidebar()
 *   - createSkeletonLoader()
 *   - debounce()
 */

// =============================================================================
// Supabase client
// =============================================================================

const _cfg = window.HEATR_CONFIG || {};
const supabase = window.supabase.createClient(
  _cfg.SUPABASE_URL || '',
  _cfg.SUPABASE_ANON_KEY || ''
);

// =============================================================================
// Auth
// =============================================================================

/**
 * Ensure the user is authenticated. Redirects to /index.html if not.
 * @returns {Promise<object>} Supabase session object
 */
async function requireAuth() {
  // Dev bypass — no Supabase Auth required for local development
  if (_cfg.SUPABASE_ANON_KEY === 'dev-mode') {
    sessionStorage.setItem('heatr_token', 'dev-token');
    return { user: { email: 'dev@aerys.nl' }, access_token: 'dev-token' };
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session) {
    window.location.href = '/index.html';
    return null;
  }
  sessionStorage.setItem('heatr_token', session.access_token);
  return session;
}

/**
 * Sign the user out, clear storage, redirect to login.
 */
async function signOut() {
  await supabase.auth.signOut();
  sessionStorage.removeItem('heatr_token');
  window.location.href = '/index.html';
}

// =============================================================================
// API helper
// =============================================================================

const API_BASE = _cfg.API_BASE || 'http://localhost:8000';

/**
 * Wrapper around fetch for Heatr FastAPI calls.
 * Automatically adds Authorization header from sessionStorage.
 *
 * @param {string} endpoint - Path like '/leads' or '/leads?sector=alternatieve_zorg'
 * @param {RequestInit} options - fetch options (method, body, etc.)
 * @returns {Promise<any>} Parsed JSON response
 * @throws {Error} On non-2xx response with message from API
 */
async function apiCall(endpoint, options = {}) {
  const token = sessionStorage.getItem('heatr_token');
  const headers = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(options.headers || {}),
  };

  const res = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers,
  });

  if (!res.ok) {
    let msg = `API error ${res.status}`;
    try {
      const err = await res.json();
      msg = err.detail || err.message || msg;
    } catch (_) {}
    throw new Error(msg);
  }

  // 204 No Content
  if (res.status === 204) return null;

  try {
    return await res.json();
  } catch (_) {
    return null;
  }
}

// =============================================================================
// Formatters
// =============================================================================

/**
 * Format a 0-100 score as a coloured badge HTML string.
 * @param {number|null} score
 * @returns {string} HTML badge
 */
function formatScore(score) {
  if (score === null || score === undefined) {
    return '<span class="score-badge score-badge-gray">—</span>';
  }
  const n = Number(score);
  let cls = 'score-badge-red';      // 0-29 poor
  if (n >= 70) cls = 'score-badge-green';   // 70-100 good
  else if (n >= 45) cls = 'score-badge-yellow'; // 45-69 mid
  else if (n >= 30) cls = 'score-badge-orange'; // 30-44 weak
  return `<span class="score-badge ${cls}">${n}</span>`;
}

/**
 * Format an ISO date string to Dutch locale date.
 * @param {string} iso
 * @returns {string}
 */
function formatDate(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString('nl-NL', {
    day: 'numeric',
    month: 'short',
    year: 'numeric',
  });
}

/**
 * Format seconds/minutes/hours/days ago.
 * @param {string} iso
 * @returns {string}
 */
function formatRelativeTime(iso) {
  if (!iso) return '—';
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'zojuist';
  if (mins < 60) return `${mins}m geleden`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}u geleden`;
  const days = Math.floor(hrs / 24);
  return `${days}d geleden`;
}

/**
 * Format a number as Euro currency.
 * @param {number} amount
 * @returns {string}
 */
function formatEuro(amount) {
  return new Intl.NumberFormat('nl-NL', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0,
  }).format(amount);
}

/**
 * Return CSS class for email status badge.
 */
function emailStatusBadge(status) {
  const map = {
    verified: '<span class="badge badge-green">✓ Geverifieerd</span>',
    catch_all: '<span class="badge badge-yellow">Catch-all</span>',
    not_found: '<span class="badge badge-red">Niet gevonden</span>',
    pending: '<span class="badge badge-neutral">In wachtrij</span>',
  };
  return map[status] || `<span class="badge badge-neutral">${status || '—'}</span>`;
}

// =============================================================================
// Toast notifications
// =============================================================================

let _toastContainer = null;

function _ensureToastContainer() {
  if (!_toastContainer) {
    _toastContainer = document.createElement('div');
    _toastContainer.className = 'toast-container';
    document.body.appendChild(_toastContainer);
  }
  return _toastContainer;
}

/**
 * Show a toast notification.
 * @param {string} message
 * @param {'success'|'error'|'info'} type
 * @param {number} duration - milliseconds
 */
function showToast(message, type = 'info', duration = 3500) {
  const container = _ensureToastContainer();
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;

  const icon = { success: '✓', error: '✕', info: 'ℹ' }[type] || 'ℹ';
  toast.innerHTML = `<span class="toast-icon">${icon}</span><span class="toast-msg">${message}</span>`;

  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('toast-visible'));

  setTimeout(() => {
    toast.classList.remove('toast-visible');
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// =============================================================================
// Sidebar
// =============================================================================

const _NAV_ITEMS = [
  { key: 'dashboard',     href: '/dashboard.html',       icon: '◈', label: 'Dashboard' },
  { key: 'search',        href: '/search.html',           icon: '⊕', label: 'Zoeken' },
  { key: 'leads',         href: '/leads.html',            icon: '◉', label: 'Leads' },
  { key: 'website-kansen',href: '/website-kansen.html',   icon: '◎', label: 'Website Kansen' },
  { key: 'campaigns',     href: '/campaigns.html',        icon: '▷', label: 'Campagnes' },
  { key: 'inbox',         href: '/inbox.html',            icon: '◻', label: 'Inbox' },
  { key: 'crm',           href: '/crm.html',              icon: '⬡', label: 'CRM' },
  { key: 'analytics',     href: '/analytics.html',        icon: '◌', label: 'Analytics' },
];

/**
 * Inject sidebar HTML into the #sidebar element on the page.
 * @param {string} activePage - Key matching _NAV_ITEMS
 */
function renderSidebar(activePage) {
  const el = document.getElementById('sidebar');
  if (!el) return;

  const items = _NAV_ITEMS.map(item => {
    const active = item.key === activePage ? ' active' : '';
    return `<a href="${item.href}" class="sidebar-item${active}">
      <span class="icon" style="display:inline-flex;align-items:center;justify-content:center;font-size:14px;width:16px;">${item.icon}</span>
      <span>${item.label}</span>
    </a>`;
  }).join('');

  el.innerHTML = `
    <div class="sidebar-brand">
      <div class="sidebar-brand-mark">H</div>
      <div class="sidebar-brand-name">Heatr</div>
    </div>

    <div class="sidebar-section">
      <div class="sidebar-section-label">Navigatie</div>
      ${items}
    </div>

    <div class="sidebar-footer">
      <div class="sidebar-avatar">AE</div>
      <div style="flex:1; min-width:0;">
        <div class="sidebar-user-name">Aerys</div>
        <div class="sidebar-user-org">info@aeryssolution.nl</div>
      </div>
      <a href="#" onclick="signOut(); return false;" class="btn btn-ghost btn-sm btn-icon" title="Uitloggen" style="width:28px;height:28px;padding:0;">⎋</a>
    </div>
  `;
}

// =============================================================================
// Skeleton loader
// =============================================================================

/**
 * Return HTML for a skeleton table loading state.
 * @param {number} rows
 * @param {number} cols
 * @returns {string} HTML
 */
function createSkeletonLoader(rows = 5, cols = 5) {
  const cells = Array(cols).fill('<td><div class="skeleton skeleton-text"></div></td>').join('');
  const rowsHtml = Array(rows).fill(`<tr>${cells}</tr>`).join('');
  return `<tbody class="skeleton-rows">${rowsHtml}</tbody>`;
}

/**
 * Return a simple skeleton card HTML.
 * @param {number} count
 * @returns {string}
 */
function createSkeletonCards(count = 6) {
  return Array(count).fill(`
    <div class="card" style="padding:20px">
      <div class="skeleton skeleton-text" style="width:60%;margin-bottom:12px"></div>
      <div class="skeleton skeleton-text" style="width:40%;margin-bottom:8px"></div>
      <div class="skeleton skeleton-text" style="width:80%"></div>
    </div>
  `).join('');
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Debounce a function.
 * @param {Function} fn
 * @param {number} delay
 * @returns {Function}
 */
function debounce(fn, delay = 300) {
  let timer;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), delay);
  };
}

/**
 * Build a query string from an object, omitting null/undefined/empty values.
 * @param {object} params
 * @returns {string} e.g. '?sector=foo&city=bar'
 */
function buildQuery(params) {
  const qs = Object.entries(params)
    .filter(([, v]) => v !== null && v !== undefined && v !== '')
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  return qs ? `?${qs}` : '';
}

/**
 * Render a simple error banner inside a container element.
 * @param {HTMLElement} el
 * @param {string} message
 */
function showError(el, message) {
  el.innerHTML = `<div class="error-banner"><strong>Fout:</strong> ${message}</div>`;
}

/**
 * Truncate text to maxLen characters.
 */
function truncate(text, maxLen = 80) {
  if (!text) return '—';
  return text.length > maxLen ? text.slice(0, maxLen) + '…' : text;
}

/**
 * Sector display name map.
 */
const SECTOR_LABELS = {
  makelaars: 'Makelaars',
  alternatieve_geneeskunde: 'Alternatieve Geneeskunde',
  cosmetische_behandelaars: 'Cosmetische Behandelaars',
  bouwbedrijven: 'Bouwbedrijven',
};

function sectorLabel(key) {
  return SECTOR_LABELS[key] || key || '—';
}

/**
 * Opportunity type badge HTML.
 */
function opportunityBadges(types) {
  if (!types || !types.length) return '';
  const map = {
    website_rebuild: ['Website', 'badge-red'],
    conversion_optimisation: ['Conversie', 'badge-orange'],
    chatbot: ['Chatbot', 'badge-yellow'],
    ai_audit: ['AI Audit', 'badge-accent'],
  };
  return types.map(t => {
    const [label, cls] = map[t] || [t, 'badge-neutral'];
    return `<span class="badge ${cls}">${label}</span>`;
  }).join(' ');
}
