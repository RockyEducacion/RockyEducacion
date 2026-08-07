import { el } from '../../utils/dom.js';
import { navigate } from '../../router.js';
import { can, isSuperAdmin } from '../../permissions.js';

export function renderModuleDashboard(mount, deps = {}, config = {}) {
  const actions = visibleActions(config.actions || []);
  const metrics = config.metrics || [];
  const today = todayBogota();

  const metricsNode = el('div', { className: 'module-dashboard__metrics' },
    metrics.map((metric) => metricTile(metric.label, '...', metric.tone))
  );
  const actionNodes = actions.map((action) => actionTile(action));
  const emptyActions = el('p', { className: 'text-muted' }, ['No hay accesos disponibles para tu rol en este modulo.']);

  const ui = el('section', { className: `main-card module-dashboard ${config.className || ''}` }, [
    el('div', { className: 'module-dashboard__header' }, [
      el('div', {}, [
        el('p', { className: 'module-dashboard__eyebrow' }, ['Dashboard de modulo']),
        el('h2', {}, [config.title || 'Dashboard']),
        el('p', { className: 'text-muted' }, [config.lead || ''])
      ]),
      el('span', { className: 'badge' }, [`Corte: ${today}`])
    ]),
    metricsNode,
    el('div', { className: 'section-block module-dashboard__insights' }, [
      el('h3', { className: 'section-title' }, [config.sectionTitle || 'Resumen']),
      el('div', { className: 'module-dashboard__insight-list' }, (config.insights || []).map((text) =>
        el('p', { className: 'module-dashboard__insight' }, [text])
      ))
    ]),
    el('div', { className: 'section-block' }, [
      el('h3', { className: 'section-title' }, ['Accesos del modulo']),
      el('div', { className: 'module-dashboard__actions' }, actionNodes.length ? actionNodes : [emptyActions])
    ])
  ]);

  mount.replaceChildren(ui);
  loadMetrics(metricsNode, metrics, deps, actions);
  return () => {};
}

export function visibleActions(actions = []) {
  return actions.filter((action) => {
    if (action.perm === 'superadmin') return isSuperAdmin();
    return !action.perm || can(action.perm);
  });
}

export function isActive(row = {}) {
  return String(row?.estado || 'activo').trim().toLowerCase() === 'activo';
}

export function countStream(streamFn, predicate = null) {
  if (typeof streamFn !== 'function') return 0;
  return streamOnce(streamFn).then((rows) => predicate ? rows.filter(predicate).length : rows.length);
}

export async function countIncapacitiesToday(deps = {}) {
  const today = todayBogota();
  const rows = await deps.listIncapacidadesRange?.(today, today);
  return Array.isArray(rows) ? rows.length : 0;
}

export async function dailyMetric(deps = {}, field = '') {
  const today = todayBogota();
  const rows = await deps.listDailyMetricsRange?.(today, today);
  const row = Array.isArray(rows) ? rows.find((item) => String(item?.fecha || '').trim() === today) : null;
  return Number(row?.[field] || 0);
}

export async function countCurrentMonthMetrics(deps = {}) {
  const today = todayBogota();
  const rows = await deps.listDailyMetricsRange?.(monthStartBogota(today), today);
  return Array.isArray(rows) ? rows.length : 0;
}

export function daysElapsedInMonth() {
  return Number(todayBogota().slice(8, 10) || 0);
}

function actionTile(action = {}) {
  const btn = el('button', { className: 'module-dashboard__action', type: 'button' }, [
    el('span', { className: 'module-dashboard__action-label' }, [action.label || '-']),
    el('span', { className: 'module-dashboard__action-detail' }, [action.detail || 'Abrir modulo'])
  ]);
  btn.addEventListener('click', () => navigate(action.route || '/'));
  return btn;
}

function metricTile(label, value, tone = 'blue') {
  return el('div', { className: `metric-tile metric-tile--${tone}` }, [
    el('span', { className: 'metric-tile__label' }, [label]),
    el('strong', { className: 'metric-tile__value' }, [String(value ?? '-')])
  ]);
}

async function loadMetrics(container, metrics, deps, actions) {
  const tiles = Array.from(container.querySelectorAll('.metric-tile__value'));
  await Promise.all(metrics.map(async (metric, index) => {
    try {
      const value = await metric.load?.(deps, actions);
      if (tiles[index]) tiles[index].textContent = formatNumber(value);
    } catch (_) {
      if (tiles[index]) tiles[index].textContent = '-';
    }
  }));
}

function streamOnce(streamFn, timeoutMs = 10000) {
  return new Promise((resolve) => {
    let settled = false;
    let unsub = () => {};
    const finish = (rows) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { unsub?.(); } catch {}
      resolve(Array.isArray(rows) ? rows : []);
    };
    const timer = setTimeout(() => finish([]), timeoutMs);
    try {
      unsub = streamFn((rows) => finish(rows), () => finish([]));
      if (settled) {
        try { unsub?.(); } catch {}
      }
    } catch (_) {
      finish([]);
    }
  });
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

function monthStartBogota(today) {
  const [year, month] = String(today || todayBogota()).split('-');
  return `${year}-${month}-01`;
}

function formatNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? new Intl.NumberFormat('es-CO').format(number) : '-';
}
