import { PERMS } from '../../permissions.js';
import { ROLE_LABELS } from '../../roles.js';
import { navigate } from '../../router.js';
import { el } from '../../utils/dom.js';
import { isActive, visibleActions } from './ModuleDashboardUtils.js';

const ACTIONS = [
  { label: 'Centro de Permisos', route: '/permissions', perm: PERMS.VIEW_PERMISSIONS, detail: 'Matriz de permisos y excepciones por usuario.' },
  { label: 'Auditoria', route: '/permissions-audit', perm: PERMS.VIEW_AUDIT, detail: 'Cambios registrados sobre permisos y roles.' },
  { label: 'Usuarios', route: '/users', perm: PERMS.VIEW_USERS, detail: 'Roles, estado y sincronizacion de accesos.' }
];

const METRICS = [
  { key: 'total', label: 'Usuarios', tone: 'blue' },
  { key: 'active', label: 'Activos', tone: 'green' },
  { key: 'inactive', label: 'Inactivos', tone: 'red' }
];

export const GobiernoDashboard = (mount, deps = {}) => {
  const today = todayBogota();
  const actions = visibleActions(ACTIONS);
  const metricsNode = el('div', { className: 'module-dashboard__metrics' },
    METRICS.map((metric) => metricTile(metric.label, '...', metric.tone))
  );
  const chartMount = el('div', { className: 'gov-role-chart__body' }, [
    el('p', { className: 'text-muted' }, ['Cargando usuarios activos...'])
  ]);
  const closureChartMount = el('div', { className: 'gov-closure-chart__body' }, [
    el('p', { className: 'text-muted' }, ['Cargando cierres del mes...'])
  ]);
  const actionNodes = actions.map((action) => actionTile(action));

  const ui = el('section', { className: 'main-card module-dashboard module-dashboard--gobierno' }, [
    el('div', { className: 'module-dashboard__header' }, [
      el('div', {}, [
        el('p', { className: 'module-dashboard__eyebrow' }, ['Dashboard de modulo']),
        el('h2', {}, ['Gobierno'])
      ]),
      el('span', { className: 'badge' }, [`Corte: ${today}`])
    ]),
    metricsNode,
    el('div', { className: 'section-block module-dashboard__charts' }, [
      el('article', { className: 'gov-role-chart' }, [
        el('div', { className: 'gov-role-chart__head' }, [
          el('div', {}, [
            el('strong', { className: 'gov-role-chart__title' }, ['Usuarios activos por rol']),
            el('span', { className: 'gov-role-chart__subtitle' }, ['Distribucion actual de usuarios con estado activo.'])
          ]),
          el('span', { id: 'govActiveRoleTotal', className: 'badge' }, ['0 activos'])
        ]),
        chartMount
      ]),
      el('article', { className: 'gov-role-chart gov-closure-chart' }, [
        el('div', { className: 'gov-role-chart__head' }, [
          el('div', {}, [
            el('strong', { className: 'gov-role-chart__title' }, ['Cierres del mes actual']),
            el('span', { className: 'gov-role-chart__subtitle' }, ['Dias con cierre completado frente a dias pendientes.'])
          ]),
          el('span', { id: 'govClosureMonthTotal', className: 'badge' }, ['0/0 cerrados'])
        ]),
        closureChartMount
      ])
    ]),
    el('div', { className: 'section-block' }, [
      el('h3', { className: 'section-title' }, ['Accesos de gobierno']),
      el('div', { className: 'module-dashboard__actions' }, actionNodes.length
        ? actionNodes
        : [el('p', { className: 'text-muted' }, ['No hay accesos disponibles para tu rol en este modulo.'])]
      )
    ])
  ]);

  mount.replaceChildren(ui);

  let unsubscribeUsers = null;
  let unsubscribeClosures = null;
  try {
    unsubscribeUsers = deps.streamUsers?.((rows) => {
      const users = Array.isArray(rows) ? rows : [];
      renderUserMetrics(metricsNode, users);
      renderUsersByRoleChart(ui, chartMount, users);
    }) || null;
  } catch (_) {
    renderUserMetrics(metricsNode, []);
    renderUsersByRoleChart(ui, chartMount, []);
  }
  try {
    unsubscribeClosures = deps.streamDailyClosures?.((rows) => {
      renderClosureMonthChart(ui, closureChartMount, rows, today);
    }) || null;
  } catch (_) {
    renderClosureMonthChart(ui, closureChartMount, [], today);
  }
  if (!deps.streamDailyClosures) {
    closureChartMount.replaceChildren(el('p', { className: 'text-muted' }, ['No hay conexion para cierres diarios.']));
  }

  return () => {
    try { unsubscribeUsers?.(); } catch {}
    try { unsubscribeClosures?.(); } catch {}
  };
};

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

function renderUserMetrics(container, users = []) {
  const tiles = Array.from(container.querySelectorAll('.metric-tile__value'));
  const rows = Array.isArray(users) ? users : [];
  const active = rows.filter(isActive).length;
  const values = {
    total: rows.length,
    active,
    inactive: Math.max(0, rows.length - active)
  };
  METRICS.forEach((metric, index) => {
    if (tiles[index]) tiles[index].textContent = formatNumber(values[metric.key]);
  });
}

function renderUsersByRoleChart(scope, mount, users = []) {
  const activeRows = (Array.isArray(users) ? users : []).filter(isActive);
  const roleCounts = new Map();
  activeRows.forEach((row) => {
    const role = String(row?.role || row?.rol || 'sin_rol').trim() || 'sin_rol';
    roleCounts.set(role, (roleCounts.get(role) || 0) + 1);
  });
  const rows = Array.from(roleCounts.entries())
    .map(([role, count]) => ({ role, label: ROLE_LABELS[role] || roleLabel(role), count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  const max = Math.max(1, ...rows.map((row) => row.count));
  const total = activeRows.length;
  const totalNode = scope.querySelector('#govActiveRoleTotal');
  if (totalNode) totalNode.textContent = `${formatNumber(total)} activo${total === 1 ? '' : 's'}`;

  if (!rows.length) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['No hay usuarios activos para graficar.']));
    return;
  }

  mount.replaceChildren(...rows.map((row) => {
    const percent = Math.round((row.count / max) * 100);
    const share = total > 0 ? Math.round((row.count / total) * 100) : 0;
    return el('div', { className: 'gov-role-chart__row' }, [
      el('div', { className: 'gov-role-chart__meta' }, [
        el('span', { className: 'gov-role-chart__role' }, [row.label]),
        el('span', { className: 'gov-role-chart__count' }, [`${formatNumber(row.count)} (${share}%)`])
      ]),
      el('div', { className: 'gov-role-chart__track', 'aria-label': `${row.label}: ${row.count}` }, [
        el('div', { className: 'gov-role-chart__bar', style: `width:${percent}%` }, [])
      ])
    ]);
  }));
}

function renderClosureMonthChart(scope, mount, closures = [], today = todayBogota()) {
  const days = daysInCurrentMonthToDate(today);
  const closedDates = new Set(
    (Array.isArray(closures) ? closures : [])
      .filter(isClosedDay)
      .map((row) => String(row?.fecha || '').slice(0, 10))
      .filter(Boolean)
  );
  const closedCount = days.filter((day) => closedDates.has(day.date)).length;
  const pendingCount = Math.max(0, days.length - closedCount);
  const totalNode = scope.querySelector('#govClosureMonthTotal');
  if (totalNode) totalNode.textContent = `${formatNumber(closedCount)}/${formatNumber(days.length)} cerrados`;

  if (!days.length) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['No hay dias del mes para graficar.']));
    return;
  }

  mount.replaceChildren(
    el('div', { className: 'gov-closure-chart__summary' }, [
      closureSummaryItem('Completados', closedCount, 'closed'),
      closureSummaryItem('Pendientes', pendingCount, 'pending')
    ]),
    el('div', { className: 'gov-closure-chart__grid', 'aria-label': 'Estado de cierres del mes actual' },
      days.map((day) => {
        const closed = closedDates.has(day.date);
        return el('span', {
          className: `gov-closure-chart__day ${closed ? 'is-closed' : 'is-pending'}`,
          title: `${day.date}: ${closed ? 'cierre completado' : 'sin cierre completado'}`,
          'aria-label': `${day.date}: ${closed ? 'cierre completado' : 'sin cierre completado'}`
        }, [String(day.day)]);
      })
    )
  );
}

function closureSummaryItem(label, value, state) {
  return el('div', { className: `gov-closure-chart__summary-item gov-closure-chart__summary-item--${state}` }, [
    el('span', {}, [label]),
    el('strong', {}, [formatNumber(value)])
  ]);
}

function isClosedDay(row = {}) {
  return row?.locked === true || String(row?.status || '').trim().toLowerCase() === 'closed';
}

function daysInCurrentMonthToDate(today) {
  const value = String(today || todayBogota()).slice(0, 10);
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return [];
  const year = Number(match[1]);
  const month = Number(match[2]);
  const dayCount = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(dayCount)) return [];
  return Array.from({ length: Math.max(0, dayCount) }, (_, index) => {
    const day = index + 1;
    return {
      day,
      date: `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`
    };
  });
}

function roleLabel(role) {
  return String(role || 'sin_rol')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

function formatNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? new Intl.NumberFormat('es-CO').format(number) : '-';
}
