import { PERMS } from '../../permissions.js';
import { navigate } from '../../router.js';
import { el } from '../../utils/dom.js';
import { isActive, visibleActions } from './ModuleDashboardUtils.js';

const ACTIONS = [
  { label: 'Empleados', route: '/employees', perm: PERMS.VIEW_EMPLOYEES, detail: 'Base de empleados y cambios de estado.' },
  { label: 'Novedades empleados', route: '/employee-novelties', perm: PERMS.VIEW_EMPLOYEE_NOVELTIES, detail: 'Historial laboral por empleado.' },
  { label: 'Supervisores', route: '/supervisors', perm: PERMS.VIEW_SUPERVISORS, detail: 'Asignacion de zonas.' },
  { label: 'Incapacidades', route: '/upload', perm: PERMS.VIEW_INCAPACITIES, detail: 'Registro manual y consulta de soportes.' }
];

const METRICS = [
  { key: 'employees', label: 'Empleados activos', tone: 'green' },
  { key: 'supervisors', label: 'Supervisores', tone: 'blue' },
  { key: 'todayIncapacities', label: 'Incapacidades hoy', tone: 'red' }
];

export const EmpleadosDashboard = (mount, deps = {}) => {
  const today = todayBogota();
  const actions = visibleActions(ACTIONS);
  const metricsNode = el('div', { className: 'module-dashboard__metrics' },
    METRICS.map((metric) => metricTile(metric.label, '...', metric.tone))
  );
  const peopleChartMount = el('div', { className: 'gov-role-chart__body' }, [
    el('p', { className: 'text-muted' }, ['Cargando personal activo...'])
  ]);
  const incapacityChartMount = el('div', { className: 'gov-role-chart__body employee-incapacity-chart' }, [
    el('p', { className: 'text-muted' }, ['Cargando incapacidades...'])
  ]);
  const actionNodes = actions.map((action) => actionTile(action));
  const state = {
    employees: null,
    supervisors: null,
    supernumerarios: null,
    incapacities: null
  };

  const ui = el('section', { className: 'main-card module-dashboard module-dashboard--empleados' }, [
    el('div', { className: 'module-dashboard__header' }, [
      el('div', {}, [
        el('p', { className: 'module-dashboard__eyebrow' }, ['Dashboard de modulo']),
        el('h2', {}, ['Empleados'])
      ]),
      el('span', { className: 'badge' }, [`Corte: ${today}`])
    ]),
    metricsNode,
    el('div', { className: 'section-block module-dashboard__charts' }, [
      el('article', { className: 'gov-role-chart' }, [
        el('div', { className: 'gov-role-chart__head' }, [
          el('div', {}, [
            el('strong', { className: 'gov-role-chart__title' }, ['Empleados activos por cargo']),
            el('span', { className: 'gov-role-chart__subtitle' }, ['Distribucion actual de empleados activos segun su cargo.'])
          ]),
          el('span', { id: 'employeePeopleTotal', className: 'badge' }, ['0 activos'])
        ]),
        peopleChartMount
      ]),
      el('article', { className: 'gov-role-chart' }, [
        el('div', { className: 'gov-role-chart__head' }, [
          el('div', {}, [
            el('strong', { className: 'gov-role-chart__title' }, ['Ausencias']),
            el('span', { className: 'gov-role-chart__subtitle' }, ['Distribucion del mes actual por tipo y personas con mas registros.'])
          ]),
          el('span', { id: 'employeeIncapacityTotal', className: 'badge' }, ['0 dias'])
        ]),
        incapacityChartMount
      ])
    ]),
    el('div', { className: 'section-block' }, [
      el('h3', { className: 'section-title' }, ['Gestion de personas']),
      el('div', { className: 'module-dashboard__actions' }, actionNodes.length
        ? actionNodes
        : [el('p', { className: 'text-muted' }, ['No hay accesos disponibles para tu rol en este modulo.'])])
    ])
  ]);

  mount.replaceChildren(ui);

  const refreshPeople = () => {
    renderMetrics(metricsNode, state, today);
    renderPeopleChart(ui, peopleChartMount, state);
  };
  const refreshIncapacities = () => {
    renderMetrics(metricsNode, state, today);
    renderIncapacityChart(ui, incapacityChartMount, state.incapacities, today);
  };

  let unEmployees = null;
  let unSupervisors = null;
  let unSupernumerarios = null;
  let unIncapacities = null;

  try {
    unEmployees = deps.streamEmployees?.((rows) => {
      state.employees = Array.isArray(rows) ? rows : [];
      refreshPeople();
    }) || null;
  } catch (_) {
    state.employees = [];
    refreshPeople();
  }
  if (!deps.streamEmployees) {
    state.employees = [];
    refreshPeople();
  }
  try {
    unSupervisors = deps.streamSupervisors?.((rows) => {
      state.supervisors = Array.isArray(rows) ? rows : [];
      refreshPeople();
    }) || null;
  } catch (_) {
    state.supervisors = [];
    refreshPeople();
  }
  if (!deps.streamSupervisors) {
    state.supervisors = [];
    refreshPeople();
  }
  try {
    unSupernumerarios = deps.streamSupernumerarios?.((rows) => {
      state.supernumerarios = Array.isArray(rows) ? rows : [];
      refreshPeople();
    }, today) || null;
  } catch (_) {
    state.supernumerarios = [];
    refreshPeople();
  }
  if (!deps.streamSupernumerarios) {
    state.supernumerarios = [];
    refreshPeople();
  }
  try {
    unIncapacities = deps.streamIncapacidades?.((rows) => {
      state.incapacities = Array.isArray(rows) ? rows : [];
      refreshIncapacities();
    }) || null;
  } catch (_) {
    state.incapacities = [];
    refreshIncapacities();
  }
  if (!deps.streamIncapacidades) {
    state.incapacities = [];
    refreshIncapacities();
  }

  if (!deps.streamEmployees || !deps.streamSupervisors || !deps.streamSupernumerarios) {
    renderPeopleChart(ui, peopleChartMount, state);
  }
  if (!deps.streamIncapacidades) {
    incapacityChartMount.replaceChildren(el('p', { className: 'text-muted' }, ['No hay conexion para incapacidades.']));
  }

  return () => {
    try { unEmployees?.(); } catch {}
    try { unSupervisors?.(); } catch {}
    try { unSupernumerarios?.(); } catch {}
    try { unIncapacities?.(); } catch {}
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

function renderMetrics(container, state, today) {
  const tiles = Array.from(container.querySelectorAll('.metric-tile__value'));
  const people = activePeopleCounts(state);
  const values = {
    employees: people.employees,
    supervisors: people.supervisors,
    todayIncapacities: countTodayIncapacities(state.incapacities, today)
  };
  METRICS.forEach((metric, index) => {
    if (!tiles[index]) return;
    tiles[index].textContent = stateValueReady(metric.key, state) ? formatNumber(values[metric.key]) : '...';
  });
}

function stateValueReady(key, state) {
  if (key === 'employees') return [state.employees, state.supervisors, state.supernumerarios].every(Array.isArray);
  if (key === 'supervisors') return Array.isArray(state.supervisors);
  if (key === 'todayIncapacities') return Array.isArray(state.incapacities);
  return true;
}

function renderPeopleChart(scope, mount, state) {
  if (![state.employees, state.supervisors, state.supernumerarios].every(Array.isArray)) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['Cargando personal activo...']));
    return;
  }
  const chartEmployees = activeEmployeesForCargoChart(state);
  const byCargo = countBy(chartEmployees, employeeCargoLabel);
  const rows = [...byCargo.entries()]
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  const total = rows.reduce((sum, row) => sum + row.count, 0);
  const max = Math.max(1, ...rows.map((row) => row.count));
  const totalNode = scope.querySelector('#employeePeopleTotal');
  if (totalNode) totalNode.textContent = `${formatNumber(total)} activo${total === 1 ? '' : 's'}`;

  if (!total) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['No hay empleados activos para graficar.']));
    return;
  }

  mount.replaceChildren(...rows.map((row) => {
    const percent = Math.round((row.count / max) * 100);
    const share = total > 0 ? Math.round((row.count / total) * 100) : 0;
    return barRow(row.label, row.count, percent, share);
  }));
}

function renderIncapacityChart(scope, mount, rows = null, today = todayBogota()) {
  if (!Array.isArray(rows)) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['Cargando incapacidades...']));
    return;
  }
  const monthStart = monthStartBogota(today);
  const activeRows = rows
    .filter((row) => isActive(row) && incapacityOverlapsRange(row, monthStart, today))
    .map((row) => ({ ...row, monthDays: incapacityDaysInRange(row, monthStart, today) }))
    .filter((row) => row.monthDays > 0);
  const totalDays = activeRows.reduce((sum, row) => sum + row.monthDays, 0);
  const totalNode = scope.querySelector('#employeeIncapacityTotal');
  if (totalNode) totalNode.textContent = `${formatNumber(totalDays)} dia${totalDays === 1 ? '' : 's'}`;

  if (!totalDays) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['No hay incapacidades del mes actual para graficar.']));
    return;
  }

  const byType = sumBy(activeRows, (row) => String(row?.source || 'Sin tipo').trim() || 'Sin tipo', (row) => row.monthDays);
  const byPerson = sumBy(activeRows, personKey, (row) => row.monthDays);
  const typeRows = [...byType.entries()]
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
  const peopleRows = [...byPerson.entries()]
    .map(([key, count]) => ({ label: personLabel(key), count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
    .slice(0, 5);
  const typeMax = Math.max(1, ...typeRows.map((row) => row.count));

  mount.replaceChildren(
    el('div', { className: 'employee-incapacity-chart__split' }, [
      el('div', { className: 'employee-incapacity-chart__panel' }, [
        el('strong', { className: 'employee-incapacity-chart__label' }, ['Por tipo']),
        el('div', { className: 'employee-incapacity-chart__bars' }, typeRows.map((row) => {
          const percent = Math.round((row.count / typeMax) * 100);
          const share = Math.round((row.count / totalDays) * 100);
          return barRow(row.label, row.count, percent, share, 'dias');
        }))
      ]),
      el('div', { className: 'employee-incapacity-chart__panel' }, [
        el('strong', { className: 'employee-incapacity-chart__label' }, ['Top personas']),
        el('ol', { className: 'employee-incapacity-chart__top' }, peopleRows.map((row) =>
          el('li', {}, [
            el('span', {}, [row.label]),
            el('strong', {}, [`${formatNumber(row.count)} dia${row.count === 1 ? '' : 's'}`])
          ])
        ))
      ])
    ])
  );
}

function barRow(label, count, percent, share, unit = '') {
  const valueLabel = unit === 'dias'
    ? `${formatNumber(count)} dia${count === 1 ? '' : 's'}`
    : formatNumber(count);
  return el('div', { className: 'gov-role-chart__row' }, [
    el('div', { className: 'gov-role-chart__meta' }, [
      el('span', { className: 'gov-role-chart__role' }, [label]),
      el('span', { className: 'gov-role-chart__count' }, [`${valueLabel} (${share}%)`])
    ]),
    el('div', { className: 'gov-role-chart__track', 'aria-label': `${label}: ${count}` }, [
      el('div', { className: 'gov-role-chart__bar', style: `width:${percent}%` }, [])
    ])
  ]);
}

function activePeopleCounts(state) {
  const activeSupervisors = (state.supervisors || []).filter(isActive);
  return {
    employees: activeBaseEmployees(state).length,
    supervisors: activeSupervisors.length,
    supernumerarios: (state.supernumerarios || []).filter(isActive).length
  };
}

function activeBaseEmployees(state) {
  const activeSupervisors = (state.supervisors || []).filter(isActive);
  const activeSupernumerarios = (state.supernumerarios || []).filter(isActive);
  const supervisorDocs = new Set(activeSupervisors.map(personDoc).filter(Boolean));
  const supernumerarioDocs = new Set(activeSupernumerarios.map(personDoc).filter(Boolean));
  return (state.employees || [])
    .filter(isActive)
    .filter((row) => {
      const doc = personDoc(row);
      return !doc || (!supervisorDocs.has(doc) && !supernumerarioDocs.has(doc));
    });
}

function activeEmployeesForCargoChart(state) {
  const rows = [];
  const seen = new Set();
  const add = (row) => {
    if (!row || !isActive(row)) return;
    const key = personIdentityKey(row);
    if (key && seen.has(key)) return;
    if (key) seen.add(key);
    rows.push(row);
  };
  (state.employees || []).forEach(add);
  (state.supervisors || []).forEach(add);
  (state.supernumerarios || []).forEach(add);
  return rows;
}

function employeeCargoLabel(row = {}) {
  return String(row?.cargoNombre || row?.cargoCodigo || 'Sin cargo').trim() || 'Sin cargo';
}

function countTodayIncapacities(rows = null, today = todayBogota()) {
  if (!Array.isArray(rows)) return 0;
  return rows.filter((row) => isActive(row) && incapacityOverlapsDay(row, today)).length;
}

function incapacityOverlapsDay(row = {}, day = todayBogota()) {
  const start = String(row?.fechaInicio || '').slice(0, 10);
  const end = String(row?.fechaFin || start).slice(0, 10);
  if (!start && !end) return false;
  if (start && start > day) return false;
  if (end && end < day) return false;
  return true;
}

function incapacityOverlapsRange(row = {}, from = '', to = '') {
  const start = String(row?.fechaInicio || '').slice(0, 10);
  const end = String(row?.fechaFin || start).slice(0, 10);
  if (!start && !end) return false;
  if (from && end && end < from) return false;
  if (to && start && start > to) return false;
  return true;
}

function incapacityDaysInRange(row = {}, from = '', to = '') {
  const start = String(row?.fechaInicio || '').slice(0, 10);
  const end = String(row?.fechaFin || start).slice(0, 10);
  if (!start && !end) return 0;
  const effectiveStart = start && (!from || start > from) ? start : from;
  const effectiveEnd = end && (!to || end < to) ? end : to;
  return inclusiveDaysBetween(effectiveStart, effectiveEnd);
}

function inclusiveDaysBetween(startDate = '', endDate = '') {
  if (!startDate || !endDate || endDate < startDate) return 0;
  const [sy, sm, sd] = startDate.split('-').map((part) => Number(part));
  const [ey, em, ed] = endDate.split('-').map((part) => Number(part));
  const start = Date.UTC(sy, (sm || 1) - 1, sd || 1);
  const end = Date.UTC(ey, (em || 1) - 1, ed || 1);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return 0;
  return Math.floor((end - start) / 86400000) + 1;
}

function countBy(rows = [], keyFn) {
  const out = new Map();
  rows.forEach((row) => {
    const key = keyFn(row);
    out.set(key, (out.get(key) || 0) + 1);
  });
  return out;
}

function sumBy(rows = [], keyFn, valueFn) {
  const out = new Map();
  rows.forEach((row) => {
    const key = keyFn(row);
    const value = Number(valueFn(row) || 0);
    out.set(key, (out.get(key) || 0) + value);
  });
  return out;
}

function personKey(row = {}) {
  const doc = String(row?.documento || '').trim();
  const name = String(row?.nombre || '').trim();
  return `${name || 'Sin nombre'}|${doc || 'Sin documento'}`;
}

function personLabel(key = '') {
  const [name, doc] = String(key || '').split('|');
  return doc && doc !== 'Sin documento' ? `${name} (${doc})` : (name || '-');
}

function personDoc(row = {}) {
  return String(row?.documento || '').trim();
}

function personIdentityKey(row = {}) {
  const id = String(row?.id || row?.employeeId || '').trim();
  if (id) return `id:${id}`;
  const doc = personDoc(row);
  return doc ? `doc:${doc}` : '';
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

function monthStartBogota(today = todayBogota()) {
  const [year, month] = String(today || todayBogota()).split('-');
  return `${year}-${month}-01`;
}

function formatNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? new Intl.NumberFormat('es-CO').format(number) : '-';
}
