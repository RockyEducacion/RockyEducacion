import { el, qs } from '../utils/dom.js';

export const RegistroDiarioSupervisor = (mount, deps = {}) => {
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Registro Diario Supervisor']),
    el('p', { className: 'text-muted', style: 'margin-top:.25rem;' }, ['Consulta de solo lectura filtrada por empleados visibles del supervisor.']),
    el('div', { className: 'toolbar', style: 'margin-top:10px;gap:8px;display:grid;grid-template-columns:1fr auto auto;' }, [
      el('input', { id: 'txtSearch', className: 'input', placeholder: 'Buscar por cédula, nombre, sede o novedad...' }),
      el('input', { id: 'fltDate', className: 'input', type: 'date' }),
      el('button', { id: 'btnToday', className: 'btn', type: 'button' }, ['Hoy'])
    ]),
    el('div', { className: 'table-wrap', style: 'margin-top:10px;' }, [
      el('table', { className: 'table', id: 'tblSupervisorDaily' }, [
        el('thead', {}, [el('tr', {}, [
          el('th', { 'data-sort': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
          el('th', { 'data-sort': 'hora', style: 'cursor:pointer' }, ['Hora']),
          el('th', { 'data-sort': 'sede', style: 'cursor:pointer' }, ['Sede']),
          el('th', { 'data-sort': 'documento', style: 'cursor:pointer' }, ['Cédula']),
          el('th', { 'data-sort': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
          el('th', { 'data-sort': 'novedad', style: 'cursor:pointer' }, ['Novedad']),
          el('th', { 'data-sort': 'estado', style: 'cursor:pointer' }, ['Estado'])
        ])]),
        el('tbody', {})
      ])
    ]),
    el('p', { id: 'msg', className: 'text-muted', style: 'margin-top:8px;' }, ['Cargando...'])
  ]);

  const tbody = qs('#tblSupervisorDaily tbody', ui);
  const searchInput = qs('#txtSearch', ui);
  const dateInput = qs('#fltDate', ui);
  const msg = qs('#msg', ui);

  let attendance = [];
  let replacements = [];
  let employees = [];
  let sedes = [];
  let unAtt = () => {};
  let unRepl = () => {};
  let unEmp = () => {};
  let unSedes = () => {};
  let activeDay = '';
  let legacyLoadToken = 0;
  let sortKey = 'hora';
  let sortDir = -1;

  function todayISO() {
    const d = new Date();
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }

  function norm(v) {
    return String(v || '').trim().toLowerCase();
  }

  function rowKey(r = {}) {
    const empId = String(r.empleadoId || r.employeeId || '').trim();
    return `${String(r.fecha || '').trim()}_${empId}`;
  }

  function replacementMap() {
    const map = new Map();
    (replacements || []).forEach((r) => map.set(rowKey(r), r));
    return map;
  }

  function allowedSets() {
    const docs = new Set();
    const ids = new Set();
    const sedeCodes = new Set();
    (employees || []).forEach((e) => {
      const doc = String(e?.documento || '').trim();
      const id = String(e?.id || '').trim();
      if (doc) docs.add(doc);
      if (id) ids.add(id);
    });
    (sedes || []).forEach((s) => {
      const c = String(s?.codigo || '').trim();
      if (c) sedeCodes.add(c);
    });
    return { docs, ids, sedeCodes };
  }

  function filterBySupervisorScope(rows = []) {
    const { docs, ids, sedeCodes } = allowedSets();
    const hasEmployeeScope = docs.size > 0 || ids.size > 0;
    return (rows || []).filter((r) => {
      const doc = String(r?.documento || r?.cedula || '').trim();
      const id = String(r?.empleadoId || r?.employeeId || '').trim();
      const sedeCode = String(r?.sedeCodigo || '').trim();
      if (!hasEmployeeScope) {
        return Boolean(sedeCode && sedeCodes.has(sedeCode));
      }
      if (doc && docs.has(doc)) return true;
      if (id && ids.has(id)) return true;
      return false;
    });
  }

  function displayNovedad(r = {}) {
    const raw = String(r.novedadNombre || r.novedad || r.novedadCodigo || r.novelty || '-').trim();
    const code = /^\d+$/.test(String(r.novedadCodigo || r.novedad || '').trim())
      ? String(r.novedadCodigo || r.novedad || '').trim()
      : '';
    if (code === '1') return 'Trabajando';
    if (code === '2') return 'Accidente Laboral';
    if (code === '3') return 'Enfermedad General';
    if (code === '4') return 'Calamidad';
    if (code === '5') return 'Licencia No Remunerada';
    if (code === '7') return 'Compensatorio';
    if (code === '9') return 'Vacaciones';
    return raw || '-';
  }

  function statusOf(row = {}, repl = null) {
    const decision = String(repl?.decision || '').trim();
    if (decision === 'reemplazo') return `Reemplazado por ${repl?.supernumerarioNombre || repl?.supernumerarioDocumento || '-'}`;
    if (decision === 'ausentismo') return 'Ausentismo';
    const nov = norm(displayNovedad(row));
    if (!nov || nov === '1' || nov === 'trabajando') return 'Trabajo';
    return 'Novedad';
  }

  function sortVal(r, key, replMap) {
    if (key === 'fecha') return String(r.fecha || '');
    if (key === 'hora') return String(r.hora || '');
    if (key === 'sede') return norm(r.sedeNombre || r.sedeCodigo || '');
    if (key === 'documento') return String(r.documento || '');
    if (key === 'nombre') return norm(r.nombre || '');
    if (key === 'novedad') return norm(displayNovedad(r));
    if (key === 'estado') return norm(statusOf(r, replMap.get(rowKey(r)) || null));
    return '';
  }

  function applyFilters() {
    const term = norm(searchInput.value);
    const selectedDate = String(dateInput.value || '').trim();
    const replMap = replacementMap();
    const scopedRows = filterBySupervisorScope(attendance);
    const out = scopedRows.filter((r) => {
      if (selectedDate && String(r.fecha || '').trim() !== selectedDate) return false;
      if (!term) return true;
      const blob = [
        r.fecha || '',
        r.hora || '',
        r.sedeNombre || r.sedeCodigo || '',
        r.documento || r.cedula || '',
        r.nombre || '',
        displayNovedad(r),
        statusOf(r, replMap.get(rowKey(r)) || null)
      ].join(' ').toLowerCase();
      return blob.includes(term);
    });
    out.sort((a, b) => {
      const va = sortVal(a, sortKey, replMap);
      const vb = sortVal(b, sortKey, replMap);
      if (va === vb) return 0;
      return va > vb ? sortDir : -sortDir;
    });
    return { rows: out, replMap };
  }

  function render() {
    const { rows, replMap } = applyFilters();
    tbody.replaceChildren(...rows.map((r) => el('tr', {}, [
      el('td', {}, [String(r.fecha || '-').trim() || '-']),
      el('td', {}, [String(r.hora || '-').trim() || '-']),
      el('td', {}, [String(r.sedeNombre || r.sedeCodigo || '-').trim() || '-']),
      el('td', {}, [String(r.documento || r.cedula || '-').trim() || '-']),
      el('td', {}, [String(r.nombre || '-').trim() || '-']),
      el('td', {}, [displayNovedad(r)]),
      el('td', {}, [statusOf(r, replMap.get(rowKey(r)) || null)])
    ])));
    const hasEmployeeScope = (employees || []).length > 0;
    msg.textContent = `Registros visibles: ${rows.length} | Asistencias cargadas: ${(attendance || []).length} | Reemplazos cargados: ${(replacements || []).length} | Empleados scope: ${(employees || []).length} | Sedes scope: ${(sedes || []).length} | Filtro activo: ${hasEmployeeScope ? 'empleados' : 'sedes'}`;
  }

  async function loadLegacyIfNeeded(day, token) {
    if (!day) return;
    if (!deps.listAttendanceRange || !deps.listImportReplacementsRange) return;
    if ((attendance || []).length > 0) return;
    try {
      const [attRows, replRows] = await Promise.all([
        deps.listAttendanceRange(day, day),
        deps.listImportReplacementsRange(day, day)
      ]);
      if (token !== legacyLoadToken || day !== activeDay) return;
      attendance = attRows || [];
      replacements = filterBySupervisorScope(replRows || []);
      render();
      if ((attendance || []).length > 0) msg.textContent = `Registros visibles: ${applyFilters().rows.length} (fuente respaldo)`;
    } catch (err) {
      if (token !== legacyLoadToken || day !== activeDay) return;
      msg.textContent = `Error cargando respaldo: ${err?.message || err}`;
    }
  }

  function subscribeDay(day) {
    activeDay = String(day || '').trim();
    legacyLoadToken += 1;
    const token = legacyLoadToken;
    unAtt?.();
    unRepl?.();
    msg.textContent = 'Cargando registros del día...';
    unAtt = deps.streamDashboardAttendanceByDate?.(
      day,
      (rows) => {
        attendance = rows || [];
        render();
        if ((attendance || []).length === 0) loadLegacyIfNeeded(activeDay, token);
      },
      (err) => { msg.textContent = `Error cargando asistencia: ${err?.message || err}`; }
    ) || (() => {});
    unRepl = deps.streamDashboardReplacementsByDate?.(
      day,
      (rows) => {
        replacements = filterBySupervisorScope(rows || []);
        render();
        if ((attendance || []).length === 0) loadLegacyIfNeeded(activeDay, token);
      },
      (err) => { msg.textContent = `Error cargando reemplazos: ${err?.message || err}`; }
    ) || (() => {});
  }

  dateInput.value = todayISO();

  unEmp = deps.streamEmployees?.((arr) => {
    employees = arr || [];
    replacements = filterBySupervisorScope(replacements || []);
    render();
  }) || (() => {});
  unSedes = deps.streamSedes?.((arr) => {
    sedes = arr || [];
    replacements = filterBySupervisorScope(replacements || []);
    render();
  }) || (() => {});

  dateInput.addEventListener('change', () => subscribeDay(String(dateInput.value || '').trim() || todayISO()));
  searchInput.addEventListener('input', render);
  qs('#btnToday', ui).addEventListener('click', () => {
    dateInput.value = todayISO();
    subscribeDay(dateInput.value);
  });
  ui.querySelectorAll('th[data-sort]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else { sortKey = key; sortDir = 1; }
      render();
    });
  });

  subscribeDay(dateInput.value);
  mount.replaceChildren(ui);

  return () => {
    unAtt?.();
    unRepl?.();
    unEmp?.();
    unSedes?.();
  };
};
