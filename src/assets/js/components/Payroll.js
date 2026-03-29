import { el, qs, enableSectionToggles } from '../utils/dom.js';

export const Payroll = (mount, deps = {}) => {
  const today = todayBogota();
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Nomina']),
    el('p', { className: 'text-muted' }, ['Consulta el estado diario de nomina por sede en el periodo seleccionado.']),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Generar consulta']),
      el('div', { className: 'form-row mt-1' }, [
        field('Fecha inicial', el('input', { id: 'payrollDateFrom', className: 'input', type: 'date', value: monthStartBogota(today) })),
        field('Fecha final', el('input', { id: 'payrollDateTo', className: 'input', type: 'date', value: today })),
        el('button', { id: 'btnPayrollRun', className: 'btn btn--primary', type: 'button' }, ['Actualizar']),
        el('span', { id: 'payrollMsg', className: 'text-muted' }, [' '])
      ]),
      el('div', { className: 'form-row mt-2' }, [
        field('Busqueda', el('input', { id: 'payrollSearch', className: 'input', placeholder: 'Documento, nombre, cargo o sede...' })),
        field('Dependencia', el('select', { id: 'payrollDependencyFilter', className: 'select' }, [el('option', { value: '' }, ['Todas'])])),
        field('Zona', el('select', { id: 'payrollZoneFilter', className: 'select' }, [el('option', { value: '' }, ['Todas'])])),
        field('Sede', el('select', { id: 'payrollSedeFilter', className: 'select' }, [el('option', { value: '' }, ['Todas'])])),
        field('Tipo', el('select', { id: 'payrollTypeFilter', className: 'select' }, [
          el('option', { value: '' }, ['Todos']),
          el('option', { value: 'empleado' }, ['Empleados']),
          el('option', { value: 'supernumerario' }, ['Supernumerarios'])
        ]))
      ]),
      el('div', { className: 'payroll-kpis mt-2' }, [
        kpiCard('Periodo', 'Sin consultar', 'payrollKpiRange'),
        kpiCard('Sedes', '0', 'payrollKpiSedes'),
        kpiCard('Personas', '0', 'payrollKpiPeople'),
        kpiCard('Registros', '0', 'payrollKpiShifts')
      ])
    ]),
    sectionTable('Resumen por sede', 'payrollSummaryTable', [
      th('sedeNombre', 'Sede', 'summary'),
      th('dependenciaNombre', 'Dependencia', 'summary'),
      th('zonaNombre', 'Zona', 'summary'),
      th('peopleCount', 'Personas', 'summary'),
      th('shiftCount', 'Registros', 'summary'),
      th('firstDate', 'Primera fecha', 'summary'),
      th('lastDate', 'Ultima fecha', 'summary'),
      el('th', {}, ['Accion'])
    ], 'payrollSummaryTotals', 'Sin resultados.'),
    sectionTable('Detalle de personas', 'payrollWorkersTable', [
      th('documento', 'Documento', 'workers'),
      th('nombre', 'Nombre', 'workers'),
      th('cargo', 'Cargo', 'workers'),
      th('workerTypeLabel', 'Tipo', 'workers'),
      th('sedeNombre', 'Sede', 'workers'),
      th('daysWorked', 'Dias con registro', 'workers'),
      th('firstDate', 'Primera fecha', 'workers'),
      th('lastDate', 'Ultima fecha', 'workers'),
      el('th', {}, ['Accion'])
    ], 'payrollWorkersTotals', 'Sin resultados.', 'payrollWorkersTitle'),
    sectionTable('Detalle diario', 'payrollDailyTable', [
      th('fecha', 'Fecha', 'daily'),
      th('sedeNombre', 'Sede', 'daily'),
      th('documento', 'Documento', 'daily'),
      th('nombre', 'Nombre', 'daily'),
      th('cargo', 'Cargo', 'daily'),
      th('workerTypeLabel', 'Tipo', 'daily'),
      th('sourceLabel', 'Estado / nomina', 'daily')
    ], 'payrollDailyTotals', 'Selecciona una fila para ver el detalle.', 'payrollDailyTitle')
  ]);

  const msg = qs('#payrollMsg', ui);
  let summaryRows = [];
  let workerRows = [];
  let dailyRows = [];
  let selectedSedeCode = '';
  let selectedWorkerKey = '';
  let summarySortKey = 'sedeNombre';
  let summarySortDir = 1;
  let workersSortKey = 'daysWorked';
  let workersSortDir = -1;
  let dailySortKey = 'fecha';
  let dailySortDir = 1;

  const clearSelection = (workerOnly = false) => {
    if (!workerOnly) selectedSedeCode = '';
    selectedWorkerKey = '';
  };

  qs('#btnPayrollRun', ui).addEventListener('click', run);
  qs('#payrollSearch', ui).addEventListener('input', renderAll);
  qs('#payrollDependencyFilter', ui).addEventListener('change', () => { clearSelection(); renderAll(); });
  qs('#payrollZoneFilter', ui).addEventListener('change', () => { clearSelection(); renderAll(); });
  qs('#payrollSedeFilter', ui).addEventListener('change', () => { clearSelection(); renderAll(); });
  qs('#payrollTypeFilter', ui).addEventListener('change', () => { clearSelection(true); renderAll(); });

  bindSorting(ui, '#payrollSummaryTable th[data-sort-summary]', 'data-sort-summary', (key) => {
    if (summarySortKey === key) summarySortDir *= -1;
    else { summarySortKey = key; summarySortDir = 1; }
    renderSummary();
  });
  bindSorting(ui, '#payrollWorkersTable th[data-sort-workers]', 'data-sort-workers', (key) => {
    if (workersSortKey === key) workersSortDir *= -1;
    else { workersSortKey = key; workersSortDir = key === 'daysWorked' ? -1 : 1; }
    renderWorkers();
  });
  bindSorting(ui, '#payrollDailyTable th[data-sort-daily]', 'data-sort-daily', (key) => {
    if (dailySortKey === key) dailySortDir *= -1;
    else { dailySortKey = key; dailySortDir = 1; }
    renderDaily();
  });

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  run();
  return () => {};

  async function run() {
    const dateFrom = String(qs('#payrollDateFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#payrollDateTo', ui)?.value || '').trim();
    if (!isISODate(dateFrom) || !isISODate(dateTo)) return setMessage('Selecciona un rango de fechas valido.');
    if (dateFrom > dateTo) return setMessage('La fecha inicial no puede ser mayor que la fecha final.');

    const btn = qs('#btnPayrollRun', ui);
    try {
      btn.disabled = true;
      btn.textContent = 'Consultando...';
      setMessage('Consultando informacion de nomina...');
      const [statusRows, employees, supernumerarios, cargos] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(dateFrom, dateTo) || [],
        snapshotOnce(deps.streamEmployees),
        snapshotOnce(deps.streamSupernumerarios),
        snapshotOnce(deps.streamCargos)
      ]);
      const dataset = buildPayrollDataset({ statusRows, employees, supernumerarios, cargos });
      summaryRows = dataset.summaryRows;
      workerRows = dataset.workerRows;
      dailyRows = dataset.dailyRows;
      clearSelection();
      syncSelectOptions(qs('#payrollDependencyFilter', ui), dataset.meta.dependencies, 'Todas');
      syncSelectOptions(qs('#payrollZoneFilter', ui), dataset.meta.zones, 'Todas');
      syncSelectOptions(qs('#payrollSedeFilter', ui), dataset.meta.sedes, 'Todas');
      updateKpis(ui, dateFrom, dateTo, dataset.meta);
      renderAll();
      setMessage(`Consulta lista. Registros encontrados: ${dataset.meta.shiftCount}.`);
    } catch (error) {
      console.error('Payroll error:', error);
      summaryRows = [];
      workerRows = [];
      dailyRows = [];
      clearSelection();
      updateKpis(ui, dateFrom, dateTo, { sedeCount: 0, peopleCount: 0, shiftCount: 0 });
      renderAll();
      setMessage(`Error: ${error?.message || error}`);
    } finally {
      btn.disabled = false;
      btn.textContent = 'Actualizar';
    }
  }

  function renderAll() {
    renderSummary();
    renderWorkers();
    renderDaily();
  }

  function renderSummary() {
    const rows = sortRows(filterSummaryRows(ui, summaryRows), summarySortKey, summarySortDir);
    const tbody = qs('#payrollSummaryTable tbody', ui);
    const selectedFromFilter = String(qs('#payrollSedeFilter', ui)?.value || '').trim();
    if (selectedFromFilter) selectedSedeCode = selectedFromFilter;
    if (selectedSedeCode && !rows.some((row) => row.sedeCodigo === selectedSedeCode)) selectedSedeCode = '';

    if (!rows.length) {
      tbody.replaceChildren(emptyRow(8, 'Sin sedes para el filtro seleccionado.'));
      qs('#payrollSummaryTotals', ui).textContent = 'Sedes filtradas: 0 | Personas: 0 | Registros: 0';
      updateSortIndicators(ui, '#payrollSummaryTable th[data-sort-summary]', 'data-sort-summary', summarySortKey, summarySortDir);
      return;
    }

    tbody.replaceChildren(...rows.map((row) => {
      const tr = el('tr', { className: row.sedeCodigo === selectedSedeCode ? 'is-selected' : '' }, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      const select = () => {
        selectedSedeCode = row.sedeCodigo;
        selectedWorkerKey = '';
        renderSummary();
        renderWorkers();
        renderDaily();
      };
      tr.addEventListener('click', select);
      btn.addEventListener('click', (event) => { event.stopPropagation(); select(); });
      tr.append(
        el('td', {}, [row.sedeNombre || '-']),
        el('td', {}, [row.dependenciaNombre || '-']),
        el('td', {}, [row.zonaNombre || '-']),
        el('td', {}, [String(row.peopleCount || 0)]),
        el('td', {}, [String(row.shiftCount || 0)]),
        el('td', {}, [row.firstDate || '-']),
        el('td', {}, [row.lastDate || '-']),
        el('td', {}, [btn])
      );
      return tr;
    }));
    const totals = rows.reduce((acc, row) => ({ people: acc.people + Number(row.peopleCount || 0), shifts: acc.shifts + Number(row.shiftCount || 0) }), { people: 0, shifts: 0 });
    qs('#payrollSummaryTotals', ui).textContent = `Sedes filtradas: ${rows.length} | Personas: ${totals.people} | Registros: ${totals.shifts}`;
    updateSortIndicators(ui, '#payrollSummaryTable th[data-sort-summary]', 'data-sort-summary', summarySortKey, summarySortDir);
  }

  function renderWorkers() {
    const rows = sortRows(filterWorkerRows(ui, workerRows, selectedSedeCode), workersSortKey, workersSortDir);
    const tbody = qs('#payrollWorkersTable tbody', ui);
    const title = qs('#payrollWorkersTitle', ui);
    const selectedSede = summaryRows.find((row) => row.sedeCodigo === selectedSedeCode) || null;
    title.textContent = selectedSede ? `Detalle de personas - ${selectedSede.sedeNombre}` : 'Detalle de personas';
    if (selectedWorkerKey && !rows.some((row) => row.workerKey === selectedWorkerKey)) selectedWorkerKey = '';

    if (!rows.length) {
      tbody.replaceChildren(emptyRow(9, 'Sin personas para el filtro seleccionado.'));
      qs('#payrollWorkersTotals', ui).textContent = 'Personas filtradas: 0 | Dias con registro: 0';
      updateSortIndicators(ui, '#payrollWorkersTable th[data-sort-workers]', 'data-sort-workers', workersSortKey, workersSortDir);
      return;
    }

    tbody.replaceChildren(...rows.map((row) => {
      const tr = el('tr', { className: row.workerKey === selectedWorkerKey ? 'is-selected' : '' }, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      const select = () => {
        selectedWorkerKey = row.workerKey;
        renderWorkers();
        renderDaily();
      };
      tr.addEventListener('click', select);
      btn.addEventListener('click', (event) => { event.stopPropagation(); select(); });
      tr.append(
        el('td', {}, [row.documento || '-']),
        el('td', {}, [row.nombre || '-']),
        el('td', {}, [row.cargo || '-']),
        el('td', {}, [row.workerTypeLabel || '-']),
        el('td', {}, [row.sedeNombre || '-']),
        el('td', {}, [String(row.daysWorked || 0)]),
        el('td', {}, [row.firstDate || '-']),
        el('td', {}, [row.lastDate || '-']),
        el('td', {}, [btn])
      );
      return tr;
    }));
    const totals = rows.reduce((acc, row) => acc + Number(row.daysWorked || 0), 0);
    qs('#payrollWorkersTotals', ui).textContent = `Personas filtradas: ${rows.length} | Dias con registro: ${totals}`;
    updateSortIndicators(ui, '#payrollWorkersTable th[data-sort-workers]', 'data-sort-workers', workersSortKey, workersSortDir);
  }

  function renderDaily() {
    const rows = sortRows(filterDailyRows(ui, dailyRows, workerRows, selectedSedeCode, selectedWorkerKey), dailySortKey, dailySortDir);
    const tbody = qs('#payrollDailyTable tbody', ui);
    const title = qs('#payrollDailyTitle', ui);
    const selectedWorker = workerRows.find((row) => row.workerKey === selectedWorkerKey) || null;
    title.textContent = selectedWorker
      ? `Detalle diario - ${selectedWorker.nombre || '-'} (${selectedWorker.sedeNombre || '-'})`
      : selectedSedeCode
        ? `Detalle diario - ${summaryRows.find((row) => row.sedeCodigo === selectedSedeCode)?.sedeNombre || '-'}`
        : 'Detalle diario';

    if (!rows.length) {
      tbody.replaceChildren(emptyRow(7, 'Sin detalle para la seleccion actual.'));
      qs('#payrollDailyTotals', ui).textContent = selectedWorkerKey || selectedSedeCode ? 'Registros diarios: 0' : 'Selecciona una fila para ver el detalle.';
      updateSortIndicators(ui, '#payrollDailyTable th[data-sort-daily]', 'data-sort-daily', dailySortKey, dailySortDir);
      return;
    }

    tbody.replaceChildren(...rows.map((row) => el('tr', {}, [
      el('td', {}, [row.fecha || '-']),
      el('td', {}, [row.sedeNombre || '-']),
      el('td', {}, [row.documento || '-']),
      el('td', {}, [row.nombre || '-']),
      el('td', {}, [row.cargo || '-']),
      el('td', {}, [row.workerTypeLabel || '-']),
      el('td', {}, [row.sourceLabel || '-'])
    ])));
    qs('#payrollDailyTotals', ui).textContent = `Registros diarios: ${rows.length}`;
    updateSortIndicators(ui, '#payrollDailyTable th[data-sort-daily]', 'data-sort-daily', dailySortKey, dailySortDir);
  }

  function setMessage(text) {
    msg.textContent = text || ' ';
  }
};

function field(label, control) {
  return el('div', {}, [el('label', { className: 'label' }, [label]), control]);
}

function th(key, label, group) {
  return el('th', { [`data-sort-${group}`]: key, style: 'cursor:pointer' }, [label]);
}

function sectionTable(title, tableId, headers, totalsId, totalsText, titleId = '') {
  return el('div', { className: 'section-block mt-2' }, [
    el('h3', { className: 'section-title', ...(titleId ? { id: titleId } : {}) }, [title]),
    el('div', { className: 'table-wrap mt-2' }, [
      el('table', { className: 'table', id: tableId }, [
        el('thead', {}, [el('tr', {}, headers)]),
        el('tbody', {})
      ])
    ]),
    el('p', { id: totalsId, className: 'text-muted mt-2' }, [totalsText])
  ]);
}

function kpiCard(label, value, id) {
  return el('div', { className: 'payroll-kpi' }, [
    el('span', { className: 'payroll-kpi__label' }, [label]),
    el('strong', { className: 'payroll-kpi__value', id }, [value])
  ]);
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
}

function monthStartBogota(today) {
  const [year, month] = String(today || todayBogota()).split('-');
  return `${year}-${month}-01`;
}

function buildPayrollDataset({ statusRows = [], employees = [], supernumerarios = [], cargos = [] } = {}) {
  const cargoByCode = makeMap(cargos, 'codigo');
  const employeeById = makeMap(employees, 'id');
  const employeeByDoc = makeMap(employees, 'documento');
  const superById = makeMap(supernumerarios, 'id');
  const superByDoc = makeMap(supernumerarios, 'documento');
  const seen = new Set();
  const dailyRows = [];

  (statusRows || []).forEach((row) => {
    const item = normalizePayrollStatusRecord({
      row,
      employeeById,
      employeeByDoc,
      superById,
      superByDoc,
      cargoByCode
    });
    if (!item) return;
    const key = buildShiftKey(item);
    if (seen.has(key)) return;
    seen.add(key);
    dailyRows.push(item);
  });

  dailyRows.sort((a, b) => {
    if (a.fecha !== b.fecha) return String(a.fecha || '').localeCompare(String(b.fecha || ''));
    const bySede = String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || ''));
    if (bySede !== 0) return bySede;
    const byName = String(a.nombre || '').localeCompare(String(b.nombre || ''));
    if (byName !== 0) return byName;
    return String(a.documento || '').localeCompare(String(b.documento || ''));
  });

  const workerMap = new Map();
  const summaryMap = new Map();
  dailyRows.forEach((row) => {
    if (!workerMap.has(row.workerKey)) {
      workerMap.set(row.workerKey, { ...row, firstDate: row.fecha, lastDate: row.fecha, daysSet: new Set() });
    }
    const worker = workerMap.get(row.workerKey);
    worker.daysSet.add(String(row.fecha || '').trim());
    if (row.fecha < worker.firstDate) worker.firstDate = row.fecha;
    if (row.fecha > worker.lastDate) worker.lastDate = row.fecha;

    const summaryKey = row.sedeCodigo || ('NO_SEDE:' + (row.sedeNombre || '-'));
    if (!summaryMap.has(summaryKey)) {
      summaryMap.set(summaryKey, {
        sedeCodigo: row.sedeCodigo || '',
        sedeNombre: row.sedeNombre || '-',
        dependenciaCodigo: row.dependenciaCodigo || '',
        dependenciaNombre: row.dependenciaNombre || 'Sin dependencia',
        zonaCodigo: row.zonaCodigo || '',
        zonaNombre: row.zonaNombre || 'Sin zona',
        peopleSet: new Set(),
        shiftSet: new Set(),
        workerTypes: new Set(),
        firstDate: row.fecha,
        lastDate: row.fecha
      });
    }
    const summary = summaryMap.get(summaryKey);
    summary.peopleSet.add(row.workerKey);
    summary.shiftSet.add(buildShiftKey(row));
    summary.workerTypes.add(row.workerType);
    if (row.fecha < summary.firstDate) summary.firstDate = row.fecha;
    if (row.fecha > summary.lastDate) summary.lastDate = row.fecha;
  });

  const workerRows = Array.from(workerMap.values()).map((row) => ({ ...row, daysWorked: row.daysSet.size }));
  const summaryRows = Array.from(summaryMap.values()).map((row) => ({
    ...row,
    peopleCount: row.peopleSet.size,
    shiftCount: row.shiftSet.size,
    workerTypes: Array.from(row.workerTypes)
  }));

  return {
    summaryRows,
    workerRows,
    dailyRows,
    meta: {
      dependencies: selectOptionsFromRows(summaryRows, 'dependenciaCodigo', 'dependenciaNombre'),
      zones: selectOptionsFromRows(summaryRows, 'zonaCodigo', 'zonaNombre'),
      sedes: selectOptionsFromRows(summaryRows, 'sedeCodigo', 'sedeNombre'),
      sedeCount: summaryRows.length,
      peopleCount: workerRows.length,
      shiftCount: dailyRows.length
    }
  };
}

function normalizePayrollStatusRecord({
  row,
  employeeById,
  employeeByDoc,
  superById,
  superByDoc,
  cargoByCode
} = {}) {
  const fecha = String(row?.fecha || '').trim();
  const doc = String(row?.documento || '').trim();
  const id = String(row?.employeeId || '').trim();
  const workerType = String(row?.tipoPersonal || '').trim() === 'supernumerario' ? 'supernumerario' : 'empleado';
  const employee = (id && employeeById.get(id)) || (doc && employeeByDoc.get(doc)) || null;
  const supernumerario = (id && superById.get(id)) || (doc && superByDoc.get(doc)) || null;
  const person = workerType === 'supernumerario' ? (supernumerario || employee) : (employee || supernumerario);
  const finalDoc = doc || String(person?.documento || '').trim();
  const finalName = String(row?.nombre || person?.nombre || '').trim();
  if (!fecha || (!finalDoc && !finalName)) return null;

  const cargoCode = String(person?.cargoCodigo || '').trim();
  const cargo = cargoByCode.get(cargoCode) || null;
  const finalSedeCode = String(row?.sedeCodigo || '').trim();
  const finalSedeName = String(row?.sedeNombreSnapshot || row?.sedeCodigo || '').trim() || '-';
  return {
    fecha,
    documento: finalDoc || '-',
    nombre: finalName || '-',
    cargo: String(person?.cargoNombre || cargo?.nombre || cargoCode || '-').trim() || '-',
    workerType,
    workerTypeLabel: workerType === 'supernumerario' ? 'Supernumerario' : 'Empleado',
    sedeCodigo: finalSedeCode,
    sedeNombre: finalSedeName,
    dependenciaCodigo: String(row?.dependenciaCodigoSnapshot || '').trim(),
    dependenciaNombre: String(row?.dependenciaNombreSnapshot || 'Sin dependencia').trim() || 'Sin dependencia',
    zonaCodigo: String(row?.zonaCodigoSnapshot || '').trim(),
    zonaNombre: String(row?.zonaNombreSnapshot || 'Sin zona').trim() || 'Sin zona',
    estadoDia: String(row?.estadoDia || '').trim(),
    estadoLabel: formatPayrollStateLabel(row),
    sourceLabel: formatPayrollStatusSummary(row),
    workerKey: buildWorkerKey(workerType, finalDoc || id || finalName, finalSedeCode || finalSedeName)
  };
}

function formatPayrollStateLabel(row = {}) {
  const state = String(row?.estadoDia || '').trim();
  if (state === 'trabajado') return 'Trabajado';
  if (state === 'trabajado_reemplazo') return 'Reemplazo';
  if (state === 'ausente_con_novedad') return 'Ausencia con novedad';
  if (state === 'ausente_sin_reemplazo') return 'Ausencia sin reemplazo';
  if (state === 'incapacidad') return 'Incapacidad';
  if (state === 'vacaciones') return 'Vacaciones';
  if (state === 'compensatorio') return 'Compensatorio';
  if (state === 'sin_registro') return 'Sin registro';
  if (state === 'no_programado') return 'No programado';
  return state || 'Sin estado';
}

function formatPayrollNominaLabel(value) {
  if (value === true) return 'Paga';
  if (value === false) return 'No paga';
  return 'Pendiente';
}

function formatPayrollStatusSummary(row = {}) {
  const parts = [formatPayrollStateLabel(row), formatPayrollNominaLabel(row?.pagaNomina)];
  if (String(row?.tipoPersonal || '').trim() === 'supernumerario' && row?.reemplazaANombre) {
    parts.push('Reemplaza a ' + row.reemplazaANombre);
  } else if (row?.reemplazadoPorNombre) {
    parts.push('Cubierto por ' + row.reemplazadoPorNombre);
  }
  return parts.filter(Boolean).join(' | ');
}

function filterSummaryRows(ui, rows = []) {
  const search = normalizeText(qs('#payrollSearch', ui)?.value || '');
  const dependency = String(qs('#payrollDependencyFilter', ui)?.value || '').trim();
  const zone = String(qs('#payrollZoneFilter', ui)?.value || '').trim();
  const sede = String(qs('#payrollSedeFilter', ui)?.value || '').trim();
  const type = String(qs('#payrollTypeFilter', ui)?.value || '').trim();
  return (rows || []).filter((row) => {
    if (dependency && row.dependenciaCodigo !== dependency) return false;
    if (zone && row.zonaCodigo !== zone) return false;
    if (sede && row.sedeCodigo !== sede) return false;
    if (type && !row.workerTypes.includes(type)) return false;
    if (!search) return true;
    return normalizeText(`${row.sedeNombre || ''} ${row.dependenciaNombre || ''} ${row.zonaNombre || ''}`).includes(search);
  });
}

function filterWorkerRows(ui, rows = [], selectedSedeCode = '') {
  const search = normalizeText(qs('#payrollSearch', ui)?.value || '');
  const dependency = String(qs('#payrollDependencyFilter', ui)?.value || '').trim();
  const zone = String(qs('#payrollZoneFilter', ui)?.value || '').trim();
  const sede = String(qs('#payrollSedeFilter', ui)?.value || '').trim() || selectedSedeCode;
  const type = String(qs('#payrollTypeFilter', ui)?.value || '').trim();
  return (rows || []).filter((row) => {
    if (dependency && row.dependenciaCodigo !== dependency) return false;
    if (zone && row.zonaCodigo !== zone) return false;
    if (sede && row.sedeCodigo !== sede) return false;
    if (type && row.workerType !== type) return false;
    if (!search) return true;
    return normalizeText(`${row.documento || ''} ${row.nombre || ''} ${row.cargo || ''} ${row.sedeNombre || ''}`).includes(search);
  });
}

function filterDailyRows(ui, rows = [], workerRows = [], selectedSedeCode = '', selectedWorkerKey = '') {
  const allowed = new Set(filterWorkerRows(ui, workerRows, selectedSedeCode).map((row) => row.workerKey));
  const sede = String(qs('#payrollSedeFilter', ui)?.value || '').trim() || selectedSedeCode;
  const dependency = String(qs('#payrollDependencyFilter', ui)?.value || '').trim();
  const zone = String(qs('#payrollZoneFilter', ui)?.value || '').trim();
  const type = String(qs('#payrollTypeFilter', ui)?.value || '').trim();
  return (rows || []).filter((row) => {
    if (dependency && row.dependenciaCodigo !== dependency) return false;
    if (zone && row.zonaCodigo !== zone) return false;
    if (sede && row.sedeCodigo !== sede) return false;
    if (type && row.workerType !== type) return false;
    if (selectedWorkerKey) return row.workerKey === selectedWorkerKey;
    if (selectedSedeCode || qs('#payrollSedeFilter', ui)?.value) return row.sedeCodigo === sede;
    return allowed.has(row.workerKey);
  });
}

function syncSelectOptions(select, options = [], emptyLabel = 'Todas') {
  if (!select) return;
  const previous = String(select.value || '').trim();
  select.replaceChildren(
    el('option', { value: '' }, [emptyLabel]),
    ...(options || []).map((opt) => el('option', { value: opt.value, selected: opt.value === previous }, [`${opt.label} (${opt.value})`]))
  );
  select.value = (options || []).some((opt) => opt.value === previous) ? previous : '';
}

function updateKpis(ui, dateFrom, dateTo, meta = {}) {
  qs('#payrollKpiRange', ui).textContent = dateFrom && dateTo ? `${dateFrom} a ${dateTo}` : 'Sin consultar';
  qs('#payrollKpiSedes', ui).textContent = String(meta.sedeCount || 0);
  qs('#payrollKpiPeople', ui).textContent = String(meta.peopleCount || 0);
  qs('#payrollKpiShifts', ui).textContent = String(meta.shiftCount || 0);
}

function snapshotOnce(streamFn) {
  if (typeof streamFn !== 'function') return Promise.resolve([]);
  return new Promise((resolve) => {
    let done = false;
    let unsub = () => {};
    const finish = (rows) => {
      if (done) return;
      done = true;
      try { unsub(); } catch {}
      resolve(Array.isArray(rows) ? rows : []);
    };
    unsub = streamFn((rows) => finish(rows)) || (() => {});
    setTimeout(() => finish([]), 5000);
  });
}

function bindSorting(scope, selector, attr, onSort) {
  scope.querySelectorAll(selector).forEach((th) => th.addEventListener('click', () => {
    const key = String(th.getAttribute(attr) || '').trim();
    if (key) onSort(key);
  }));
}

function updateSortIndicators(scope, selector, attr, key, dir) {
  scope.querySelectorAll(selector).forEach((th) => {
    const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v]$/, '');
    th.dataset.baseLabel = base;
    th.textContent = String(th.getAttribute(attr) || '').trim() === key ? `${base} ${dir === 1 ? '^' : 'v'}` : base;
  });
}

function sortRows(rows = [], key = '', dir = 1) {
  return [...(rows || [])].sort((a, b) => {
    const av = sortableValue(a?.[key]);
    const bv = sortableValue(b?.[key]);
    if (av === bv) return 0;
    return av > bv ? dir : -dir;
  });
}

function sortableValue(value) {
  if (typeof value === 'number') return value;
  return String(value ?? '').toLowerCase();
}

function emptyRow(colSpan, text) {
  return el('tr', {}, [el('td', { colSpan, className: 'text-muted' }, [text])]);
}

function selectOptionsFromRows(rows = [], valueKey, labelKey) {
  return Array.from(new Map((rows || []).map((row) => [String(row?.[valueKey] || '').trim(), String(row?.[labelKey] || '-').trim() || '-']).filter(([value]) => Boolean(value))).entries())
    .sort((a, b) => String(a[1] || '').localeCompare(String(b[1] || '')))
    .map(([value, label]) => ({ value, label }));
}

function makeMap(rows = [], key) {
  return new Map((rows || []).map((row) => [String(row?.[key] || '').trim(), row]).filter(([value]) => Boolean(value)));
}

function buildWorkerKey(type, doc, sede) {
  return `${String(type || '').trim()}|${String(doc || '').trim()}|${String(sede || '').trim() || '-'}`;
}

function buildShiftKey(row = {}) {
  return `${String(row.fecha || '').trim()}|${String(row.documento || '').trim()}|${String(row.sedeCodigo || row.sedeNombre || '').trim()}`;
}

function isISODate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '').trim());
}

function normalizeText(value) {
  return String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\s+/g, ' ').trim().toLowerCase();
}
