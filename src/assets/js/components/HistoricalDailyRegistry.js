import { el, qs, enableSectionToggles } from '../utils/dom.js';
import { createTablePagination } from '../utils/pagination.js';

export const HistoricalDailyRegistry = (mount, deps = {}) => {
  const maxDate = yesterdayBogota();
  let selectedDate = maxDate;
  let generatedRows = [];
  let running = false;
  let sortKey = 'hora';
  let sortDir = -1;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Historico de Registro Diario']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label' }, ['Fecha']),
        el('input', {
          id: 'historicalDailyDate',
          className: 'input',
          type: 'date',
          value: selectedDate,
          max: maxDate
        }, [])
      ]),
      el('button', { id: 'btnGenerateHistoricalDaily', className: 'btn btn--primary', type: 'button' }, ['Consultar fecha']),
      el('button', { id: 'btnExportHistoricalDaily', className: 'btn', type: 'button', disabled: true }, ['Exportar Excel']),
      el('span', { id: 'historicalDailyMsg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Registros del dia']),
      el('div', { className: 'table-wrap' }, [
        el('table', { className: 'table', id: 'tblHistoricalDaily' }, [
          el('thead', {}, [
            el('tr', {}, [
              el('th', { 'data-sort-historical': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
              el('th', { 'data-sort-historical': 'hora', style: 'cursor:pointer' }, ['Hora']),
              el('th', { 'data-sort-historical': 'cedula', style: 'cursor:pointer' }, ['Cedula']),
              el('th', { 'data-sort-historical': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
              el('th', { 'data-sort-historical': 'sede', style: 'cursor:pointer' }, ['Sede']),
              el('th', { 'data-sort-historical': 'novedad', style: 'cursor:pointer' }, ['Novedad']),
              el('th', { 'data-sort-historical': 'estado', style: 'cursor:pointer' }, ['Estado'])
            ])
          ]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'historicalDailyTotal', className: 'text-muted' }, ['Total registros del dia: 0'])
    ])
  ]);

  const paginator = createTablePagination(ui, { id: 'historicalDaily', after: '#tblHistoricalDaily', onChange: syncRows });
  qs('#btnGenerateHistoricalDaily', ui)?.addEventListener('click', generateReport);
  qs('#btnExportHistoricalDaily', ui)?.addEventListener('click', exportExcel);
  qs('#historicalDailyDate', ui)?.addEventListener('change', () => {
    generatedRows = [];
    paginator.reset();
    syncRows();
    setMessage(' ');
  });
  ui.querySelectorAll('#tblHistoricalDaily th[data-sort-historical]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-historical') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else {
        sortKey = key;
        sortDir = 1;
      }
      paginator.reset();
      syncRows();
    });
  });

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  syncRows();
  return ui;

  function setMessage(text) {
    qs('#historicalDailyMsg', ui).textContent = text || ' ';
  }

  function todayBogota() {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
  }

  function yesterdayBogota() {
    const cursor = new Date(`${todayBogota()}T00:00:00Z`);
    cursor.setUTCDate(cursor.getUTCDate() - 1);
    return cursor.toISOString().slice(0, 10);
  }

  function attendanceKey(item = {}) {
    return [
      String(item?.fecha || '').trim(),
      String(item?.employeeId || item?.empleadoId || '').trim(),
      String(item?.documento || '').trim()
    ].join('|');
  }

  function statusDetailState(row = {}) {
    const estadoDia = String(row?.estadoDia || '').trim();
    const decision = String(row?.decisionCobertura || '').trim().toLowerCase();
    const tipoPersonal = String(row?.tipoPersonal || 'empleado').trim().toLowerCase();
    const reemplazaA = row?.reemplazaANombre || row?.reemplazaADocumento || '-';
    const reemplazadoPor = row?.reemplazadoPorNombre || row?.reemplazadoPorDocumento || '-';

    if (tipoPersonal === 'supernumerario' && estadoDia === 'trabajado_reemplazo') {
      return `Supernumerario reemplazando a ${reemplazaA}`;
    }
    if (decision === 'reemplazo') return `Reemplazado por ${reemplazadoPor}`;
    if (row?.asistio === true) return tipoPersonal === 'supernumerario' ? 'Trabajo supernumerario' : 'Trabajo';
    if (decision === 'ausentismo' || row?.cuentaPagoServicio === false) return 'Ausentismo';
    if (estadoDia === 'sin_registro') return 'Sin registro';
    if (estadoDia === 'incapacidad') return 'Incapacidad';
    if (estadoDia === 'vacaciones') return 'Vacaciones';
    if (estadoDia === 'compensatorio') return 'Compensatorio';
    if (estadoDia === 'ausente_con_novedad') return 'Ausente con novedad';
    if (estadoDia === 'ausente_sin_reemplazo') return 'Ausentismo';
    if (estadoDia === 'no_programado') return 'No programado';
    return estadoDia
      ? estadoDia.replace(/_/g, ' ').replace(/\b\w/g, (ch) => ch.toUpperCase())
      : '-';
  }

  function normalizeRows(fecha, statusRows = [], attendanceRows = []) {
    const attendanceByKey = new Map();
    (attendanceRows || []).forEach((item) => {
      attendanceByKey.set(attendanceKey(item), item);
    });

    return (statusRows || [])
      .slice()
      .sort((a, b) => {
        const hourA = String(attendanceByKey.get(attendanceKey(a))?.hora || '');
        const hourB = String(attendanceByKey.get(attendanceKey(b))?.hora || '');
        const byHour = hourB.localeCompare(hourA);
        if (byHour !== 0) return byHour;
        const bySede = String(a?.sedeNombreSnapshot || a?.sedeCodigo || '').localeCompare(String(b?.sedeNombreSnapshot || b?.sedeCodigo || ''));
        if (bySede !== 0) return bySede;
        return String(a?.nombre || '').localeCompare(String(b?.nombre || ''));
      })
      .map((statusRow) => {
        const attendanceRow = attendanceByKey.get(attendanceKey(statusRow)) || null;
        return {
          fecha: statusRow.fecha || fecha,
          hora: attendanceRow?.hora || '-',
          cedula: statusRow.documento || '-',
          nombre: statusRow.nombre || '-',
          sede: statusRow.sedeNombreSnapshot || statusRow.sedeCodigo || '-',
          novedad: statusRow.novedadNombre || statusRow.novedadCodigo || '-',
          estado: statusDetailState(statusRow)
        };
      });
  }

  function syncRows() {
    const tbody = qs('#tblHistoricalDaily tbody', ui);
    const totalNode = qs('#historicalDailyTotal', ui);
    const exportBtn = qs('#btnExportHistoricalDaily', ui);
    const rows = sortRows(generatedRows, sortKey, sortDir);
    const pageRows = paginator.slice(rows);
    if (tbody) tbody.replaceChildren(...renderRows(pageRows, rows.length));
    if (totalNode) totalNode.textContent = `Total registros del dia: ${generatedRows.length}`;
    if (exportBtn) exportBtn.disabled = generatedRows.length === 0;
    updateSortIndicators(ui, '#tblHistoricalDaily th[data-sort-historical]', 'data-sort-historical', sortKey, sortDir);
  }

  async function generateReport() {
    if (running) return;
    const input = qs('#historicalDailyDate', ui);
    const date = String(input?.value || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      setMessage('Selecciona una fecha valida para generar el reporte.');
      return;
    }
    if (date >= todayBogota()) {
      setMessage('Este modulo solo permite consultar fechas anteriores al dia actual.');
      return;
    }
    if (date > maxDate) {
      setMessage(`La fecha maxima permitida es ${maxDate}.`);
      return;
    }
    running = true;
    selectedDate = date;
    const btn = qs('#btnGenerateHistoricalDaily', ui);
    try {
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Consultando...';
      }
      const isClosed = await deps.isOperationDayClosed?.(date);
      if (!isClosed) throw new Error('Solo se pueden generar reportes historicos de dias cerrados.');
      const [statusRows, attendanceRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(date, date) || [],
        deps.listAttendanceRange?.(date, date) || []
      ]);
      generatedRows = normalizeRows(date, statusRows, attendanceRows);
      paginator.reset();
      syncRows();
      setMessage(`Reporte generado para ${date}. Registros: ${generatedRows.length}`);
    } catch (error) {
      generatedRows = [];
      paginator.reset();
      syncRows();
      setMessage(`Error al generar historico de registro diario: ${error?.message || error}`);
    } finally {
      running = false;
      if (btn) {
        btn.disabled = false;
        btn.textContent = 'Consultar fecha';
      }
    }
  }

  async function exportExcel() {
    const btn = qs('#btnExportHistoricalDaily', ui);
    try {
      if (!generatedRows.length) throw new Error('Primero genera el reporte.');
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(generatedRows.map((row) => ({
        Fecha: row.fecha,
        Hora: row.hora,
        Cedula: row.cedula,
        Nombre: row.nombre,
        Sede: row.sede,
        Novedad: row.novedad,
        Estado: row.estado
      })));
      ws['!cols'] = [{ wch: 12 }, { wch: 12 }, { wch: 18 }, { wch: 30 }, { wch: 26 }, { wch: 26 }, { wch: 30 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Historico registro');
      mod.writeFile(wb, `historico_registro_diario_${selectedDate}.xlsx`);
      setMessage(`Excel generado correctamente para ${selectedDate}. Registros: ${generatedRows.length}`);
    } catch (error) {
      setMessage(`Error al generar Excel: ${error?.message || error}`);
    } finally {
      if (btn) {
        btn.disabled = generatedRows.length === 0;
        btn.textContent = 'Exportar Excel';
      }
    }
  }
};

function renderRows(rows = [], totalRows = rows.length) {
  if (!totalRows) {
    return [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin registros para la fecha seleccionada.'])])];
  }
  return rows.map((row) => el('tr', {}, [
    el('td', {}, [row.fecha]),
    el('td', {}, [row.hora]),
    el('td', {}, [row.cedula]),
    el('td', {}, [row.nombre]),
    el('td', {}, [row.sede]),
    el('td', {}, [row.novedad]),
    el('td', {}, [row.estado])
  ]));
}

function sortValue(row, key) {
  const value = row?.[key];
  if (typeof value === 'number') return value;
  return String(value ?? '').toLowerCase();
}

function sortRows(rows, key, dir) {
  return [...(rows || [])].sort((a, b) => {
    const va = sortValue(a, key);
    const vb = sortValue(b, key);
    if (va === vb) return 0;
    return va > vb ? dir : -dir;
  });
}

function updateSortIndicators(scope, selector, attrName, activeKey, dir) {
  scope.querySelectorAll(selector).forEach((th) => {
    const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v▲▼?]$/, '');
    th.dataset.baseLabel = base;
    const key = String(th.getAttribute(attrName) || '').trim();
    th.textContent = key && key === activeKey ? base + ' ' + (dir === 1 ? '^' : 'v') : base;
  });
}
