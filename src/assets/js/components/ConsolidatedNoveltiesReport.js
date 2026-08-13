import { el, qs } from '../utils/dom.js';
import { can, PERMS } from '../permissions.js';

export const ConsolidatedNoveltiesReport = (mount, deps = {}) => {
  const canExport = can(PERMS.EXPORT_REPORTS_NOVELTIES_CONSOLIDATED);
  let generatedRows = [];
  let running = false;
  let selectedDateFrom = `${todayBogota().slice(0, 7)}-01`;
  let selectedDateTo = todayBogota();
  let sortKey = 'fecha';
  let sortDir = -1;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Consolidado Novedades']),
    el('p', { className: 'text-muted' }, ['Periodo de tiempo con personas reportadas en novedades distintas de Trabajando y Compensatorio.']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label', for: 'noveltiesReportDateFrom' }, ['Desde']),
        el('input', { id: 'noveltiesReportDateFrom', className: 'input', type: 'date', value: selectedDateFrom, max: todayBogota(), style: 'max-width:180px' })
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'noveltiesReportDateTo' }, ['Hasta']),
        el('input', { id: 'noveltiesReportDateTo', className: 'input', type: 'date', value: selectedDateTo, max: todayBogota(), style: 'max-width:180px' })
      ]),
      el('button', { id: 'btnGenerateNoveltiesReport', className: 'btn btn--primary', type: 'button' }, ['Generar reporte']),
      el('button', { id: 'btnExportNoveltiesReport', className: 'btn', type: 'button', disabled: true, title: canExport ? '' : 'Modo consulta: no puedes exportar.' }, ['Exportar Excel']),
      el('span', { id: 'noveltiesReportMsg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'table-wrap mt-2' }, [
      el('table', { className: 'table', id: 'noveltiesReportTable' }, [
        el('thead', {}, [el('tr', {}, [
          el('th', { 'data-sort-novelties-report': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
          el('th', { 'data-sort-novelties-report': 'cedula', style: 'cursor:pointer' }, ['Cedula']),
          el('th', { 'data-sort-novelties-report': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
          el('th', { 'data-sort-novelties-report': 'sede', style: 'cursor:pointer' }, ['Sede']),
          el('th', { 'data-sort-novelties-report': 'novedad', style: 'cursor:pointer' }, ['Novedad']),
          el('th', { 'data-sort-novelties-report': 'cobertura', style: 'cursor:pointer' }, ['Reemplazo/Ausentismo'])
        ])]),
        el('tbody', { id: 'noveltiesReportTbody' }, [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin generar.'])])])
      ])
    ]),
    el('p', { id: 'noveltiesReportTotal', className: 'text-muted mt-2' }, ['Selecciona el periodo y genera el reporte.'])
  ]);

  qs('#btnGenerateNoveltiesReport', ui)?.addEventListener('click', generateReport);
  qs('#btnExportNoveltiesReport', ui)?.addEventListener('click', exportExcel);
  ui.querySelectorAll('#noveltiesReportTable th[data-sort-novelties-report]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-novelties-report') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else {
        sortKey = key;
        sortDir = key === 'fecha' ? -1 : 1;
      }
      renderTable();
    });
  });

  mount.replaceChildren(ui);
  updateSortIndicators();
  return ui;

  function setMessage(text) {
    qs('#noveltiesReportMsg', ui).textContent = text || ' ';
  }

  function todayBogota() {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
  }

  function normalizeText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  function displayNovedadLabel(row = {}) {
    const code = String(row.novedadCodigo || (/^\d+$/.test(String(row.novedad || '').trim()) ? String(row.novedad || '').trim() : '')).trim();
    const raw = String(row.novedadNombre || row.novedad || '-').trim();
    if (code === '1') return 'Trabajando';
    if (code === '2') return 'Accidente Laboral';
    if (code === '3') return 'Enfermedad General';
    if (code === '4') return 'Calamidad';
    if (code === '5') return 'Licencia No Remunerada';
    if (code === '7') return 'Compensatorio';
    if (code === '9') return 'Vacaciones';
    return raw || '-';
  }

  function noveltyReplacementKeys(row = {}) {
    const fecha = String(row?.fecha || '').trim();
    const employeeId = String(row?.employeeId || row?.empleadoId || '').trim();
    const documento = String(row?.documento || '').trim();
    const keys = [];
    if (fecha && employeeId) keys.push(`${fecha}|id|${employeeId}`);
    if (fecha && documento) keys.push(`${fecha}|doc|${documento}`);
    return keys;
  }

  function buildReplacementMap(rows = []) {
    const map = new Map();
    (rows || []).forEach((row) => {
      noveltyReplacementKeys(row).forEach((key) => {
        if (key) map.set(key, row);
      });
    });
    return map;
  }

  function coverageDetail(row = {}, replacementMap = new Map()) {
    const replacement = noveltyReplacementKeys(row).map((key) => replacementMap.get(key)).find(Boolean) || null;
    const decision = String(replacement?.decision || '').trim().toLowerCase();
    if (decision === 'reemplazo') {
      const name = String(replacement?.supernumerarioNombre || '').trim();
      const doc = String(replacement?.supernumerarioDocumento || '').trim();
      if (name && doc) return `${name} (${doc})`;
      if (name) return name;
      if (doc) return doc;
      return 'Reemplazo confirmado';
    }
    if (decision === 'ausentismo') return 'Ausentismo confirmado';
    if (String(row?.decisionCobertura || '').trim().toLowerCase() === 'reemplazo') return 'Reemplazo pendiente';
    if (String(row?.decisionCobertura || '').trim().toLowerCase() === 'ausentismo' || row?.cuentaPagoServicio === false) return 'Ausentismo pendiente';
    return 'No aplica';
  }

  function normalizeRows(statusRows = [], replacementRows = []) {
    const replacementMap = buildReplacementMap(replacementRows);
    return (statusRows || [])
      .filter((row) => {
        const code = String(row?.novedadCodigo || '').trim();
        const label = normalizeText(displayNovedadLabel(row));
        if (code === '1' || code === '7') return false;
        if (!code && (label === 'trabajando' || label === 'compensatorio' || label === '-')) return false;
        return Boolean(code || label);
      })
      .map((row) => ({
        fecha: String(row?.fecha || '').trim() || '-',
        cedula: String(row?.documento || '').trim() || '-',
        nombre: String(row?.nombre || '').trim() || '-',
        sede: String(row?.sedeNombreSnapshot || row?.sedeCodigo || '-').trim() || '-',
        novedad: displayNovedadLabel(row),
        cobertura: coverageDetail(row, replacementMap)
      }))
      .sort((a, b) => {
        const byDate = String(a.fecha || '').localeCompare(String(b.fecha || ''));
        if (byDate !== 0) return byDate;
        const byName = String(a.nombre || '').localeCompare(String(b.nombre || ''));
        if (byName !== 0) return byName;
        return String(a.cedula || '').localeCompare(String(b.cedula || ''));
      });
  }

  function sortRows(rows = [], key = '', dir = 1) {
    return [...(rows || [])].sort((a, b) => {
      const av = String(a?.[key] ?? '').toLowerCase();
      const bv = String(b?.[key] ?? '').toLowerCase();
      if (av === bv) return 0;
      return av > bv ? dir : -dir;
    });
  }

  function renderRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin novedades para el periodo seleccionado.'])])];
    return rows.map((row) => el('tr', {}, [
      el('td', {}, [row.fecha]),
      el('td', {}, [row.cedula]),
      el('td', {}, [row.nombre]),
      el('td', {}, [row.sede]),
      el('td', {}, [row.novedad]),
      el('td', {}, [row.cobertura])
    ]));
  }

  function renderTable() {
    const tbody = qs('#noveltiesReportTbody', ui);
    if (tbody) tbody.replaceChildren(...renderRows(sortRows(generatedRows, sortKey, sortDir)));
    updateSortIndicators();
  }

  function updateSortIndicators() {
    ui.querySelectorAll('#noveltiesReportTable th[data-sort-novelties-report]').forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v\u25B2\u25BC]$/, '');
      th.dataset.baseLabel = base;
      th.textContent = String(th.getAttribute('data-sort-novelties-report') || '').trim() === sortKey ? `${base} ${sortDir === 1 ? '\u25B2' : '\u25BC'}` : base;
    });
  }

  async function generateReport() {
    if (running) return;
    const dateFrom = String(qs('#noveltiesReportDateFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#noveltiesReportDateTo', ui)?.value || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateFrom) || !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      setMessage('Selecciona un rango valido para generar el consolidado de novedades.');
      return;
    }
    if (dateFrom > dateTo) {
      setMessage('La fecha inicial no puede ser mayor que la fecha final.');
      return;
    }
    running = true;
    selectedDateFrom = dateFrom;
    selectedDateTo = dateTo;
    const btnGenerate = qs('#btnGenerateNoveltiesReport', ui);
    const btnExport = qs('#btnExportNoveltiesReport', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const [statusRows, replacementRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(dateFrom, dateTo) || [],
        deps.listImportReplacementsRange?.(dateFrom, dateTo) || []
      ]);
      generatedRows = normalizeRows(statusRows, replacementRows);
      const peopleCount = new Set(generatedRows.map((row) => `${row.cedula}|${row.nombre}`)).size;
      const totalNode = qs('#noveltiesReportTotal', ui);
      if (totalNode) totalNode.textContent = `Periodo: ${dateFrom} a ${dateTo} | Registros: ${generatedRows.length} | Personas: ${peopleCount}`;
      renderTable();
      if (btnExport) btnExport.disabled = !canExport || generatedRows.length === 0;
      setMessage(' ');
    } catch (error) {
      setMessage(`Error al generar consolidado de novedades: ${error?.message || error}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function exportExcel() {
    const btn = qs('#btnExportNoveltiesReport', ui);
    try {
      if (!generatedRows.length) throw new Error('Primero genera el reporte.');
      if (!canExport) throw new Error('No tienes permiso para exportar este reporte.');
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(generatedRows.map((row) => ({
        Fecha: row.fecha,
        Cedula: row.cedula,
        Nombre: row.nombre,
        Sede: row.sede,
        Novedad: row.novedad,
        'Reemplazo/Ausentismo': row.cobertura
      })));
      ws['!cols'] = [{ wch: 12 }, { wch: 18 }, { wch: 30 }, { wch: 30 }, { wch: 28 }, { wch: 24 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Consolidado Novedades');
      mod.writeFile(wb, `reporte_consolidado_novedades_${selectedDateFrom}_a_${selectedDateTo}.xlsx`);
      setMessage(`Excel generado correctamente. Registros: ${generatedRows.length}`);
    } catch (error) {
      setMessage(`Error al generar Excel: ${error?.message || error}`);
    } finally {
      if (btn) {
        btn.disabled = !canExport || generatedRows.length === 0;
        btn.textContent = 'Exportar Excel';
      }
    }
  }
};
