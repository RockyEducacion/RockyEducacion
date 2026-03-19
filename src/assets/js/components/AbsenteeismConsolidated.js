import { el, qs, enableSectionToggles } from '../utils/dom.js';

export const AbsenteeismConsolidated = (mount, deps = {}) => {
  const today = todayBogota();
  const monthStart = startOfMonth(today);
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Ausentismo Consolidado']),
    el('p', { className: 'text-muted' }, ['Consolidado por sede para periodos de hasta 31 dias, con vista diaria y totalizada.']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [el('label', { className: 'label' }, ['Desde']), el('input', { id: 'dateFrom', className: 'input', type: 'date', value: monthStart, max: today })]),
      el('div', {}, [el('label', { className: 'label' }, ['Hasta']), el('input', { id: 'dateTo', className: 'input', type: 'date', value: today, max: today })]),
      el('button', { id: 'btnRun', className: 'btn btn--primary', type: 'button' }, ['Consultar']),
      el('button', { id: 'btnExportSummary', className: 'btn', type: 'button' }, ['Exportar consolidado Excel']),
      el('button', { id: 'btnExportDaily', className: 'btn', type: 'button' }, ['Exportar detalle Excel']),
      el('span', { id: 'msg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Totales por sede']),
      el('div', { className: 'form-row mt-1' }, [
        el('div', {}, [
          el('label', { className: 'label' }, ['Buscar sede']),
          el('input', { id: 'summarySearch', className: 'input', placeholder: 'Sede, codigo, zona o dependencia...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Dependencia']),
          el('select', { id: 'summaryDependencyFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Zona']),
          el('select', { id: 'summaryZoneFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Estado']),
          el('select', { id: 'summaryStateFilter', className: 'input' }, [
            el('option', { value: 'all' }, ['Todos']),
            el('option', { value: 'faltantes' }, ['Con faltantes']),
            el('option', { value: 'sobrantes' }, ['Con sobrantes']),
            el('option', { value: 'completa' }, ['Completa'])
          ])
        ])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'tblSummary' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-summary': 'sedeNombre', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-summary': 'zonaNombre', style: 'cursor:pointer' }, ['Zona']),
            el('th', { 'data-sort-summary': 'dependenciaNombre', style: 'cursor:pointer' }, ['Dependencia']),
            el('th', { 'data-sort-summary': 'dias', style: 'cursor:pointer' }, ['Dias']),
            el('th', { 'data-sort-summary': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
            el('th', { 'data-sort-summary': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
            el('th', { 'data-sort-summary': 'registrados', style: 'cursor:pointer' }, ['Registrados']),
            el('th', { 'data-sort-summary': 'faltantes', style: 'cursor:pointer' }, ['Faltantes']),
            el('th', { 'data-sort-summary': 'sobrantes', style: 'cursor:pointer' }, ['Sobrantes']),
            el('th', { 'data-sort-summary': 'estado', style: 'cursor:pointer' }, ['Estado']),
            el('th', {}, ['Detalle'])
          ])]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'summaryTotals', className: 'text-muted' }, ['Total sedes: 0, Dias: 0, Planeados: 0, Contratados: 0, Registrados: 0, Faltantes: 0, Sobrantes: 0'])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { id: 'dailyTitle', className: 'section-title' }, ['Detalle diario por sede']),
      el('div', { className: 'form-row mt-1' }, [
        el('div', {}, [
          el('label', { className: 'label' }, ['Buscar detalle']),
          el('input', { id: 'dailySearch', className: 'input', placeholder: 'Fecha, sede, codigo, zona o dependencia...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Dependencia']),
          el('select', { id: 'dailyDependencyFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Zona']),
          el('select', { id: 'dailyZoneFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Estado']),
          el('select', { id: 'dailyStateFilter', className: 'input' }, [
            el('option', { value: 'all' }, ['Todos']),
            el('option', { value: 'faltantes' }, ['Con faltantes']),
            el('option', { value: 'sobrantes' }, ['Con sobrantes']),
            el('option', { value: 'completa' }, ['Completa'])
          ])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Sede seleccionada']),
          el('button', { id: 'btnClearSelectedSede', className: 'btn', type: 'button' }, ['Ver todas'])
        ])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'tblDaily' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-daily': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
            el('th', { 'data-sort-daily': 'sedeNombre', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-daily': 'zonaNombre', style: 'cursor:pointer' }, ['Zona']),
            el('th', { 'data-sort-daily': 'dependenciaNombre', style: 'cursor:pointer' }, ['Dependencia']),
            el('th', { 'data-sort-daily': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
            el('th', { 'data-sort-daily': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
            el('th', { 'data-sort-daily': 'registrados', style: 'cursor:pointer' }, ['Registrados']),
            el('th', { 'data-sort-daily': 'faltantes', style: 'cursor:pointer' }, ['Faltantes']),
            el('th', { 'data-sort-daily': 'sobrantes', style: 'cursor:pointer' }, ['Sobrantes']),
            el('th', { 'data-sort-daily': 'estado', style: 'cursor:pointer' }, ['Estado'])
          ])]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'dailyTotals', className: 'text-muted' }, ['Filas: 0, Planeados: 0, Contratados: 0, Registrados: 0, Faltantes: 0, Sobrantes: 0'])
    ])
  ]);

  const msg = qs('#msg', ui);
  let dailyRows = [];
  let summaryRows = [];
  let summarySortKey = 'sedeNombre';
  let summarySortDir = 1;
  let dailySortKey = 'fecha';
  let dailySortDir = -1;
  let selectedSedeCode = '';

  qs('#btnRun', ui).addEventListener('click', run);
  qs('#btnExportSummary', ui).addEventListener('click', exportSummaryExcel);
  qs('#btnExportDaily', ui).addEventListener('click', exportDailyExcel);
  qs('#summarySearch', ui).addEventListener('input', renderSummary);
  qs('#summaryDependencyFilter', ui).addEventListener('change', renderSummary);
  qs('#summaryZoneFilter', ui).addEventListener('change', renderSummary);
  qs('#summaryStateFilter', ui).addEventListener('change', renderSummary);
  qs('#dailySearch', ui).addEventListener('input', renderDaily);
  qs('#dailyDependencyFilter', ui).addEventListener('change', renderDaily);
  qs('#dailyZoneFilter', ui).addEventListener('change', renderDaily);
  qs('#dailyStateFilter', ui).addEventListener('change', renderDaily);
  qs('#btnClearSelectedSede', ui).addEventListener('click', () => {
    selectedSedeCode = '';
    qs('#dailyTitle', ui).textContent = 'Detalle diario por sede';
    renderDaily();
  });

  ui.querySelectorAll('#tblSummary th[data-sort-summary]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-summary') || '').trim();
      if (!key) return;
      if (summarySortKey === key) summarySortDir *= -1;
      else {
        summarySortKey = key;
        summarySortDir = 1;
      }
      renderSummary();
    });
  });

  ui.querySelectorAll('#tblDaily th[data-sort-daily]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-daily') || '').trim();
      if (!key) return;
      if (dailySortKey === key) dailySortDir *= -1;
      else {
        dailySortKey = key;
        dailySortDir = 1;
      }
      renderDaily();
    });
  });

  async function run() {
    const dateFrom = String(qs('#dateFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#dateTo', ui)?.value || '').trim();
    const validationError = validateRange(dateFrom, dateTo);
    if (validationError) {
      msg.textContent = validationError;
      return;
    }

    msg.textContent = 'Consultando consolidado...';
    try {
      const [sedeClosureRows, closedDays] = await Promise.all([
        deps.listDailySedeClosuresRange?.(dateFrom, dateTo) || [],
        deps.listClosedOperationDaysRange?.(dateFrom, dateTo) || [],
      ]);

      const requestedDateRange = getDateRange(dateFrom, dateTo);
      selectedSedeCode = '';
      qs('#dailyTitle', ui).textContent = 'Detalle diario por sede';
      const closedDaySet = new Set(Array.isArray(closedDays) ? closedDays : []);
      const dateRange = requestedDateRange.filter((fecha) => closedDaySet.has(fecha));
      if (!dateRange.length) {
        dailyRows = [];
        summaryRows = [];
        refreshZoneFilters();
        renderSummary();
        renderDaily();
        msg.textContent = 'No hay dias cerrados en el rango consultado. Este consolidado usa solo informacion historica cerrada.';
        return;
      }
      dailyRows = (sedeClosureRows || [])
        .filter((row) => closedDaySet.has(String(row.fecha || '').trim()))
        .map((row) => ({
          fecha: String(row.fecha || '').trim(),
          sedeCodigo: String(row.sedeCodigo || '').trim(),
          sedeNombre: String(row.sedeNombre || row.sedeCodigo || '-').trim() || '-',
          zonaCodigo: String(row.zonaCodigo || '').trim(),
          zonaNombre: String(row.zonaNombre || '').trim() || 'Sin zona',
          dependenciaCodigo: String(row.dependenciaCodigo || '').trim(),
          dependenciaNombre: String(row.dependenciaNombre || '').trim() || 'Sin dependencia',
          planeados: Number(row.planeados || 0),
          contratados: Number(row.contratados || 0),
          registrados: Number(row.registrados || 0),
          faltantes: Number(row.faltantes || 0),
          sobrantes: Number(row.sobrantes || 0),
          estado: resolveSedeState(row.faltantes, row.sobrantes)
        }));

      dailyRows.sort((a, b) => {
        const byDate = String(a.fecha || '').localeCompare(String(b.fecha || ''));
        if (byDate !== 0) return byDate;
        return String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || ''));
      });

      const summaryBySede = new Map();
      dailyRows.forEach((row) => {
        if (!summaryBySede.has(row.sedeCodigo)) {
          summaryBySede.set(row.sedeCodigo, {
            sedeCodigo: row.sedeCodigo,
            sedeNombre: row.sedeNombre,
            zonaCodigo: row.zonaCodigo,
            zonaNombre: row.zonaNombre,
            dependenciaCodigo: row.dependenciaCodigo,
            dependenciaNombre: row.dependenciaNombre,
            dias: 0,
            planeados: 0,
            contratados: 0,
            registrados: 0,
            faltantes: 0,
            sobrantes: 0,
            diasConFaltantes: 0,
            diasConSobrantes: 0
          });
        }
        const acc = summaryBySede.get(row.sedeCodigo);
        acc.dias += 1;
        acc.planeados += Number(row.planeados || 0);
        acc.contratados += Number(row.contratados || 0);
        acc.registrados += Number(row.registrados || 0);
        acc.faltantes += Number(row.faltantes || 0);
        acc.sobrantes += Number(row.sobrantes || 0);
        if (Number(row.faltantes || 0) > 0) acc.diasConFaltantes += 1;
        if (Number(row.sobrantes || 0) > 0) acc.diasConSobrantes += 1;
      });

      summaryRows = Array.from(summaryBySede.values()).map((row) => ({
        ...row,
        estado: resolveSedeState(row.faltantes, row.sobrantes)
      }));

      refreshZoneFilters();
      renderSummary();
      renderDaily();
      msg.textContent = `Consulta OK. Dias cerrados: ${dateRange.length}, sedes: ${summaryRows.length}, filas historicas: ${dailyRows.length}`;
    } catch (error) {
      msg.textContent = `Error: ${error?.message || error}`;
    }
  }

  function renderSummary() {
    const rows = getFilteredSummaryRows();
    const sortedRows = sortRows(rows, summarySortKey, summarySortDir);
    const tbody = qs('#tblSummary tbody', ui);
    if (!sortedRows.length) {
      tbody.replaceChildren(el('tr', {}, [el('td', { colSpan: 11, className: 'text-muted' }, ['Sin sedes para los filtros actuales.'])]));
    } else {
      tbody.replaceChildren(...sortedRows.map((row) => {
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver dias']);
      btn.addEventListener('click', () => {
        selectedSedeCode = row.sedeCodigo;
        qs('#dailyTitle', ui).textContent = `Detalle diario por sede: ${row.sedeNombre || '-'}`;
        renderDaily();
      });
      return el('tr', {}, [
        el('td', {}, [row.sedeNombre || '-']),
        el('td', {}, [row.zonaNombre || 'Sin zona']),
        el('td', {}, [row.dependenciaNombre || 'Sin dependencia']),
        el('td', {}, [String(row.dias || 0)]),
        el('td', {}, [String(row.planeados || 0)]),
        el('td', {}, [String(row.contratados || 0)]),
        el('td', {}, [String(row.registrados || 0)]),
        el('td', {}, [String(row.faltantes || 0)]),
        el('td', {}, [String(row.sobrantes || 0)]),
        el('td', {}, [row.estado || '-']),
        el('td', {}, [btn])
      ]);
      }));
    }

    const totals = rows.reduce((acc, row) => ({
      dias: acc.dias + Number(row.dias || 0),
      planeados: acc.planeados + Number(row.planeados || 0),
      contratados: acc.contratados + Number(row.contratados || 0),
      registrados: acc.registrados + Number(row.registrados || 0),
      faltantes: acc.faltantes + Number(row.faltantes || 0),
      sobrantes: acc.sobrantes + Number(row.sobrantes || 0)
    }), { dias: 0, planeados: 0, contratados: 0, registrados: 0, faltantes: 0, sobrantes: 0 });

    qs('#summaryTotals', ui).textContent = `Total sedes: ${rows.length}, Dias: ${totals.dias}, Planeados: ${totals.planeados}, Contratados: ${totals.contratados}, Registrados: ${totals.registrados}, Faltantes: ${totals.faltantes}, Sobrantes: ${totals.sobrantes}`;
    updateSortIndicators(ui, '#tblSummary th[data-sort-summary]', 'data-sort-summary', summarySortKey, summarySortDir);
  }

  function renderDaily() {
    const rows = getFilteredDailyRows();
    const sortedRows = sortRows(rows, dailySortKey, dailySortDir);
    const tbody = qs('#tblDaily tbody', ui);
    if (!sortedRows.length) {
      tbody.replaceChildren(el('tr', {}, [el('td', { colSpan: 10, className: 'text-muted' }, ['Sin filas diarias para los filtros actuales.'])]));
    } else {
      tbody.replaceChildren(...sortedRows.map((row) => el('tr', {}, [
        el('td', {}, [row.fecha || '-']),
        el('td', {}, [row.sedeNombre || '-']),
        el('td', {}, [row.zonaNombre || 'Sin zona']),
        el('td', {}, [row.dependenciaNombre || 'Sin dependencia']),
        el('td', {}, [String(row.planeados || 0)]),
        el('td', {}, [String(row.contratados || 0)]),
        el('td', {}, [String(row.registrados || 0)]),
        el('td', {}, [String(row.faltantes || 0)]),
        el('td', {}, [String(row.sobrantes || 0)]),
        el('td', {}, [row.estado || '-'])
      ])));
    }

    const totals = rows.reduce((acc, row) => ({
      planeados: acc.planeados + Number(row.planeados || 0),
      contratados: acc.contratados + Number(row.contratados || 0),
      registrados: acc.registrados + Number(row.registrados || 0),
      faltantes: acc.faltantes + Number(row.faltantes || 0),
      sobrantes: acc.sobrantes + Number(row.sobrantes || 0)
    }), { planeados: 0, contratados: 0, registrados: 0, faltantes: 0, sobrantes: 0 });

    qs('#dailyTotals', ui).textContent = `Filas: ${rows.length}, Planeados: ${totals.planeados}, Contratados: ${totals.contratados}, Registrados: ${totals.registrados}, Faltantes: ${totals.faltantes}, Sobrantes: ${totals.sobrantes}`;
    updateSortIndicators(ui, '#tblDaily th[data-sort-daily]', 'data-sort-daily', dailySortKey, dailySortDir);
  }

  function refreshZoneFilters() {
    const zones = Array.from(
      new Map(
        summaryRows.map((row) => [String(row.zonaCodigo || '').trim(), String(row.zonaNombre || 'Sin zona').trim() || 'Sin zona'])
      ).entries()
    )
      .filter(([code]) => code)
      .sort((a, b) => String(a[1] || '').localeCompare(String(b[1] || '')));

    refreshSingleZoneFilter(qs('#summaryZoneFilter', ui), zones);
    refreshSingleZoneFilter(qs('#dailyZoneFilter', ui), zones);
    refreshDependencyFilters();
  }

  function getFilteredSummaryRows() {
    const term = normalizeText(qs('#summarySearch', ui)?.value || '');
    const dependencyFilter = String(qs('#summaryDependencyFilter', ui)?.value || '').trim();
    const zoneFilter = String(qs('#summaryZoneFilter', ui)?.value || '').trim();
    const stateFilter = String(qs('#summaryStateFilter', ui)?.value || 'all').trim();
    return summaryRows.filter((row) => {
      const blob = `${row.sedeNombre || ''} ${row.sedeCodigo || ''} ${row.zonaNombre || ''} ${row.dependenciaNombre || ''}`;
      if (term && !normalizeText(blob).includes(term)) return false;
      if (dependencyFilter && String(row.dependenciaCodigo || '').trim() !== dependencyFilter) return false;
      if (zoneFilter && String(row.zonaCodigo || '').trim() !== zoneFilter) return false;
      return matchesStateFilter(row, stateFilter);
    });
  }

  function getFilteredDailyRows() {
    const term = normalizeText(qs('#dailySearch', ui)?.value || '');
    const dependencyFilter = String(qs('#dailyDependencyFilter', ui)?.value || '').trim();
    const zoneFilter = String(qs('#dailyZoneFilter', ui)?.value || '').trim();
    const stateFilter = String(qs('#dailyStateFilter', ui)?.value || 'all').trim();
    return dailyRows.filter((row) => {
      if (selectedSedeCode && row.sedeCodigo !== selectedSedeCode) return false;
      const blob = `${row.fecha || ''} ${row.sedeNombre || ''} ${row.sedeCodigo || ''} ${row.zonaNombre || ''} ${row.dependenciaNombre || ''}`;
      if (term && !normalizeText(blob).includes(term)) return false;
      if (dependencyFilter && String(row.dependenciaCodigo || '').trim() !== dependencyFilter) return false;
      if (zoneFilter && String(row.zonaCodigo || '').trim() !== zoneFilter) return false;
      return matchesStateFilter(row, stateFilter);
    });
  }

  async function exportSummaryExcel() {
    const rows = sortRows(getFilteredSummaryRows(), summarySortKey, summarySortDir);
    if (!rows.length) {
      msg.textContent = 'No hay datos consolidados para exportar con los filtros actuales.';
      return;
    }
    const btn = qs('#btnExportSummary', ui);
    try {
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(rows.map((row) => ({
        Sede: row.sedeNombre,
        'Codigo sede': row.sedeCodigo,
        Zona: row.zonaNombre,
        Dependencia: row.dependenciaNombre,
        Dias: row.dias,
        Planeados: row.planeados,
        Contratados: row.contratados,
        Registrados: row.registrados,
        Faltantes: row.faltantes,
        Sobrantes: row.sobrantes,
        Estado: row.estado
      })));
      ws['!cols'] = [{ wch: 30 }, { wch: 14 }, { wch: 22 }, { wch: 28 }, { wch: 10 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 18 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Consolidado por sede');
      mod.writeFile(wb, `ausentismo_consolidado_${safeRangeLabel()}.xlsx`);
      msg.textContent = `Excel consolidado generado. Sedes: ${rows.length}`;
    } catch (error) {
      msg.textContent = `Error exportando consolidado: ${error?.message || error}`;
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = 'Exportar consolidado Excel';
      }
    }
  }

  async function exportDailyExcel() {
    const rows = sortRows(getFilteredDailyRows(), dailySortKey, dailySortDir);
    if (!rows.length) {
      msg.textContent = 'No hay detalle diario para exportar con los filtros actuales.';
      return;
    }
    const btn = qs('#btnExportDaily', ui);
    try {
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(rows.map((row) => ({
        Fecha: row.fecha,
        Sede: row.sedeNombre,
        'Codigo sede': row.sedeCodigo,
        Zona: row.zonaNombre,
        Dependencia: row.dependenciaNombre,
        Planeados: row.planeados,
        Contratados: row.contratados,
        Registrados: row.registrados,
        Faltantes: row.faltantes,
        Sobrantes: row.sobrantes,
        Estado: row.estado
      })));
      ws['!cols'] = [{ wch: 12 }, { wch: 30 }, { wch: 14 }, { wch: 22 }, { wch: 28 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 18 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Detalle diario');
      mod.writeFile(wb, `ausentismo_consolidado_detalle_${safeRangeLabel()}.xlsx`);
      msg.textContent = `Excel detalle generado. Filas: ${rows.length}`;
    } catch (error) {
      msg.textContent = `Error exportando detalle: ${error?.message || error}`;
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = 'Exportar detalle Excel';
      }
    }
  }

  function safeRangeLabel() {
    const from = String(qs('#dateFrom', ui)?.value || 'desde').trim();
    const to = String(qs('#dateTo', ui)?.value || 'hasta').trim();
    return `${from}_a_${to}`.replace(/[^\d_a-zA-Z-]/g, '_');
  }

  function refreshDependencyFilters() {
    const dependencies = Array.from(
      new Map(
        summaryRows.map((row) => [String(row.dependenciaCodigo || '').trim(), String(row.dependenciaNombre || 'Sin dependencia').trim() || 'Sin dependencia'])
      ).entries()
    )
      .filter(([code]) => code)
      .sort((a, b) => String(a[1] || '').localeCompare(String(b[1] || '')));

    refreshSingleOptionFilter(qs('#summaryDependencyFilter', ui), dependencies);
    refreshSingleOptionFilter(qs('#dailyDependencyFilter', ui), dependencies);
  }

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  run();
  return () => {};
};

function refreshSingleZoneFilter(selectNode, zones = []) {
  refreshSingleOptionFilter(selectNode, zones);
}

function refreshSingleOptionFilter(selectNode, options = []) {
  if (!selectNode) return;
  const previous = String(selectNode.value || '').trim();
  selectNode.replaceChildren(
    el('option', { value: '' }, ['Todas']),
    ...options.map(([code, name]) => el('option', { value: code, selected: code === previous }, [`${name} (${code})`]))
  );
}

function validateRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return 'Selecciona ambas fechas.';
  if (dateFrom > dateTo) return 'La fecha inicial no puede ser mayor a la final.';
  if (diffInDays(dateFrom, dateTo) > 30) return 'El periodo no puede ser mayor a 31 dias.';
  return '';
}

function diffInDays(dateFrom, dateTo) {
  const start = new Date(`${dateFrom}T00:00:00Z`);
  const end = new Date(`${dateTo}T00:00:00Z`);
  return Math.round((end.getTime() - start.getTime()) / 86400000);
}

function getDateRange(dateFrom, dateTo) {
  const out = [];
  const cursor = new Date(`${dateFrom}T00:00:00Z`);
  const end = new Date(`${dateTo}T00:00:00Z`);
  while (cursor.getTime() <= end.getTime()) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

function startOfMonth(isoDate) {
  const [year, month] = String(isoDate || '').split('-');
  if (!year || !month) return isoDate;
  return `${year}-${month}-01`;
}

function resolveSedeState(faltantes, sobrantes) {
  if (Number(faltantes || 0) > 0) return 'Con faltantes';
  if (Number(sobrantes || 0) > 0) return 'Con sobrantes';
  return 'Completa';
}

function matchesStateFilter(row, stateFilter) {
  if (stateFilter === 'faltantes') return Number(row.faltantes || 0) > 0;
  if (stateFilter === 'sobrantes') return Number(row.sobrantes || 0) > 0;
  if (stateFilter === 'completa') return Number(row.faltantes || 0) === 0 && Number(row.sobrantes || 0) === 0;
  return true;
}

function sortRows(rows, key, dir) {
  return [...(rows || [])].sort((a, b) => {
    const valueA = sortValue(a, key);
    const valueB = sortValue(b, key);
    if (valueA === valueB) return 0;
    return valueA > valueB ? dir : -dir;
  });
}

function sortValue(row, key) {
  const value = row?.[key];
  if (typeof value === 'number') return value;
  return String(value ?? '').toLowerCase();
}

function updateSortIndicators(rootNode, selector, attrName, activeKey, dir) {
  (rootNode?.querySelectorAll(selector) || []).forEach((th) => {
    const base = th.dataset.baseLabel || th.textContent.replace(/\s[\^v\u25B2\u25BC]$/, '');
    th.dataset.baseLabel = base;
    const key = String(th.getAttribute(attrName) || '').trim();
    th.textContent = key && key === activeKey ? `${base} ${dir === 1 ? '\u25B2' : '\u25BC'}` : base;
  });
}

function normalizeText(v) {
  return String(v || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function parseOperatorCount(value) {
  if (value == null) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : 0;
  const raw = String(value).trim();
  if (!raw) return 0;
  const digits = raw.replace(/[^\d]/g, '');
  if (!digits) return 0;
  const parsed = Number(digits);
  return Number.isFinite(parsed) ? parsed : 0;
}

function todayBogota() {
  const fmt = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota', year: 'numeric', month: '2-digit', day: '2-digit' });
  return fmt.format(new Date());
}
