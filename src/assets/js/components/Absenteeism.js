import { el, qs, enableSectionToggles } from '../utils/dom.js';

export const Absenteeism = (mount, deps = {}) => {
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Ausentismo y pago por dependencia']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [el('label', { className: 'label' }, ['Fecha']), el('input', { id: 'opDate', className: 'input', type: 'date' })]),
      el('button', { id: 'btnRun', className: 'btn btn--primary', type: 'button' }, ['Consultar fecha']),
      el('button', { id: 'btnExportSummary', className: 'btn', type: 'button' }, ['Exportar resumen Excel']),
      el('button', { id: 'btnExportSede', className: 'btn', type: 'button' }, ['Exportar sedes Excel']),
      el('button', { id: 'btnExportDetail', className: 'btn', type: 'button' }, ['Exportar detalle Excel']),
      el('span', { id: 'msg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Resumen por dependencia']),
      el('div', { className: 'table-wrap' }, [
        el('table', { className: 'table', id: 'tblDependency' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-dep': 'dependenciaNombre', style: 'cursor:pointer' }, ['Dependencia']),
            el('th', { 'data-sort-dep': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
            el('th', { 'data-sort-dep': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
            el('th', { 'data-sort-dep': 'noContratado', style: 'cursor:pointer' }, ['No contratado']),
            el('th', { 'data-sort-dep': 'novSinReemplazo', style: 'cursor:pointer' }, ['Novedad sin reemplazo']),
            el('th', { 'data-sort-dep': 'ausentismoTotal', style: 'cursor:pointer' }, ['Total ausentismo']),
            el('th', { 'data-sort-dep': 'totalPagar', style: 'cursor:pointer' }, ['Total a pagar']),
            el('th', {}, ['Detalle'])
          ])]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'totDependency', className: 'text-muted' }, ['Total dependencias - Planeados: 0, Contratados: 0, No contratado: 0, Novedad sin reemplazo: 0, Total ausentismo: 0, Total a pagar: 0'])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Resumen por sede']),
      el('div', { className: 'table-wrap' }, [
        el('table', { className: 'table', id: 'tblTotals' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-sede': 'sedeNombre', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-sede': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
            el('th', { 'data-sort-sede': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
            el('th', { 'data-sort-sede': 'noContratado', style: 'cursor:pointer' }, ['No contratado']),
            el('th', { 'data-sort-sede': 'novSinReemplazo', style: 'cursor:pointer' }, ['Novedad sin reemplazo']),
            el('th', { 'data-sort-sede': 'ausentismoTotal', style: 'cursor:pointer' }, ['Total ausentismo']),
            el('th', { 'data-sort-sede': 'totalPagar', style: 'cursor:pointer' }, ['Total a pagar']),
            el('th', {}, ['Detalle'])
          ])]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'totRange', className: 'text-muted' }, ['Total rango a pagar: 0'])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { id: 'detailTitle', className: 'section-title' }, ['Detalle dependencia']),
      el('div', { className: 'table-wrap' }, [
        el('table', { className: 'table', id: 'tblDetail' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-detail': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
            el('th', { 'data-sort-detail': 'sede', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-detail': 'documento', style: 'cursor:pointer' }, ['Documento']),
            el('th', { 'data-sort-detail': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
            el('th', { 'data-sort-detail': 'estado', style: 'cursor:pointer' }, ['Estado'])
          ])]),
          el('tbody', {})
        ])
      ])
    ])
  ]);

  const msg = qs('#msg', ui);
  qs('#opDate', ui).value = todayBogota();

  let sedeDailyRows = [];
  let dependencyRows = [];
  let totalsRows = [];
  let detailRowsCache = [];
  let employeeRowsBySede = new Map();
  let depSortKey = 'dependenciaNombre';
  let depSortDir = 1;
  let sedeSortKey = 'sedeNombre';
  let sedeSortDir = 1;
  let detailSortKey = 'fecha';
  let detailSortDir = -1;

  qs('#btnRun', ui).addEventListener('click', run);
  qs('#btnExportSummary', ui).addEventListener('click', () => exportSummaryExcel());
  qs('#btnExportSede', ui).addEventListener('click', () => exportSedeExcel());
  qs('#btnExportDetail', ui).addEventListener('click', () => exportDetailExcel());

  ui.querySelectorAll('#tblDependency th[data-sort-dep]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-dep') || '').trim();
      if (!key) return;
      if (depSortKey === key) depSortDir *= -1;
      else {
        depSortKey = key;
        depSortDir = 1;
      }
      renderDependency(qs('#opDate', ui).value);
    });
  });

  ui.querySelectorAll('#tblTotals th[data-sort-sede]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-sede') || '').trim();
      if (!key) return;
      if (sedeSortKey === key) sedeSortDir *= -1;
      else {
        sedeSortKey = key;
        sedeSortDir = 1;
      }
      renderTotals();
    });
  });

  ui.querySelectorAll('#tblDetail th[data-sort-detail]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-detail') || '').trim();
      if (!key) return;
      if (detailSortKey === key) detailSortDir *= -1;
      else {
        detailSortKey = key;
        detailSortDir = 1;
      }
      renderDetailRows();
    });
  });

  async function run() {
    const date = String(qs('#opDate', ui)?.value || '').trim();
    if (!date) {
      msg.textContent = 'Selecciona una fecha.';
      return;
    }

    const dayClosed = await deps.isOperationDayClosed?.(date);
    if (!dayClosed) {
      clearUi('La fecha seleccionada no esta cerrada. El CRUD solo muestra dias cerrados.');
      return;
    }

    msg.textContent = 'Consultando...';
    try {
      const [statusRows, sedeClosures] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(date, date) || [],
        deps.listDailySedeClosuresRange?.(date, date) || []
      ]);

      const baseRows = (Array.isArray(statusRows) ? statusRows : []).filter((row) => String(row?.tipoPersonal || '').trim() === 'empleado');
      const sedeClosuresByCode = new Map((Array.isArray(sedeClosures) ? sedeClosures : []).map((row) => [String(row?.sedeCodigo || '').trim(), row]));
      employeeRowsBySede = new Map();
      baseRows.forEach((row) => {
        const sedeCode = String(row?.sedeCodigo || '').trim();
        if (!sedeCode) return;
        if (!employeeRowsBySede.has(sedeCode)) employeeRowsBySede.set(sedeCode, []);
        employeeRowsBySede.get(sedeCode).push(row);
      });

      const fixedSnapshotCodes = (Array.isArray(sedeClosures) ? sedeClosures : [])
        .map((row) => String(row?.sedeCodigo || '').trim())
        .filter(Boolean);

      const allCodes = new Set([...fixedSnapshotCodes, ...Array.from(employeeRowsBySede.keys())]);
      sedeDailyRows = Array.from(allCodes)
        .map((sedeCode) => buildSedeDailyRow(date, sedeCode, sedeClosuresByCode.get(sedeCode) || null, employeeRowsBySede.get(sedeCode) || []))
        .filter((row) => row.planeados > 0 || row.contratados > 0 || row.noContratado > 0 || row.ausentismoTotal > 0 || row.totalPagar > 0 || row.actualCount > 0)
        .sort((a, b) => String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || '')));

      const depMap = new Map();
      sedeDailyRows.forEach((row) => {
        if (!depMap.has(row.dependenciaKey)) {
          depMap.set(row.dependenciaKey, {
            dependenciaKey: row.dependenciaKey,
            dependenciaCodigo: row.dependenciaCodigo,
            dependenciaNombre: row.dependenciaNombre,
            planeados: 0,
            contratados: 0,
            noContratado: 0,
            noRegistrado: 0,
            novSinReemplazo: 0,
            ausentismoTotal: 0,
            totalPagar: 0
          });
        }
        const target = depMap.get(row.dependenciaKey);
        target.planeados += row.planeados;
        target.contratados += row.contratados;
        target.noContratado += row.noContratado;
        target.noRegistrado += row.noRegistrado;
        target.novSinReemplazo += row.novSinReemplazo;
        target.ausentismoTotal += row.ausentismoTotal;
        target.totalPagar += row.totalPagar;
      });
      dependencyRows = Array.from(depMap.values()).sort((a, b) => String(a.dependenciaNombre || '').localeCompare(String(b.dependenciaNombre || '')));

      renderDependency(date);
      renderTotals();
      msg.textContent = 'Consulta OK. Dependencias: ' + dependencyRows.length;
    } catch (error) {
      msg.textContent = 'Error: ' + (error?.message || error);
    }
  }

  function buildSedeDailyRow(date, sedeCode, sedeSnapshot, rows) {
    const orderedRows = [...(rows || [])].sort((a, b) => String(a?.nombre || '').localeCompare(String(b?.nombre || '')));
    const scheduledRows = orderedRows.filter((row) => row?.servicioProgramado === true);
    const actualRows = orderedRows.filter((row) => row?.asistio === true || row?.asistio === false || row?.sourceIncapacityId || row?.sourceAttendanceId || row?.sourceReplacementId || row?.sourceAbsenteeismId);
    const firstRow = orderedRows[0] || null;
    const scheduled = Boolean(sedeSnapshot) || scheduledRows.length > 0;
    const planeados = parseOperatorCount(sedeSnapshot?.planeados);
    const contratados = scheduledRows.length;
    const noContratado = Math.max(0, planeados - contratados);
    const noRegistrado = scheduledRows.filter((row) => isNoRegistroAbsenteeism(row)).length;
    const novSinReemplazo = scheduledRows.filter((row) => isNoveltyWithoutReplacement(row)).length;
    const ausentismoTotal = scheduled
      ? scheduledRows.filter((row) => row?.cuentaPagoServicio !== true).length
      : actualRows.filter((row) => row?.asistio === false).length;
    const totalPagar = scheduled
      ? Math.max(0, planeados - noContratado - ausentismoTotal)
      : actualRows.filter((row) => row?.asistio === true).length;
    const dependenciaCodigo = String(sedeSnapshot?.dependenciaCodigo || firstRow?.dependenciaCodigoSnapshot || '').trim();
    const dependenciaNombre = String(sedeSnapshot?.dependenciaNombre || firstRow?.dependenciaNombreSnapshot || 'Sin dependencia').trim() || 'Sin dependencia';

    return {
      fecha: date,
      sedeCodigo: sedeCode,
      sedeNombre: String(sedeSnapshot?.sedeNombre || firstRow?.sedeNombreSnapshot || sedeCode || '-').trim() || '-',
      dependenciaCodigo,
      dependenciaNombre,
      dependenciaKey: dependenciaCodigo || 'NO_DEP:' + dependenciaNombre,
      planeados,
      contratados,
      noContratado,
      noRegistrado,
      novSinReemplazo,
      ausentismoTotal,
      totalPagar,
      scheduled,
      actualCount: actualRows.length
    };
  }

  function renderDependency(date) {
    const rows = sortRows(dependencyRows, depSortKey, depSortDir);
    const tbody = qs('#tblDependency tbody', ui);
    tbody.replaceChildren(...rows.map((row) => {
      const tr = el('tr', {}, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      btn.addEventListener('click', () => renderDetail(row.dependenciaKey, row.dependenciaNombre, date));
      tr.append(
        el('td', {}, [row.dependenciaNombre || '-']),
        el('td', {}, [String(row.planeados)]),
        el('td', {}, [String(row.contratados)]),
        el('td', {}, [String(row.noContratado)]),
        el('td', {}, [String(row.novSinReemplazo)]),
        el('td', {}, [String(row.ausentismoTotal)]),
        el('td', {}, [String(row.totalPagar)]),
        el('td', {}, [btn])
      );
      return tr;
    }));

    const totals = dependencyRows.reduce((acc, row) => ({
      planeados: acc.planeados + Number(row.planeados || 0),
      contratados: acc.contratados + Number(row.contratados || 0),
      noContratado: acc.noContratado + Number(row.noContratado || 0),
      novSinReemplazo: acc.novSinReemplazo + Number(row.novSinReemplazo || 0),
      ausentismoTotal: acc.ausentismoTotal + Number(row.ausentismoTotal || 0),
      totalPagar: acc.totalPagar + Number(row.totalPagar || 0)
    }), { planeados: 0, contratados: 0, noContratado: 0, novSinReemplazo: 0, ausentismoTotal: 0, totalPagar: 0 });

    qs('#totDependency', ui).textContent = 'Total dependencias - Planeados: ' + totals.planeados + ', Contratados: ' + totals.contratados + ', No contratado: ' + totals.noContratado + ', Novedad sin reemplazo: ' + totals.novSinReemplazo + ', Total ausentismo: ' + totals.ausentismoTotal + ', Total a pagar: ' + totals.totalPagar;
    updateSortIndicators('#tblDependency th[data-sort-dep]', 'data-sort-dep', depSortKey, depSortDir);
  }

  function renderTotals() {
    totalsRows = [...sedeDailyRows];
    const rows = sortRows(totalsRows, sedeSortKey, sedeSortDir);
    const tbody = qs('#tblTotals tbody', ui);
    tbody.replaceChildren(...rows.map((row) => {
      const tr = el('tr', {}, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      btn.addEventListener('click', () => renderSedeDetail(row.sedeCodigo, row.sedeNombre));
      tr.append(
        el('td', {}, [row.sedeNombre || '-']),
        el('td', {}, [String(row.planeados)]),
        el('td', {}, [String(row.contratados)]),
        el('td', {}, [String(row.noContratado)]),
        el('td', {}, [String(row.novSinReemplazo)]),
        el('td', {}, [String(row.ausentismoTotal)]),
        el('td', {}, [String(row.totalPagar)]),
        el('td', {}, [btn])
      );
      return tr;
    }));
    const totalRange = totalsRows.reduce((acc, row) => acc + Number(row.totalPagar || 0), 0);
    qs('#totRange', ui).textContent = 'Total rango a pagar: ' + totalRange;
    updateSortIndicators('#tblTotals th[data-sort-sede]', 'data-sort-sede', sedeSortKey, sedeSortDir);
  }

  function renderDetail(dependenciaKey, dependenciaNombre, date) {
    qs('#detailTitle', ui).textContent = 'Detalle dependencia: ' + (dependenciaNombre || '-') + ' (' + date + ')';
    const detailRows = [];
    const rows = sedeDailyRows
      .filter((row) => row.dependenciaKey === dependenciaKey)
      .sort((a, b) => String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || '')));

    rows.forEach((summary) => {
      buildDetailRowsForSede(summary).forEach((row) => detailRows.push(row));
    });

    detailRowsCache = detailRows;
    renderDetailRows();
  }

  function renderSedeDetail(sedeCodigo, sedeNombre) {
    const date = String(qs('#opDate', ui)?.value || '').trim();
    qs('#detailTitle', ui).textContent = 'Detalle sede: ' + (sedeNombre || '-') + ' (' + date + ')';
    const summary = sedeDailyRows.find((row) => row.sedeCodigo === sedeCodigo);
    detailRowsCache = summary ? buildDetailRowsForSede(summary) : [];
    renderDetailRows();
  }

  function buildDetailRowsForSede(summary) {
    const rows = [...(employeeRowsBySede.get(summary.sedeCodigo) || [])]
      .filter((row) => String(row?.tipoPersonal || '').trim() === 'empleado')
      .sort((a, b) => String(a?.nombre || '').localeCompare(String(b?.nombre || '')));

    const detailRows = rows.map((row) => ({
      fecha: summary.fecha,
      sede: summary.sedeNombre,
      documento: row?.documento || '-',
      nombre: row?.nombre || '-',
      estado: describeDetailStatus(row, summary.scheduled)
    }));

    for (let index = 0; index < Number(summary.noContratado || 0); index += 1) {
      detailRows.push({
        fecha: summary.fecha,
        sede: summary.sedeNombre,
        documento: '-',
        nombre: 'No contratado ' + (index + 1),
        estado: 'No contratado'
      });
    }

    return detailRows;
  }

  function renderDetailRows() {
    const rows = sortRows(detailRowsCache, detailSortKey, detailSortDir);
    const tbody = qs('#tblDetail tbody', ui);
    tbody.replaceChildren(...rows.map((row) => el('tr', {}, [
      el('td', {}, [row.fecha || '-']),
      el('td', {}, [row.sede || '-']),
      el('td', {}, [row.documento || '-']),
      el('td', {}, [row.nombre || '-']),
      el('td', {}, [row.estado || '-'])
    ])));
    updateSortIndicators('#tblDetail th[data-sort-detail]', 'data-sort-detail', detailSortKey, detailSortDir);
  }

  function describeDetailStatus(row, scheduled) {
    if (row?.servicioProgramado === true) {
      if (row?.cuentaPagoServicio === true) {
        const replacementName = String(row?.reemplazadoPorNombre || row?.reemplazadoPorDocumento || '').trim();
        return replacementName ? 'Reemplazado por ' + replacementName : 'Trabajo';
      }
      if (isNoRegistroAbsenteeism(row)) return 'Ausentismo (sin registro / novedad 8)';
      if (row?.estadoDia === 'incapacidad') return 'Incapacidad' + (row?.novedadNombre ? ': ' + row.novedadNombre : '');
      if (row?.novedadNombre) return 'Ausentismo - ' + row.novedadNombre;
      return 'Ausentismo';
    }

    const baseStatus = row?.asistio === true
      ? 'Trabajo'
      : row?.estadoDia === 'incapacidad'
        ? 'Incapacidad' + (row?.novedadNombre ? ': ' + row.novedadNombre : '')
        : row?.novedadNombre
          ? 'Ausentismo - ' + row.novedadNombre
          : 'Ausentismo';

    return scheduled ? 'Sobrante - ' + baseStatus : baseStatus;
  }

  function isNoRegistroAbsenteeism(row) {
    const novedadCodigo = String(row?.novedadCodigo || '').trim();
    if (novedadCodigo === '8') return true;
    return row?.servicioProgramado === true
      && row?.cuentaPagoServicio !== true
      && !row?.sourceAttendanceId
      && !row?.sourceIncapacityId
      && String(row?.decisionCobertura || '').trim() === 'ausentismo';
  }

  function isNoveltyWithoutReplacement(row) {
    if (row?.servicioProgramado !== true) return false;
    if (row?.cuentaPagoServicio === true) return false;
    if (isNoRegistroAbsenteeism(row)) return false;
    return String(row?.decisionCobertura || '').trim() === 'ausentismo' || row?.estadoDia === 'incapacidad' || row?.estadoDia === 'ausente_sin_reemplazo';
  }

  async function exportSummaryExcel() {
    if (!dependencyRows.length && !totalsRows.length) {
      msg.textContent = 'No hay datos para exportar.';
      return;
    }
    try {
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb = mod.utils.book_new();
      const depData = dependencyRows.map((row) => ({
        Dependencia: row.dependenciaNombre,
        Planeados: row.planeados,
        Contratados: row.contratados,
        NoContratado: row.noContratado,
        NovedadSinReemplazo: row.novSinReemplazo,
        TotalAusentismo: row.ausentismoTotal,
        TotalPagar: row.totalPagar
      }));
      const totalsData = totalsRows.map((row) => ({
        Sede: row.sedeNombre,
        Planeados: row.planeados,
        Contratados: row.contratados,
        NoContratado: row.noContratado,
        NovedadSinReemplazo: row.novSinReemplazo,
        TotalAusentismo: row.ausentismoTotal,
        TotalPagar: row.totalPagar
      }));
      mod.utils.book_append_sheet(wb, mod.utils.json_to_sheet(depData.length ? depData : [{ Info: 'Sin datos' }]), 'ResumenDependencia');
      mod.utils.book_append_sheet(wb, mod.utils.json_to_sheet(totalsData.length ? totalsData : [{ Info: 'Sin datos' }]), 'ResumenSede');
      const date = String(qs('#opDate', ui)?.value || 'fecha').trim();
      mod.writeFile(wb, 'ausentismo_resumen_' + date + '.xlsx');
      msg.textContent = 'Resumen exportado a Excel.';
    } catch (error) {
      msg.textContent = 'Error exportando resumen: ' + (error?.message || error);
    }
  }

  async function exportDetailExcel() {
    if (!detailRowsCache.length) {
      msg.textContent = 'Primero abre un detalle para exportar.';
      return;
    }
    try {
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb = mod.utils.book_new();
      const data = detailRowsCache.map((row) => ({
        Fecha: row.fecha,
        Sede: row.sede,
        Documento: row.documento,
        Nombre: row.nombre,
        Estado: row.estado
      }));
      mod.utils.book_append_sheet(wb, mod.utils.json_to_sheet(data), 'DetalleDependencia');
      const date = String(qs('#opDate', ui)?.value || 'fecha').trim();
      mod.writeFile(wb, 'ausentismo_detalle_' + date + '.xlsx');
      msg.textContent = 'Detalle exportado a Excel.';
    } catch (error) {
      msg.textContent = 'Error exportando detalle: ' + (error?.message || error);
    }
  }

  async function exportSedeExcel() {
    if (!totalsRows.length) {
      msg.textContent = 'No hay resumen por sede para exportar.';
      return;
    }
    try {
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb = mod.utils.book_new();
      const data = totalsRows.map((row) => ({
        Sede: row.sedeNombre,
        Planeados: row.planeados,
        Contratados: row.contratados,
        NoContratado: row.noContratado,
        NovedadSinReemplazo: row.novSinReemplazo,
        TotalAusentismo: row.ausentismoTotal,
        TotalPagar: row.totalPagar
      }));
      mod.utils.book_append_sheet(wb, mod.utils.json_to_sheet(data), 'ResumenSede');
      const date = String(qs('#opDate', ui)?.value || 'fecha').trim();
      mod.writeFile(wb, 'ausentismo_resumen_sedes_' + date + '.xlsx');
      msg.textContent = 'Resumen por sede exportado a Excel.';
    } catch (error) {
      msg.textContent = 'Error exportando sedes: ' + (error?.message || error);
    }
  }



  function clearUi(message) {
    sedeDailyRows = [];
    dependencyRows = [];
    totalsRows = [];
    detailRowsCache = [];
    employeeRowsBySede = new Map();
    qs('#tblDependency tbody', ui).replaceChildren();
    qs('#tblTotals tbody', ui).replaceChildren();
    qs('#tblDetail tbody', ui).replaceChildren();
    qs('#totDependency', ui).textContent = 'Total dependencias - Planeados: 0, Contratados: 0, No contratado: 0, Novedad sin reemplazo: 0, Total ausentismo: 0, Total a pagar: 0';
    qs('#totRange', ui).textContent = 'Total rango a pagar: 0';
    msg.textContent = message || 'Sin datos.';
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

  function updateSortIndicators(selector, attrName, activeKey, dir) {
    ui.querySelectorAll(selector).forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/s[??]$/, '');
      th.dataset.baseLabel = base;
      const key = String(th.getAttribute(attrName) || '').trim();
      th.textContent = key && key === activeKey ? base + ' ' + (dir === 1 ? '?' : '?') : base;
    });
  }

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  run();
  return ui;
};


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
  const formatter = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota', year: 'numeric', month: '2-digit', day: '2-digit' });
  return formatter.format(new Date());
}
