import { el, qs } from '../utils/dom.js';
import { can, PERMS } from '../permissions.js';

export const HiringBySedeReport = (mount, deps = {}) => {
  const canExport = can(PERMS.EXPORT_REPORTS_HIRING);
  let generatedRows = [];
  let running = false;
  let sortKey = 'dependencia';
  let sortDir = 1;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Contratacion por Sedes']),
    el('p', { className: 'text-muted' }, ['Dependencia, zona, sede, empleados planeados, contratados y diferencia.']),
    el('div', { className: 'form-row mt-2' }, [
      el('button', { id: 'btnGenerateHiringReport', className: 'btn btn--primary', type: 'button' }, ['Generar reporte']),
      el('button', { id: 'btnExportHiringReport', className: 'btn', type: 'button', disabled: true, title: canExport ? '' : 'Modo consulta: no puedes exportar.' }, ['Exportar Excel']),
      el('span', { id: 'hiringReportMsg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label', for: 'hiringReportSearch' }, ['Buscar']),
        el('input', { id: 'hiringReportSearch', className: 'input', placeholder: 'Dependencia, zona o sede...' })
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'hiringReportDependencyFilter' }, ['Dependencia']),
        el('select', { id: 'hiringReportDependencyFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'hiringReportSedeFilter' }, ['Sede']),
        el('select', { id: 'hiringReportSedeFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
      ])
    ]),
    el('div', { className: 'table-wrap mt-2' }, [
      el('table', { className: 'table', id: 'hiringReportTable' }, [
        el('thead', {}, [el('tr', {}, [
          el('th', { 'data-sort-hiring-report': 'dependencia', style: 'cursor:pointer' }, ['Dependencia']),
          el('th', { 'data-sort-hiring-report': 'zona', style: 'cursor:pointer' }, ['Zona']),
          el('th', { 'data-sort-hiring-report': 'sede', style: 'cursor:pointer' }, ['Nombre Sede']),
          el('th', { 'data-sort-hiring-report': 'empleadosPlaneados', style: 'cursor:pointer' }, ['Empleados Planeados']),
          el('th', { 'data-sort-hiring-report': 'empleadosContratados', style: 'cursor:pointer' }, ['Empleados Contratados']),
          el('th', { 'data-sort-hiring-report': 'diferencia', style: 'cursor:pointer' }, ['Diferencia'])
        ])]),
        el('tbody', { id: 'hiringReportTbody' }, [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin generar.'])])])
      ])
    ]),
    el('p', { id: 'hiringReportTotal', className: 'text-muted mt-2' }, ['Genera el reporte para ver resultados.']),
    el('p', { id: 'hiringReportFilteredTotal', className: 'text-muted' }, ['Sedes filtradas: 0'])
  ]);

  qs('#btnGenerateHiringReport', ui)?.addEventListener('click', generateReport);
  qs('#btnExportHiringReport', ui)?.addEventListener('click', exportExcel);
  qs('#hiringReportSearch', ui)?.addEventListener('input', renderTable);
  qs('#hiringReportDependencyFilter', ui)?.addEventListener('change', renderTable);
  qs('#hiringReportSedeFilter', ui)?.addEventListener('change', renderTable);
  ui.querySelectorAll('#hiringReportTable th[data-sort-hiring-report]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-hiring-report') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else {
        sortKey = key;
        sortDir = 1;
      }
      renderTable();
    });
  });

  mount.replaceChildren(ui);
  updateSortIndicators();
  return ui;

  function setMessage(text) {
    qs('#hiringReportMsg', ui).textContent = text || ' ';
  }

  function normalizeText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  function streamOnce(factory, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let unsubscribe = () => {};
      const done = (cb) => (value) => {
        if (settled) return;
        settled = true;
        try { unsubscribe?.(); } catch (_) {}
        cb(value);
      };
      try {
        unsubscribe = factory(
          done(resolve),
          done((error) => reject(error instanceof Error ? error : new Error(String(error || 'Error de consulta.'))))
        ) || (() => {});
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error || 'Error de consulta.')));
        return;
      }
      setTimeout(() => {
        if (settled) return;
        settled = true;
        try { unsubscribe?.(); } catch (_) {}
        reject(new Error('Tiempo de espera agotado al consultar datos.'));
      }, timeoutMs);
    });
  }

  function normalizeRows(sedeRows = [], employeeRows = []) {
    const contractedBySede = new Map();
    (employeeRows || []).forEach((employee) => {
      const sedeCode = String(employee.sedeCodigo || '').trim();
      if (!sedeCode) return;
      contractedBySede.set(sedeCode, (contractedBySede.get(sedeCode) || 0) + 1);
    });

    return (sedeRows || [])
      .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
      .map((sede) => {
        const sedeCode = String(sede.codigo || '').trim();
        const planned = Number(sede.numeroOperarios ?? 0);
        const empleadosPlaneados = Number.isFinite(planned) && planned > 0 ? planned : 0;
        const empleadosContratados = Number(contractedBySede.get(sedeCode) || 0);
        return {
          dependencia: String(sede.dependenciaNombre || sede.dependenciaCodigo || '-').trim() || '-',
          zona: String(sede.zonaNombre || sede.zonaCodigo || '-').trim() || '-',
          sede: String(sede.nombre || sede.codigo || '-').trim() || '-',
          empleadosPlaneados,
          empleadosContratados,
          diferencia: empleadosPlaneados - empleadosContratados
        };
      })
      .sort((a, b) => {
        const byDependency = String(a.dependencia || '').localeCompare(String(b.dependencia || ''));
        if (byDependency !== 0) return byDependency;
        const byZone = String(a.zona || '').localeCompare(String(b.zona || ''));
        if (byZone !== 0) return byZone;
        return String(a.sede || '').localeCompare(String(b.sede || ''));
      });
  }

  function getFilteredRows() {
    const search = normalizeText(qs('#hiringReportSearch', ui)?.value || '');
    const dependency = String(qs('#hiringReportDependencyFilter', ui)?.value || '').trim();
    const sede = String(qs('#hiringReportSedeFilter', ui)?.value || '').trim();
    return (generatedRows || []).filter((row) => {
      if (dependency && row.dependencia !== dependency) return false;
      if (sede && row.sede !== sede) return false;
      if (!search) return true;
      return normalizeText(`${row.dependencia || ''} ${row.zona || ''} ${row.sede || ''}`).includes(search);
    });
  }

  function syncSelectOptions(selector, values = []) {
    const select = qs(selector, ui);
    if (!select) return;
    const previous = String(select.value || '').trim();
    const options = Array.from(new Set(values.map((value) => String(value || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    select.replaceChildren(
      el('option', { value: '' }, ['Todas']),
      ...options.map((value) => el('option', { value, selected: value === previous }, [value]))
    );
    select.value = options.includes(previous) ? previous : '';
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

  function renderRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin sedes para los filtros actuales.'])])];
    return rows.map((row) => el('tr', {}, [
      el('td', {}, [row.dependencia]),
      el('td', {}, [row.zona]),
      el('td', {}, [row.sede]),
      el('td', {}, [String(row.empleadosPlaneados)]),
      el('td', {}, [String(row.empleadosContratados)]),
      el('td', {}, [String(row.diferencia)])
    ]));
  }

  function renderTable() {
    const tbody = qs('#hiringReportTbody', ui);
    if (!tbody) return;
    const filteredRows = getFilteredRows();
    const rows = sortRows(filteredRows, sortKey, sortDir);
    tbody.replaceChildren(...renderRows(rows));
    const totalsNode = qs('#hiringReportFilteredTotal', ui);
    if (totalsNode) totalsNode.textContent = `Sedes filtradas: ${rows.length}`;
    updateSortIndicators();
  }

  function updateSortIndicators() {
    ui.querySelectorAll('#hiringReportTable th[data-sort-hiring-report]').forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v\u25B2\u25BC]$/, '');
      th.dataset.baseLabel = base;
      th.textContent = String(th.getAttribute('data-sort-hiring-report') || '').trim() === sortKey ? `${base} ${sortDir === 1 ? '\u25B2' : '\u25BC'}` : base;
    });
  }

  async function generateReport() {
    if (running) return;
    running = true;
    const btnGenerate = qs('#btnGenerateHiringReport', ui);
    const btnExport = qs('#btnExportHiringReport', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      if (typeof deps.streamSedes !== 'function' || typeof deps.streamActiveBaseEmployees !== 'function') {
        throw new Error('No estan disponibles las consultas necesarias para el reporte.');
      }
      const [rawSedes, rawEmployees] = await Promise.all([
        streamOnce((ok, fail) => deps.streamSedes(ok, fail)),
        streamOnce((ok, fail) => deps.streamActiveBaseEmployees(ok, fail))
      ]);
      generatedRows = normalizeRows(rawSedes, rawEmployees);
      const totals = generatedRows.reduce((acc, row) => {
        acc.planeados += Number(row.empleadosPlaneados || 0);
        acc.contratados += Number(row.empleadosContratados || 0);
        acc.diferencia += Number(row.diferencia || 0);
        return acc;
      }, { planeados: 0, contratados: 0, diferencia: 0 });
      const totalNode = qs('#hiringReportTotal', ui);
      if (totalNode) totalNode.textContent = `Sedes: ${generatedRows.length} | Planeados: ${totals.planeados} | Contratados: ${totals.contratados} | Diferencia: ${totals.diferencia}`;
      syncSelectOptions('#hiringReportDependencyFilter', generatedRows.map((row) => row.dependencia));
      syncSelectOptions('#hiringReportSedeFilter', generatedRows.map((row) => row.sede));
      renderTable();
      if (btnExport) btnExport.disabled = !canExport || generatedRows.length === 0;
      setMessage(' ');
    } catch (error) {
      setMessage(`Error al generar reporte de contratacion: ${error?.message || error}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function exportExcel() {
    const btn = qs('#btnExportHiringReport', ui);
    try {
      if (!generatedRows.length) throw new Error('Primero genera el reporte.');
      if (!canExport) throw new Error('No tienes permiso para exportar este reporte.');
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(generatedRows.map((row) => ({
        Dependencia: row.dependencia,
        Zona: row.zona,
        'Nombre Sede': row.sede,
        'Empleados Planeados': row.empleadosPlaneados,
        'Empleados Contratados': row.empleadosContratados,
        Diferencia: row.diferencia
      })));
      ws['!cols'] = [{ wch: 28 }, { wch: 24 }, { wch: 32 }, { wch: 20 }, { wch: 22 }, { wch: 14 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Contratacion por sedes');
      const date = new Date().toISOString().slice(0, 10);
      mod.writeFile(wb, `reporte_contratacion_por_sedes_${date}.xlsx`);
      setMessage(`Excel generado correctamente. Sedes: ${generatedRows.length}`);
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
