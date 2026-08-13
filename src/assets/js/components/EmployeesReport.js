import { el, qs } from '../utils/dom.js';
import { can, PERMS } from '../permissions.js';

export const EmployeesReport = (mount, deps = {}) => {
  const canExport = can(PERMS.EXPORT_REPORTS_EMPLOYEES);
  let generatedRows = [];
  let running = false;
  let sortKey = 'nombre';
  let sortDir = 1;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Reporte de Empleados']),
    el('p', { className: 'text-muted' }, ['Empleados vigentes con cedula, nombre, cargo, tipo, zona, dependencia y sede.']),
    el('div', { className: 'form-row mt-2' }, [
      el('button', { id: 'btnGenerateEmployeesReport', className: 'btn btn--primary', type: 'button' }, ['Generar reporte']),
      el('button', { id: 'btnExportEmployeesReport', className: 'btn', type: 'button', disabled: true, title: canExport ? '' : 'Modo consulta: no puedes exportar.' }, ['Exportar Excel']),
      el('span', { id: 'employeesReportMsg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label', for: 'employeesReportSearch' }, ['Buscar']),
        el('input', { id: 'employeesReportSearch', className: 'input', placeholder: 'Cedula, nombre, cargo, dependencia o sede...' })
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'employeesReportDependencyFilter' }, ['Dependencia']),
        el('select', { id: 'employeesReportDependencyFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'employeesReportSedeFilter' }, ['Sede']),
        el('select', { id: 'employeesReportSedeFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
      ])
    ]),
    el('div', { className: 'table-wrap mt-2' }, [
      el('table', { className: 'table', id: 'employeesReportTable' }, [
        el('thead', {}, [el('tr', {}, [
          el('th', { 'data-sort-employees-report': 'cedula', style: 'cursor:pointer' }, ['Cedula']),
          el('th', { 'data-sort-employees-report': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
          el('th', { 'data-sort-employees-report': 'cargo', style: 'cursor:pointer' }, ['Cargo']),
          el('th', { 'data-sort-employees-report': 'tipo', style: 'cursor:pointer' }, ['Tipo']),
          el('th', { 'data-sort-employees-report': 'zona', style: 'cursor:pointer' }, ['Zona']),
          el('th', { 'data-sort-employees-report': 'dependencia', style: 'cursor:pointer' }, ['Dependencia']),
          el('th', { 'data-sort-employees-report': 'sede', style: 'cursor:pointer' }, ['Sede'])
        ])]),
        el('tbody', { id: 'employeesReportTbody' }, [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin generar.'])])])
      ])
    ]),
    el('p', { id: 'employeesReportTotal', className: 'text-muted mt-2' }, ['Genera el reporte para ver resultados.']),
    el('p', { id: 'employeesReportFilteredTotal', className: 'text-muted' }, ['Registros filtrados: 0'])
  ]);

  qs('#btnGenerateEmployeesReport', ui)?.addEventListener('click', generateReport);
  qs('#btnExportEmployeesReport', ui)?.addEventListener('click', exportExcel);
  qs('#employeesReportSearch', ui)?.addEventListener('input', renderTable);
  qs('#employeesReportDependencyFilter', ui)?.addEventListener('change', renderTable);
  qs('#employeesReportSedeFilter', ui)?.addEventListener('change', renderTable);
  ui.querySelectorAll('#employeesReportTable th[data-sort-employees-report]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-employees-report') || '').trim();
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
    qs('#employeesReportMsg', ui).textContent = text || ' ';
  }

  function todayBogota() {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
  }

  function toISODate(value) {
    if (!value) return '';
    try {
      if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
      if (typeof value?.toDate === 'function') return value.toDate().toISOString().slice(0, 10);
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? '' : date.toISOString().slice(0, 10);
    } catch (_) {
      return '';
    }
  }

  function normalizeText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  function normalizeCargoAlignment(value) {
    const normalized = String(value || '').trim().toLowerCase();
    if (['supernumerario', 'supervisor', 'empleado'].includes(normalized)) return normalized;
    if (normalized.includes('supernumer')) return 'supernumerario';
    if (normalized.includes('supervisor')) return 'supervisor';
    return 'empleado';
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

  function isCurrentEmployee(employee, todayISO) {
    const estado = String(employee?.estado || 'activo').trim().toLowerCase();
    const retiro = toISODate(employee?.fechaRetiro);
    if (estado === 'inactivo') return Boolean(retiro && retiro >= todayISO);
    if (estado === 'eliminado') return false;
    return true;
  }

  function normalizeRows(rawEmployees = [], sedeRows = [], cargoRows = []) {
    const sedeByCode = new Map((sedeRows || []).map((sede) => [String(sede.codigo || '').trim(), sede || {}]).filter(([key]) => Boolean(key)));
    const cargoByCode = new Map((cargoRows || []).map((cargo) => [String(cargo.codigo || '').trim(), cargo || {}]).filter(([key]) => Boolean(key)));
    const todayISO = todayBogota();
    return (rawEmployees || [])
      .filter((employee) => isCurrentEmployee(employee, todayISO))
      .map((employee) => {
        const sedeCode = String(employee.sedeCodigo || '').trim();
        const sede = sedeByCode.get(sedeCode) || {};
        const cargoCode = String(employee.cargoCodigo || '').trim();
        const cargo = cargoByCode.get(cargoCode) || null;
        const alignment = normalizeCargoAlignment(cargo?.alineacionCrud || cargo?.alineacion_crud || employee.cargoNombre || '');
        return {
          cedula: String(employee.documento || '').trim() || '-',
          nombre: String(employee.nombre || '').trim() || '-',
          cargo: String(employee.cargoNombre || employee.cargoCodigo || '-').trim() || '-',
          tipo: alignment === 'supernumerario' ? 'Supernumerario' : alignment === 'supervisor' ? 'Supervisor' : 'Empleado',
          zona: String(sede.zonaNombre || sede.zonaCodigo || '-').trim() || '-',
          dependencia: String(sede.dependenciaNombre || sede.dependenciaCodigo || '-').trim() || '-',
          sede: String(sede.nombre || employee.sedeNombre || employee.sedeCodigo || '-').trim() || '-'
        };
      })
      .sort((a, b) => {
        const byName = String(a.nombre || '').localeCompare(String(b.nombre || ''));
        if (byName !== 0) return byName;
        return String(a.cedula || '').localeCompare(String(b.cedula || ''));
      });
  }

  function getFilteredRows() {
    const search = normalizeText(qs('#employeesReportSearch', ui)?.value || '');
    const dependency = String(qs('#employeesReportDependencyFilter', ui)?.value || '').trim();
    const sede = String(qs('#employeesReportSedeFilter', ui)?.value || '').trim();
    return (generatedRows || []).filter((row) => {
      if (dependency && row.dependencia !== dependency) return false;
      if (sede && row.sede !== sede) return false;
      if (!search) return true;
      return normalizeText(`${row.cedula || ''} ${row.nombre || ''} ${row.cargo || ''} ${row.tipo || ''} ${row.zona || ''} ${row.dependencia || ''} ${row.sede || ''}`).includes(search);
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
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin empleados para los filtros actuales.'])])];
    return rows.map((row) => el('tr', {}, [
      el('td', {}, [row.cedula]),
      el('td', {}, [row.nombre]),
      el('td', {}, [row.cargo]),
      el('td', {}, [row.tipo]),
      el('td', {}, [row.zona]),
      el('td', {}, [row.dependencia]),
      el('td', {}, [row.sede])
    ]));
  }

  function renderTable() {
    const tbody = qs('#employeesReportTbody', ui);
    if (!tbody) return;
    const filteredRows = getFilteredRows();
    const rows = sortRows(filteredRows, sortKey, sortDir);
    tbody.replaceChildren(...renderRows(rows));
    const totalsNode = qs('#employeesReportFilteredTotal', ui);
    if (totalsNode) totalsNode.textContent = `Registros filtrados: ${rows.length}`;
    updateSortIndicators();
  }

  function updateSortIndicators() {
    ui.querySelectorAll('#employeesReportTable th[data-sort-employees-report]').forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v\u25B2\u25BC]$/, '');
      th.dataset.baseLabel = base;
      th.textContent = String(th.getAttribute('data-sort-employees-report') || '').trim() === sortKey ? `${base} ${sortDir === 1 ? '\u25B2' : '\u25BC'}` : base;
    });
  }

  async function generateReport() {
    if (running) return;
    running = true;
    const btnGenerate = qs('#btnGenerateEmployeesReport', ui);
    const btnExport = qs('#btnExportEmployeesReport', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      if (typeof deps.streamEmployees !== 'function' || typeof deps.streamSedes !== 'function' || typeof deps.streamCargos !== 'function') {
        throw new Error('No estan disponibles las consultas necesarias para el reporte.');
      }
      const [rawEmployees, rawSedes, rawCargos] = await Promise.all([
        streamOnce((ok, fail) => deps.streamEmployees(ok, fail)),
        streamOnce((ok, fail) => deps.streamSedes(ok, fail)),
        streamOnce((ok, fail) => deps.streamCargos(ok, fail))
      ]);
      generatedRows = normalizeRows(rawEmployees, rawSedes, rawCargos);
      const totals = generatedRows.reduce((acc, row) => {
        if (row.tipo === 'Supernumerario') acc.supernumerarios += 1;
        else if (row.tipo === 'Supervisor') acc.supervisores += 1;
        else acc.empleados += 1;
        return acc;
      }, { empleados: 0, supernumerarios: 0, supervisores: 0 });
      const totalNode = qs('#employeesReportTotal', ui);
      if (totalNode) totalNode.textContent = `Total registros vigentes: ${generatedRows.length} | Empleados: ${totals.empleados} | Supernumerarios: ${totals.supernumerarios} | Supervisores: ${totals.supervisores}`;
      syncSelectOptions('#employeesReportDependencyFilter', generatedRows.map((row) => row.dependencia));
      syncSelectOptions('#employeesReportSedeFilter', generatedRows.map((row) => row.sede));
      renderTable();
      if (btnExport) btnExport.disabled = !canExport || generatedRows.length === 0;
      setMessage(' ');
    } catch (error) {
      setMessage(`Error al generar reporte: ${error?.message || error}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function exportExcel() {
    const btn = qs('#btnExportEmployeesReport', ui);
    try {
      if (!generatedRows.length) throw new Error('Primero genera el reporte.');
      if (!canExport) throw new Error('No tienes permiso para exportar este reporte.');
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(generatedRows.map((row) => ({
        Cedula: row.cedula,
        Nombre: row.nombre,
        Cargo: row.cargo,
        Tipo: row.tipo,
        Zona: row.zona,
        Dependencia: row.dependencia,
        Sede: row.sede
      })));
      ws['!cols'] = [{ wch: 18 }, { wch: 35 }, { wch: 28 }, { wch: 18 }, { wch: 24 }, { wch: 26 }, { wch: 30 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Empleados');
      const date = new Date().toISOString().slice(0, 10);
      mod.writeFile(wb, `reporte_empleados_vigentes_${date}.xlsx`);
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
