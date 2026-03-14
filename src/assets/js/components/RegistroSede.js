import { el, qs, enableSectionToggles } from '../utils/dom.js';

export const RegistroSede = (mount, deps = {}) => {
  const today = todayBogota();
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Registro Sede']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [el('label', { className: 'label' }, ['Fecha']), el('input', { id: 'opDate', className: 'input', type: 'date', value: today, disabled: true })]),
      el('button', { id: 'btnRun', className: 'btn btn--primary', type: 'button' }, ['Actualizar']),
      el('span', { id: 'msg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Resumen por dependencia']),
      el('div', { className: 'form-row mt-1' }, [
        el('div', {}, [
          el('label', { className: 'label' }, ['Buscar dependencia']),
          el('input', { id: 'depSearch', className: 'input', placeholder: 'Nombre de dependencia...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Filtro']),
          el('select', { id: 'depFilter', className: 'input' }, [
            el('option', { value: 'all' }, ['Todas']),
            el('option', { value: 'faltantes' }, ['Con faltantes']),
            el('option', { value: 'sobrantes' }, ['Con sobrantes'])
          ])
        ])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'tblDependency' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-dep': 'dependenciaNombre', style: 'cursor:pointer' }, ['Dependencia']),
            el('th', { 'data-sort-dep': 'nroSedes', style: 'cursor:pointer' }, ['Nro sedes']),
            el('th', { 'data-sort-dep': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
            el('th', { 'data-sort-dep': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
            el('th', { 'data-sort-dep': 'registrados', style: 'cursor:pointer' }, ['Registrados']),
            el('th', { 'data-sort-dep': 'faltantes', style: 'cursor:pointer' }, ['Faltantes']),
            el('th', { 'data-sort-dep': 'sobrantes', style: 'cursor:pointer' }, ['Sobrantes']),
            el('th', {}, ['Detalle'])
          ])]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'totDependency', className: 'text-muted' }, ['Total dependencias - Sedes: 0, Planeados: 0, Contratados: 0, Registrados: 0, Faltantes: 0, Sobrantes: 0'])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Resumen por sede']),
      el('div', { className: 'form-row mt-1' }, [
        el('div', {}, [
          el('label', { className: 'label' }, ['Buscar sede']),
          el('input', { id: 'sedeSearch', className: 'input', placeholder: 'Nombre o codigo de sede...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Filtro']),
          el('select', { id: 'sedeFilter', className: 'input' }, [
            el('option', { value: 'all' }, ['Todas']),
            el('option', { value: 'faltantes' }, ['Con faltantes']),
            el('option', { value: 'sobrantes' }, ['Con sobrantes'])
          ])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Zona']),
          el('select', { id: 'sedeZoneFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'tblTotals' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-sede': 'sedeNombre', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-sede': 'zonaNombre', style: 'cursor:pointer' }, ['Zona']),
            el('th', { 'data-sort-sede': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
            el('th', { 'data-sort-sede': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
            el('th', { 'data-sort-sede': 'registrados', style: 'cursor:pointer' }, ['Registrados']),
            el('th', { 'data-sort-sede': 'faltantes', style: 'cursor:pointer' }, ['Faltantes']),
            el('th', { 'data-sort-sede': 'sobrantes', style: 'cursor:pointer' }, ['Sobrantes']),
            el('th', {}, ['Detalle'])
          ])]),
          el('tbody', {})
        ])
      ]),
      el('p', { id: 'totRange', className: 'text-muted' }, ['Total sedes - Planeados: 0, Contratados: 0, Registrados: 0, Faltantes: 0, Sobrantes: 0'])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { id: 'detailTitle', className: 'section-title' }, ['Detalle']),
      el('div', { className: 'table-wrap' }, [
        el('table', { className: 'table', id: 'tblDetail' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-detail': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
            el('th', { 'data-sort-detail': 'sede', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-detail': 'zona', style: 'cursor:pointer' }, ['Zona']),
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
  let sedeDailyRows = [];
  let dependencyRows = [];
  let totalsRows = [];
  let attendanceByKey = new Map();
  let contractedEmployeesBySede = new Map();
  let replByEmpDate = new Map();
  let replacementSuperByDateDoc = new Set();
  let novedadRules = { byCode: new Map(), byName: new Map() };
  let employeeByDoc = new Map();
  let employeeById = new Map();
  let depSortKey = 'dependenciaNombre';
  let depSortDir = 1;
  let sedeSortKey = 'sedeNombre';
  let sedeSortDir = 1;
  let detailSortKey = 'fecha';
  let detailSortDir = -1;

  qs('#btnRun', ui).addEventListener('click', run);
  qs('#depSearch', ui).addEventListener('input', () => renderDependency(currentDate()));
  qs('#depFilter', ui).addEventListener('change', () => renderDependency(currentDate()));
  qs('#sedeSearch', ui).addEventListener('input', () => renderTotals(currentDate()));
  qs('#sedeFilter', ui).addEventListener('change', () => renderTotals(currentDate()));
  qs('#sedeZoneFilter', ui).addEventListener('change', () => renderTotals(currentDate()));
  ui.querySelectorAll('#tblDependency th[data-sort-dep]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-dep') || '').trim();
      if (!key) return;
      if (depSortKey === key) depSortDir *= -1;
      else { depSortKey = key; depSortDir = 1; }
      renderDependency(currentDate());
    });
  });
  ui.querySelectorAll('#tblTotals th[data-sort-sede]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-sede') || '').trim();
      if (!key) return;
      if (sedeSortKey === key) sedeSortDir *= -1;
      else { sedeSortKey = key; sedeSortDir = 1; }
      renderTotals(currentDate());
    });
  });
  ui.querySelectorAll('#tblDetail th[data-sort-detail]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-detail') || '').trim();
      if (!key) return;
      if (detailSortKey === key) detailSortDir *= -1;
      else { detailSortKey = key; detailSortDir = 1; }
      renderDetailRows();
    });
  });
  let detailRowsCache = [];

  function updateSortIndicators(selector, attrName, activeKey, dir) {
    ui.querySelectorAll(selector).forEach((th) => {
      const base = th.dataset.baseLabel || th.textContent.replace(/\s[\^v▲▼]$/, '');
      th.dataset.baseLabel = base;
      const key = String(th.getAttribute(attrName) || '').trim();
      th.textContent = key && key === activeKey ? `${base} ${dir === 1 ? '▲' : '▼'}` : base;
    });
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

  async function run() {
    const date = todayBogota();
    msg.textContent = 'Consultando...';
    try {
      const [sedeStatus, attendance, replacements, sedes, novedades, employees, supernumerarios, dayClosed] = await Promise.all([
        deps.listSedeStatusRange?.(date, date) || [],
        deps.listAttendanceRange?.(date, date) || [],
        deps.listImportReplacementsRange?.(date, date) || [],
        loadSedesSnapshot(),
        loadNovedadesSnapshot(),
        loadEmployeesSnapshot(),
        loadSupernumerariosSnapshot(),
        deps.isOperationDayClosed?.(date) || false
      ]);
      novedadRules = buildNovedadReplacementRules(novedades || []);

      const sedeMetaByCode = new Map();
      (sedes || []).forEach((s) => {
        sedeMetaByCode.set(String(s.codigo || ''), {
          dependenciaCodigo: String(s.dependenciaCodigo || '').trim(),
          dependenciaNombre: String(s.dependenciaNombre || '').trim(),
          zonaCodigo: String(s.zonaCodigo || '').trim(),
          zonaNombre: String(s.zonaNombre || '').trim()
        });
      });

      replByEmpDate = new Map();
      replacementSuperByDateDoc = new Set();
      (replacements || []).forEach((r) => {
        const empKey = `${r.fecha || ''}|${r.empleadoId || ''}`;
        replByEmpDate.set(empKey, r);
        if (r.decision === 'reemplazo') {
          const superDoc = String(r.supernumerarioDocumento || '').trim();
          if (superDoc) replacementSuperByDateDoc.add(`${r.fecha || ''}|${superDoc}`);
        }
      });

      const statusBySede = new Map((sedeStatus || []).map((s) => [String(s.sedeCodigo || ''), s]));
      const activeSedeCodes = new Set(
        (sedes || [])
          .filter((s) => String(s.estado || 'activo').trim().toLowerCase() !== 'inactivo')
          .map((s) => String(s.codigo || '').trim())
          .filter(Boolean)
      );
      const superDocs = new Set(
        (supernumerarios || [])
          .filter((s) => isEmployeeAssignedToActiveSede(s, activeSedeCodes))
          .map((s) => String(s.documento || '').trim())
          .filter(Boolean)
      );
      const contratadosBySede = new Map();
      const contractedMapBySede = new Map();
      employeeByDoc = new Map();
      employeeById = new Map();
      (employees || []).forEach((e) => {
        const doc = String(e.documento || '').trim();
        const id = String(e.id || '').trim();
        if (doc) employeeByDoc.set(doc, e);
        if (id) employeeById.set(id, e);
        if (!isEmployeeAssignedToActiveSede(e, activeSedeCodes)) return;
        if (doc && superDocs.has(doc)) return;
        const sedeCode = String(e.sedeCodigo || '').trim();
        if (!sedeCode) return;
        if (!contractedMapBySede.has(sedeCode)) contractedMapBySede.set(sedeCode, new Map());
        const uniqueKey = doc || `EMP:${String(e.id || e.codigo || e.nombre || '').trim()}`;
        if (contractedMapBySede.get(sedeCode).has(uniqueKey)) return;
        contractedMapBySede.get(sedeCode).set(uniqueKey, {
          documento: doc || '-',
          nombre: String(e.nombre || '-').trim() || '-'
        });
        contratadosBySede.set(sedeCode, Number(contratadosBySede.get(sedeCode) || 0) + 1);
      });
      contractedEmployeesBySede = new Map(Array.from(contractedMapBySede.entries()).map(([k, m]) => [k, Array.from(m.values())]));

      attendanceByKey = new Map();
      (attendance || []).forEach((a) => {
        const effectiveSedeCode = resolveAttendanceSedeCode(a, {
          dayClosed,
          activeSedeCodes,
          employeeByDoc,
          employeeById,
          superDocs
        });
        if (!effectiveSedeCode) return;
        const key = `${a.fecha || ''}|${effectiveSedeCode}`;
        if (!attendanceByKey.has(key)) attendanceByKey.set(key, []);
        attendanceByKey.get(key).push({ ...a, sedeCodigo: effectiveSedeCode });
      });

      sedeDailyRows = (sedes || [])
        .filter((s) => String(s.estado || 'activo').trim().toLowerCase() !== 'inactivo')
        .map((s) => {
          const sedeCode = String(s.codigo || '').trim();
          const key = `${date}|${sedeCode}`;
          const atts = dedupeAttendanceRows(attendanceByKey.get(key) || []);
          const status = statusBySede.get(sedeCode) || {};
          const planeadosRaw = s.numeroOperarios ?? status.operariosPlaneados ?? status.operariosEsperados ?? 0;
          const planeados = parseOperatorCount(planeadosRaw);
          const contratados = Number(contratadosBySede.get(sedeCode) || 0);
          const registrados = atts.length;
          const faltantes = Math.max(0, contratados - registrados);
          const sobrantes = Math.max(0, registrados - contratados);
          const meta = sedeMetaByCode.get(sedeCode) || {};
          const dependenciaCodigo = String(meta.dependenciaCodigo || '').trim();
          const dependenciaNombre = String(meta.dependenciaNombre || '').trim() || 'Sin dependencia';
          const zonaCodigo = String(meta.zonaCodigo || '').trim();
          const zonaNombre = String(meta.zonaNombre || '').trim() || 'Sin zona';
          const dependenciaKey = dependenciaCodigo || `NO_DEP:${dependenciaNombre}`;
          return {
            fecha: date,
            sedeCodigo: sedeCode,
            sedeNombre: String(s.nombre || sedeCode || '-'),
            zonaCodigo,
            zonaNombre,
            dependenciaCodigo,
            dependenciaNombre,
            dependenciaKey,
            planeados,
            contratados,
            registrados,
            faltantes,
            sobrantes
          };
        })
        .sort((a, b) => String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || '')));

      const depMap = new Map();
      sedeDailyRows.forEach((r) => {
        if (!depMap.has(r.dependenciaKey)) {
          depMap.set(r.dependenciaKey, {
            dependenciaKey: r.dependenciaKey,
            dependenciaCodigo: r.dependenciaCodigo,
            dependenciaNombre: r.dependenciaNombre || 'Sin dependencia',
            nroSedes: 0,
            planeados: 0,
            contratados: 0,
            registrados: 0,
            faltantes: 0,
            sobrantes: 0
          });
        }
        const t = depMap.get(r.dependenciaKey);
        t.nroSedes += 1;
        t.planeados += r.planeados;
        t.contratados += r.contratados;
        t.registrados += r.registrados;
        t.faltantes += r.faltantes;
        t.sobrantes += r.sobrantes;
      });
      dependencyRows = Array.from(depMap.values()).sort((a, b) => String(a.dependenciaNombre || '').localeCompare(String(b.dependenciaNombre || '')));
      refreshZoneFilterOptions();

      renderDependency(date);
      renderTotals(date);
      msg.textContent = 'Consulta OK';
    } catch (e) {
      msg.textContent = 'Error: ' + (e?.message || e);
    }
  }

  function renderDependency(date) {
    const term = normalizeText(qs('#depSearch', ui)?.value || '');
    const mode = String(qs('#depFilter', ui)?.value || 'all').trim();
    const filteredRows = dependencyRows.filter((r) => {
      const matchesTerm = !term || normalizeText(r.dependenciaNombre || '').includes(term);
      if (!matchesTerm) return false;
      if (mode === 'faltantes') return Number(r.faltantes || 0) > 0;
      if (mode === 'sobrantes') return Number(r.sobrantes || 0) > 0;
      return true;
    });
    const sortedRows = sortRows(filteredRows, depSortKey, depSortDir);
    const tb = qs('#tblDependency tbody', ui);
    tb.replaceChildren(...sortedRows.map((r) => {
      const tr = el('tr', {}, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      btn.addEventListener('click', () => renderDependencyDetail(r.dependenciaKey, r.dependenciaNombre, date));
      tr.append(
        el('td', {}, [r.dependenciaNombre || '-']),
        el('td', {}, [String(r.nroSedes || 0)]),
        el('td', {}, [String(r.planeados)]),
        el('td', {}, [String(r.contratados)]),
        el('td', {}, [String(r.registrados)]),
        el('td', {}, [String(r.faltantes)]),
        el('td', {}, [String(r.sobrantes)]),
        el('td', {}, [btn])
      );
      return tr;
    }));

    const totals = filteredRows.reduce((acc, r) => ({
      nroSedes: acc.nroSedes + Number(r.nroSedes || 0),
      planeados: acc.planeados + Number(r.planeados || 0),
      contratados: acc.contratados + Number(r.contratados || 0),
      registrados: acc.registrados + Number(r.registrados || 0),
      faltantes: acc.faltantes + Number(r.faltantes || 0),
      sobrantes: acc.sobrantes + Number(r.sobrantes || 0)
    }), { nroSedes: 0, planeados: 0, contratados: 0, registrados: 0, faltantes: 0, sobrantes: 0 });
    qs('#totDependency', ui).textContent = `Total dependencias - Sedes: ${totals.nroSedes}, Planeados: ${totals.planeados}, Contratados: ${totals.contratados}, Registrados: ${totals.registrados}, Faltantes: ${totals.faltantes}, Sobrantes: ${totals.sobrantes}`;
    updateSortIndicators('#tblDependency th[data-sort-dep]', 'data-sort-dep', depSortKey, depSortDir);
  }

  function renderTotals(date) {
    const bySede = new Map();
    sedeDailyRows.forEach((r) => {
      if (!bySede.has(r.sedeCodigo)) {
        bySede.set(r.sedeCodigo, {
          sedeCodigo: r.sedeCodigo,
          sedeNombre: r.sedeNombre || '-',
          zonaCodigo: r.zonaCodigo || '',
          zonaNombre: r.zonaNombre || 'Sin zona',
          planeados: 0,
          contratados: 0,
          registrados: 0,
          faltantes: 0,
          sobrantes: 0
        });
      }
      const t = bySede.get(r.sedeCodigo);
      t.planeados += r.planeados;
      t.contratados += r.contratados;
      t.registrados += r.registrados;
      t.faltantes += r.faltantes;
      t.sobrantes += r.sobrantes;
    });
    totalsRows = Array.from(bySede.values()).sort((a, b) => String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || '')));
    const term = normalizeText(qs('#sedeSearch', ui)?.value || '');
    const mode = String(qs('#sedeFilter', ui)?.value || 'all').trim();
    const zoneFilter = String(qs('#sedeZoneFilter', ui)?.value || '').trim();
    const rows = totalsRows.filter((r) => {
      const blob = `${r.sedeNombre || ''} ${r.sedeCodigo || ''}`;
      const matchesTerm = !term || normalizeText(blob).includes(term);
      if (!matchesTerm) return false;
      if (zoneFilter && String(r.zonaCodigo || '').trim() !== zoneFilter) return false;
      if (mode === 'faltantes') return Number(r.faltantes || 0) > 0;
      if (mode === 'sobrantes') return Number(r.sobrantes || 0) > 0;
      return true;
    });
    const sortedRows = sortRows(rows, sedeSortKey, sedeSortDir);
    const tb = qs('#tblTotals tbody', ui);
    tb.replaceChildren(...sortedRows.map((r) => {
      const tr = el('tr', {}, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      btn.addEventListener('click', () => renderSedeDetail(r.sedeCodigo, r.sedeNombre, date));
      tr.append(
        el('td', {}, [r.sedeNombre || '-']),
        el('td', {}, [r.zonaNombre || 'Sin zona']),
        el('td', {}, [String(r.planeados)]),
        el('td', {}, [String(r.contratados)]),
        el('td', {}, [String(r.registrados)]),
        el('td', {}, [String(r.faltantes)]),
        el('td', {}, [String(r.sobrantes)]),
        el('td', {}, [btn])
      );
      return tr;
    }));
    const totals = rows.reduce((acc, r) => ({
      planeados: acc.planeados + Number(r.planeados || 0),
      contratados: acc.contratados + Number(r.contratados || 0),
      registrados: acc.registrados + Number(r.registrados || 0),
      faltantes: acc.faltantes + Number(r.faltantes || 0),
      sobrantes: acc.sobrantes + Number(r.sobrantes || 0)
    }), { planeados: 0, contratados: 0, registrados: 0, faltantes: 0, sobrantes: 0 });
    qs('#totRange', ui).textContent = `Total sedes - Planeados: ${totals.planeados}, Contratados: ${totals.contratados}, Registrados: ${totals.registrados}, Faltantes: ${totals.faltantes}, Sobrantes: ${totals.sobrantes}`;
    updateSortIndicators('#tblTotals th[data-sort-sede]', 'data-sort-sede', sedeSortKey, sedeSortDir);
  }

  function renderDependencyDetail(dependenciaKey, dependenciaNombre, date) {
    qs('#detailTitle', ui).textContent = `Detalle dependencia: ${dependenciaNombre || '-'} (${date})`;
    const rows = buildDetailRows(
      sedeDailyRows.filter((r) => r.dependenciaKey === dependenciaKey)
    );
    detailRowsCache = rows;
    renderDetailRows();
  }

  function renderSedeDetail(sedeCodigo, sedeNombre, date) {
    qs('#detailTitle', ui).textContent = `Detalle sede: ${sedeNombre || '-'} (${date})`;
    const rows = buildDetailRows(
      sedeDailyRows.filter((r) => r.sedeCodigo === sedeCodigo)
    );
    detailRowsCache = rows;
    renderDetailRows();
  }

  function buildDetailRows(rows = []) {
    const detailRows = [];
    rows.forEach((d) => {
      const key = `${d.fecha}|${d.sedeCodigo}`;
      const atts = dedupeAttendanceRows(attendanceByKey.get(key) || []);
      const contracted = contractedEmployeesBySede.get(d.sedeCodigo) || [];
      const registeredDocs = new Set(
        atts
          .map((a) => String(a.documento || '').trim())
          .filter(Boolean)
      );
      atts.forEach((a) => {
        const rep = replByEmpDate.get(`${a.fecha || ''}|${a.empleadoId || ''}`);
        let estado = 'Trabajo';
        if (rep) {
          estado = rep.decision === 'reemplazo'
            ? `Reemplazado por ${rep.supernumerarioNombre || rep.supernumerarioDocumento || '-'}`
            : 'Ausentismo';
        } else if (a.asistio === false) {
          estado = attendanceRequiresReplacementForSummary(a, novedadRules)
            ? 'Ausentismo'
            : `Novedad: ${a.novedadNombre || a.novedad || '-'}`;
        }
        detailRows.push({
          fecha: d.fecha,
          sede: d.sedeNombre,
          zona: d.zonaNombre || 'Sin zona',
          documento: a.documento || '-',
          nombre: a.nombre || '-',
          estado
        });
      });

      contracted.forEach((c) => {
        if (registeredDocs.has(String(c.documento || '').trim())) return;
        detailRows.push({
          fecha: d.fecha,
          sede: d.sedeNombre,
          zona: d.zonaNombre || 'Sin zona',
          documento: c.documento || '-',
          nombre: c.nombre || '-',
          estado: 'Sin registro'
        });
      });

      const effectiveCoverage = Math.max(Number(d.contratados || 0), atts.length);
      const noContratados = Math.max(0, Number(d.planeados || 0) - effectiveCoverage);
      for (let i = 0; i < noContratados; i += 1) {
        detailRows.push({
          fecha: d.fecha,
          sede: d.sedeNombre,
          zona: d.zonaNombre || 'Sin zona',
          documento: '-',
          nombre: `No contratado ${i + 1}`,
          estado: 'Sin registro'
        });
      }
    });
    return detailRows;
  }

  function renderDetailRows() {
    const rows = sortRows(detailRowsCache, detailSortKey, detailSortDir);
    const tb = qs('#tblDetail tbody', ui);
    tb.replaceChildren(...rows.map((r) => el('tr', {}, [
      el('td', {}, [r.fecha || '-']),
      el('td', {}, [r.sede || '-']),
      el('td', {}, [r.zona || 'Sin zona']),
      el('td', {}, [r.documento || '-']),
      el('td', {}, [r.nombre || '-']),
      el('td', {}, [r.estado || '-'])
    ])));
    updateSortIndicators('#tblDetail th[data-sort-detail]', 'data-sort-detail', detailSortKey, detailSortDir);
  }

  async function loadSedesSnapshot() {
    if (typeof deps.streamSedes !== 'function') return [];
    return snapshotOnce(deps.streamSedes);
  }

  async function loadNovedadesSnapshot() {
    if (typeof deps.streamNovedades !== 'function') return [];
    return snapshotOnce(deps.streamNovedades);
  }

  async function loadEmployeesSnapshot() {
    if (typeof deps.streamEmployees !== 'function') return [];
    return snapshotOnce(deps.streamEmployees);
  }

  async function loadSupernumerariosSnapshot() {
    if (typeof deps.streamSupernumerarios !== 'function') return [];
    return snapshotOnce(deps.streamSupernumerarios);
  }

  function refreshZoneFilterOptions() {
    const sel = qs('#sedeZoneFilter', ui);
    if (!sel) return;
    const previous = String(sel.value || '').trim();
    const zones = Array.from(
      new Map(
        sedeDailyRows.map((r) => [String(r.zonaCodigo || '').trim(), String(r.zonaNombre || 'Sin zona').trim() || 'Sin zona'])
      ).entries()
    )
      .filter(([code]) => code)
      .sort((a, b) => String(a[1] || '').localeCompare(String(b[1] || '')));
    sel.replaceChildren(
      el('option', { value: '' }, ['Todas']),
      ...zones.map(([code, name]) => el('option', { value: code, selected: code === previous }, [`${name} (${code})`]))
    );
  }

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  run();
  return () => {};
};

function currentDate() {
  const elDate = document.getElementById('opDate');
  return String(elDate?.value || todayBogota()).trim();
}

function snapshotOnce(streamFn) {
  return new Promise((resolve) => {
    let settled = false;
    let unsub = null;
    const finish = (rows) => {
      if (settled) return;
      settled = true;
      try { if (typeof unsub === 'function') unsub(); } catch {}
      resolve(Array.isArray(rows) ? rows : []);
    };
    unsub = streamFn((rows) => finish(rows));
    setTimeout(() => finish([]), 5000);
  });
}

function buildNovedadReplacementRules(rows = []) {
  const byCode = new Map();
  const byName = new Map();
  (Array.isArray(rows) ? rows : []).forEach((r) => {
    const code = String(r.codigoNovedad || r.codigo || '').trim();
    const name = normalizeText(String(r.nombre || '').trim());
    const repl = normalizeText(String(r.reemplazo || '').trim());
    const needs = ['si', 'yes', 'true', '1', 'reemplazo'].includes(repl);
    if (code) byCode.set(code, needs);
    if (name) byName.set(name, needs);
  });
  return { byCode, byName };
}

function attendanceRequiresReplacementForSummary(att = {}, rules = {}) {
  const code = String(att.novedadCodigo || (/^\d+$/.test(String(att.novedad || '').trim()) ? String(att.novedad || '').trim() : '')).trim();
  if (['1', '7'].includes(code)) return false;
  if (['2', '3', '4', '5', '8'].includes(code)) return true;
  if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
  const name = normalizeText(baseNovedadNameForSummary(att.novedadNombre || att.novedad || ''));
  if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
  return false;
}

function baseNovedadNameForSummary(raw) {
  return String(raw || '').replace(/\s*\(.*\)\s*$/, '').trim();
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
  const n = Number(digits);
  return Number.isFinite(n) ? n : 0;
}

function dedupeAttendanceRows(rows = []) {
  const unique = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row, idx) => {
    const documento = String(row?.documento || '').trim();
    const empleadoId = String(row?.empleadoId || '').trim();
    const fallback = `${String(row?.nombre || '').trim()}|${String(row?.sedeCodigo || '').trim()}|${String(row?.fecha || '').trim()}|${idx}`;
    const key = documento || empleadoId || fallback;
    if (!key) return;
    if (!unique.has(key)) unique.set(key, row);
  });
  return Array.from(unique.values());
}

function resolveAttendanceSedeCode(attendanceRow = {}, context = {}) {
  const rawSedeCode = String(attendanceRow?.sedeCodigo || '').trim();
  if (context?.dayClosed) return rawSedeCode || null;

  const documento = String(attendanceRow?.documento || '').trim();
  if (documento && context?.superDocs?.has(documento)) return null;

  const empleadoId = String(attendanceRow?.empleadoId || '').trim();
  const employee = (empleadoId && context?.employeeById?.get(empleadoId))
    || (documento && context?.employeeByDoc?.get(documento))
    || null;
  if (!employee) return null;
  if (!isEmployeeAssignedToActiveSede(employee, context?.activeSedeCodes || new Set())) return null;
  return String(employee?.sedeCodigo || '').trim() || null;
}

function isEmployeeExpectedForDate(emp, selectedDate, sedeRows = []) {
  if (!selectedDate) return false;
  const ingreso = toISODate(emp?.fechaIngreso);
  if (!ingreso || ingreso > selectedDate) return false;
  const retiro = toISODate(emp?.fechaRetiro);
  const estado = String(emp?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') return Boolean(retiro && retiro >= selectedDate);
  if (retiro && retiro < selectedDate) return false;
  const sedeCodigo = String(emp?.sedeCodigo || '').trim();
  if (!sedeCodigo) return false;
  const sede = (sedeRows || []).find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
  if (!isSedeScheduledForDate(sede, selectedDate)) return false;
  return true;
}

function isEmployeeAssignedToActiveSede(emp, activeSedeCodes = new Set()) {
  const sedeCodigo = String(emp?.sedeCodigo || '').trim();
  if (!sedeCodigo) return false;
  if (activeSedeCodes.size && !activeSedeCodes.has(sedeCodigo)) return false;
  const ingreso = toISODate(emp?.fechaIngreso);
  if (!ingreso) return false;
  const retiro = toISODate(emp?.fechaRetiro);
  if (retiro) return false;
  const estado = String(emp?.estado || '').trim().toLowerCase();
  return estado !== 'inactivo';
}

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = toISODate(selectedDate);
  if (!iso) return false;
  const [year, month, day] = iso.split('-').map((n) => Number(n));
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
  if (jornada === 'lun_sab') return weekday >= 1 && weekday <= 6;
  return weekday >= 1 && weekday <= 5;
}

function toISODate(value) {
  if (!value) return null;
  if (typeof value === 'string') {
    const v = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
    const dt = new Date(v);
    if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
    return null;
  }
  if (typeof value?.toDate === 'function') {
    const dt = value.toDate();
    if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
    return null;
  }
  if (value instanceof Date) {
    if (!Number.isNaN(value.getTime())) return value.toISOString().slice(0, 10);
    return null;
  }
  return null;
}

function todayBogota() {
  const fmt = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota', year: 'numeric', month: '2-digit', day: '2-digit' });
  return fmt.format(new Date());
}
