import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';

export const WhatsAppLive = (mount, deps = {}) => {
  const today = todayBogota();
  const closureDay = addDaysToIsoDate(today, -1) || today;
  const ui = el('section', { className: 'main-card' }, [
    el('section', { className: 'wa-header' }, [
      el('div', { className: 'wa-header__left' }, [
        el('div', { className: 'wa-header__top' }, [
          el('h2', {}, ['Registro Diario']),
          el('div', { className: 'wa-date-pill' }, [
            el('span', { className: 'wa-date-pill__label' }, ['Fecha']),
            el('strong', { className: 'wa-date-pill__value' }, [today])
          ])
        ]),
        el('div', { className: 'wa-filters wa-filters--wide' }, [
          el('div', { className: 'wa-field wa-field--search' }, [
            el('label', { className: 'label' }, ['Buscar']),
            el('input', { id: 'waSearch', className: 'input wa-input', placeholder: 'Cedula, nombre, novedad o reemplazo...' })
          ]),
          el('div', { className: 'wa-field' }, [
            el('label', { className: 'label' }, ['Filtro']),
            el('select', { id: 'waNoveltyFilter', className: 'input wa-input' }, [
              el('option', { value: 'all' }, ['Todas'])
            ])
          ])
        ])
      ]),
      el('div', { className: 'wa-header__right' }, [
        el('div', { className: 'wa-stats wa-stats--summary wa-stats--summary-full' }, [
          el('article', { className: 'wa-stat card wa-stat--wide wa-stat--summary-compact' }, [
            el('small', { className: 'wa-stat__label wa-stat__label--title' }, ['Resumen Operativo']),
            el('div', { className: 'wa-summary-groups' }, [
              el('div', { className: 'wa-summary-group' }, [
                el('small', { className: 'text-muted wa-summary-group__title' }, ['Planeacion']),
                el('div', { className: 'wa-kpis wa-kpis--compact wa-kpis--three' }, [
                  kpiItem('Planeados', 'waPlanned', '0'),
                  kpiItem('Contratados', 'waExpected', '0'),
                  kpiItem('Registros', 'waRegistered', '0')
                ])
              ]),
              el('div', { className: 'wa-summary-group' }, [
                el('small', { className: 'text-muted wa-summary-group__title' }, ['Operacion']),
                el('div', { className: 'wa-kpis wa-kpis--compact wa-kpis--three' }, [
                  kpiItem('Asistencias', 'waUnique', '0'),
                  kpiItem('Pendientes', 'waPending', '0'),
                  kpiItem('Ausentismo', 'waMissing', '0')
                ])
              ])
            ])
          ])
        ])
      ])
    ]),
    el('section', { className: 'wa-stats wa-stats--nov mt-2' }, [
      statCard('Novedades', 'waNoveltyTotal', '0', 'statNoveltyTotal'),
      statCard('Gestionadas', 'waNoveltyHandled', '0', 'statNoveltyHandled'),
      statCard('Pendientes', 'waNoveltyPending', '0', 'statNoveltyPending')
    ]),
    el('div', { className: 'mt-2 table-wrap' }, [
        el('table', { className: 'table wa-live-table' }, [
          el('colgroup', {}, [
            el('col', { style: 'width:90px' }),
            el('col', { style: 'width:72px' }),
            el('col', { style: 'width:106px' }),
            el('col', { style: 'width:200px' }),
            el('col', { style: 'width:220px' }),
            el('col', { style: 'width:64px' }),
            el('col', { style: 'width:220px' }),
            el('col', { style: 'width:70px' })
          ]),
          el('thead', {}, [
            el('tr', {}, [
              el('th', { 'data-sort': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
              el('th', { 'data-sort': 'hora', style: 'cursor:pointer' }, ['Hora']),
              el('th', { 'data-sort': 'documento', style: 'cursor:pointer' }, ['Cedula']),
              el('th', { 'data-sort': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
              el('th', { 'data-sort': 'novedad', style: 'cursor:pointer' }, ['Novedad']),
              el('th', { 'data-sort': 'dias', style: 'cursor:pointer' }, ['Dias']),
              el('th', { 'data-sort': 'reemplazo', style: 'cursor:pointer' }, ['Reemplazo'])
              ,
              el('th', {}, ['Info'])
            ])
          ]),
        el('tbody', {})
      ])
    ]),
    el('div', { className: 'mt-2', style: 'display:flex;justify-content:space-between;gap:.5rem;align-items:center;flex-wrap:wrap;' }, [
      el('div', { id: 'waModeHint', className: 'text-muted', style: 'font-size:.86rem;' }, ['Vista completa del día']),
      el('button', { id: 'btnManualClose', className: 'btn btn--primary', type: 'button' }, ['Cerrar dia'])
    ]),
    el('p', { id: 'waMsg', className: 'text-muted mt-2' }, ['Conectando...'])
  ]);

  const tbody = qs('tbody', ui);
  const msg = qs('#waMsg', ui);
  const searchInput = qs('#waSearch', ui);
  const noveltyFilter = qs('#waNoveltyFilter', ui);
  const modeHint = qs('#waModeHint', ui);
  const btnManualClose = qs('#btnManualClose', ui);
  const statNoveltyTotal = qs('#statNoveltyTotal', ui);
  const statNoveltyHandled = qs('#statNoveltyHandled', ui);
  const statNoveltyPending = qs('#statNoveltyPending', ui);

  let attendance = [];
  let replacements = [];
  let statsAttendance = [];
  let statsReplacements = [];
  let supernumerarios = [];
  let novedades = [];
  let employees = [];
  let sedes = [];
  let incapacitados = [];

  let unAttendance = null;
  let unReplacements = null;
  let unSupernumerarios = null;
  let unNovedades = null;
  let unEmployees = null;
  let unSedes = null;
  let unIncapacitados = null;
  let unDailyMetrics = null;
  let unDashboardAttendance = null;
  let unDashboardReplacements = null;
  let refreshTimer = null;
  let sortKey = 'hora';
  let sortDir = -1;
  let usingDashboardDocs = false;
  let lastLegacyBackfillAt = 0;
  let dailyMetrics = null;
  let cardFilter = 'all';

  function replacementRowKey(r = {}) {
    const empId = String(r.empleadoId || r.employeeId || '').trim();
    return `${String(r.fecha || '').trim()}_${empId}`;
  }

  function replacementMap() {
    const map = new Map();
    (replacements || []).forEach((r) => {
      const key = replacementRowKey(r);
      map.set(key, r);
    });
    return map;
  }

  function classifyRow(row) {
    if (isSupernumerarioAttendance(row)) return 'super_replacement';
    const raw = String(row.novedadNombre || row.novedad || '').trim();
    const code = attendanceNovedadCode(row);
    if ((!raw && !code) || code === '1' || raw === '1') return row.asistio ? 'replace_no' : 'none';
    if (code === '7') return 'replace_no';
    if (['2', '3', '4', '5', '8'].includes(code)) return 'replace_yes';
    const rawNorm = normalize(raw);
    if (rawNorm.startsWith('otra sede')) return 'otra_sede';

    const base = baseNovedadName(raw || code);
    const nov = findNovedadCatalog(base, code);
    if (nov) return nov.reemplazo === true ? 'replace_yes' : 'replace_no';

    const fallback = inferNovedadByKeyword(base);
    if (fallback) return fallback;

    return 'none';
  }

  function canAssignReplacement(row) {
    return classifyRow(row) === 'replace_yes';
  }

  function isSupernumerarioAttendance(row) {
    if (row?.isSupernumerario === true) return true;
    const doc = String(row?.documento || '').trim();
    if (!doc) return false;
    return (supernumerarios || []).some((s) => {
      if (!isPersonActiveForDate(s, String(row?.fecha || '').trim() || today)) return false;
      return String(s?.documento || '').trim() === doc;
    });
  }

  function rowStyleByClass(kind) {
    return '';
  }

  function novedadTextStyleByClass(kind) {
    if (kind === 'super_replacement') return 'color:#1d4ed8;font-weight:600;';
    if (kind === 'replace_no') return 'color:#15803d;font-weight:600;';
    if (kind === 'otra_sede') return 'color:#c2410c;font-weight:600;';
    if (kind === 'replace_yes') return 'color:#b91c1c;font-weight:600;';
    return '';
  }

  function baseNovedadName(rawValue) {
    const raw = String(rawValue || '').trim();
    if (!raw) return '';
    const noParens = raw.replace(/\s*\(.*\)\s*$/, '').trim();
    if (/^OTRA\s+SEDE\s*:/i.test(noParens)) return 'OTRA SEDE';
    return noParens;
  }

  function normalize(text) {
    return String(text || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  function normalizeIsoDate(value) {
    const v = String(value || '').trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : null;
  }

  function inclusiveDaysBetween(startDate, endDate) {
    const start = normalizeIsoDate(startDate);
    const end = normalizeIsoDate(endDate);
    if (!start || !end || end < start) return null;
    const [sy, sm, sd] = start.split('-').map((n) => Number(n));
    const [ey, em, ed] = end.split('-').map((n) => Number(n));
    const sUtc = Date.UTC(sy, (sm || 1) - 1, sd || 1);
    const eUtc = Date.UTC(ey, (em || 1) - 1, ed || 1);
    return Math.floor((eUtc - sUtc) / 86400000) + 1;
  }

  function incapacidadDaysForRow(row) {
    const byRange = inclusiveDaysBetween(row?.incapacidadInicio, row?.incapacidadFin);
    if (byRange != null) return byRange;
    const doc = String(row?.documento || '').trim();
    const active = (incapacitados || []).find((item) => {
      if (String(item?.documento || '').trim() !== doc) return false;
      const start = normalizeIsoDate(item?.fechaInicio);
      const end = normalizeIsoDate(item?.fechaFin);
      return Boolean(start && end && String(row?.fecha || '').trim() >= start && String(row?.fecha || '').trim() <= end);
    });
    if (active) {
      const fromRegistry = inclusiveDaysBetween(active.fechaInicio, active.fechaFin);
      if (fromRegistry != null) return fromRegistry;
    }
    const byValue = Number(row?.incapacidadDias);
    return Number.isFinite(byValue) && byValue > 0 ? byValue : null;
  }

  function findNovedadCatalog(name) {
    return findNovedadCatalogByNameOrCode(name, '');
  }

  function attendanceNovedadCode(row) {
    const explicit = String(row.novedadCodigo || '').trim();
    if (explicit) return explicit;
    const raw = String(row.novedad || '').trim();
    return /^\d+$/.test(raw) ? raw : '';
  }

  function findNovedadCatalogByNameOrCode(name, code = '') {
    const target = normalize(name);
    const targetCode = String(code || '').trim();
    if (!target && !targetCode) return null;
    if (['1', '7'].includes(targetCode)) {
      return { nombre: targetCode === '1' ? 'TRABAJANDO' : 'COMPENSATORIO', reemplazo: false };
    }
    if (['2', '3', '4', '5', '8'].includes(targetCode)) {
      const labels = {
        '2': 'ACCIDENTE LABORAL',
        '3': 'ENFERMEDAD GENERAL',
        '4': 'CALAMIDAD',
        '5': 'LICENCIA NO REMUNERADA',
        '8': 'NOVEDAD'
      };
      return { nombre: labels[targetCode] || targetCode, reemplazo: true };
    }
    const rows = (novedades || []).map((n) => ({
      ...n,
      _nameNorm: normalize(n.nombre),
      _codeNorm: normalize(n.codigoNovedad || n.codigo || '')
    }));

    let row = rows.find((n) => (targetCode && n._codeNorm === normalize(targetCode)) || (target && (n._nameNorm === target || n._codeNorm === target)));
    if (!row) {
      row = rows.find((n) => target && (n._nameNorm.includes(target) || target.includes(n._nameNorm)));
    }
    if (!row) {
      const targetTokens = target.split(' ').filter(Boolean);
      row = rows.find((n) => {
        const nameTokens = n._nameNorm.split(' ').filter(Boolean);
        const overlap = targetTokens.filter((t) => nameTokens.includes(t)).length;
        return overlap >= Math.min(2, targetTokens.length);
      });
    }
    if (!row) return null;
    const replNorm = normalize(row.reemplazo);
    const reemplazo = ['si', 'yes', 'true', '1', 'reemplazo'].includes(replNorm);
    return {
      nombre: String(row.nombre || '').trim() || null,
      reemplazo
    };
  }

  function inferNovedadByKeyword(baseName) {
    const t = normalize(baseName);
    if (!t) return null;
    if (t.includes('incapacidad')) return 'replace_yes';
    if (t.includes('accidente laboral')) return 'replace_yes';
    if (t.includes('calamidad')) return 'replace_yes';
    if (t.includes('permiso no remunerado')) return 'replace_yes';
    if (t.includes('compensatorio')) return 'replace_no';
    return null;
  }

  function displayNovedad(row) {
    const baseRaw = String(row.novedadNombre || row.novedad || '').trim();
    const code = attendanceNovedadCode(row);
    const raw = baseRaw || code;
    if (!raw || raw === '1' || code === '1') return 'TRABAJANDO';
    const rawNorm = normalize(raw);
    if (rawNorm.startsWith('otra sede')) return raw;

    const base = baseNovedadName(raw);
    const nov = findNovedadCatalogByNameOrCode(base, code);
    if (!nov) return raw;

    const daysMatch = raw.match(/\(([^)]+)\)\s*$/);
    const daysLabel = daysMatch ? ` (${daysMatch[1]})` : '';
    return `${nov.nombre}${daysLabel}`;
  }

  function optionsForRow(row) {
    const active = (supernumerarios || []).filter((s) => String(s.estado || 'activo') !== 'inactivo');
    const sameSede = active.filter((s) => String(s.sedeCodigo || '').trim() === String(row.sedeCodigo || '').trim());
    const list = sameSede.length ? sameSede : active;
    const used = usedReplacementDocsForDate(row.fecha, row.empleadoId);
    return list
      .map((s) => ({
        id: s.id,
        documento: String(s.documento || '').trim() || '',
        nombre: String(s.nombre || '').trim() || '-'
      }))
      .filter((s) => {
        const doc = String(s.documento || '').trim();
        if (!doc) return false;
        return !used.has(doc);
      })
      .sort((a, b) => `${a.nombre}${a.documento}`.localeCompare(`${b.nombre}${b.documento}`));
  }

  function usedReplacementDocsForDate(fecha, currentEmployeeId = null) {
    const set = new Set();
    (replacements || []).forEach((r) => {
      if (String(r.fecha || '').trim() !== String(fecha || '').trim()) return;
      if (String(r.decision || '').trim() !== 'reemplazo') return;
      const rid = String(r.empleadoId || r.employeeId || '').trim();
      if (currentEmployeeId && rid === String(currentEmployeeId).trim()) return;
      const doc = String(r.supernumerarioDocumento || '').trim();
      if (doc) set.add(doc);
    });
    return set;
  }

  function mergeReplacements(baseRows = [], newRows = []) {
    const map = new Map();
    (baseRows || []).forEach((r) => {
      const k = replacementRowKey(r);
      map.set(k, r);
    });
    (newRows || []).forEach((r) => {
      const k = replacementRowKey(r);
      map.set(k, r);
    });
    return Array.from(map.values());
  }

  async function loadLegacySnapshotIfNeeded() {
    if (!deps.listAttendanceRange || !deps.listImportReplacementsRange) return;
    const expectedRows = Number(dailyMetrics?.attendanceCount || 0) || 0;
    const currentRows = (attendance || []).length;
    if (expectedRows <= 0 || currentRows >= expectedRows) return;
    const now = Date.now();
    if (now - Number(lastLegacyBackfillAt || 0) < 8000) return;
    lastLegacyBackfillAt = now;
    msg.textContent = 'Sincronizando respaldo para completar registros del día...';
    try {
      const [att, repl] = await Promise.all([
        deps.listAttendanceRange(today, today),
        deps.listImportReplacementsRange(today, today)
      ]);
      attendance = att || [];
      replacements = repl || [];
      statsAttendance = attendance;
      statsReplacements = replacements;
      render();
    } catch (err) {
      msg.textContent = `Error cargando respaldo del día: ${err?.message || err}`;
    }
  }

  async function refreshCurrentDaySnapshot({ silent = true } = {}) {
    if (!deps.listAttendanceRange && !deps.listImportReplacementsRange) return;
    if (!silent) msg.textContent = 'Actualizando registro diario...';
    try {
      const [att, repl] = await Promise.all([
        deps.listAttendanceRange?.(today, today) || [],
        deps.listImportReplacementsRange?.(today, today) || []
      ]);
      attendance = att || [];
      replacements = repl || [];
      statsAttendance = attendance;
      statsReplacements = replacements;
      render();
    } catch (err) {
      if (!silent) msg.textContent = `Error cargando registro diario: ${err?.message || err}`;
    }
  }

  function stopRefreshTimer() {
    if (!refreshTimer) return;
    clearInterval(refreshTimer);
    refreshTimer = null;
  }

  function startRefreshTimer() {
    stopRefreshTimer();
    if (!deps.listAttendanceRange && !deps.listImportReplacementsRange) return;
    refreshTimer = setInterval(() => {
      refreshCurrentDaySnapshot({ silent: true });
    }, 5000);
  }

  function applyFilters(rows) {
    const term = String(searchInput.value || '').trim().toLowerCase();
    const selectedType = String(noveltyFilter?.value || 'all').trim();
    const replMap = replacementMap();
    let out = rows.filter((r) => String(r.fecha || '').trim() === today);

    out = out.filter((r) => {
      if (selectedType === 'all') return true;
      const rowType = noveltyTypeKey(r);
      return rowType === selectedType;
    });

    out = out.filter((r) => {
      if (cardFilter === 'all') return true;
      const isNovelty = canAssignReplacement(r);
      const key = replacementRowKey(r);
      const repl = replMap.get(key) || null;
      const decision = String(repl?.decision || '').trim();
      const handled = decision === 'reemplazo' || decision === 'ausentismo';
      if (cardFilter === 'novelty_total') return isNovelty;
      if (cardFilter === 'novelty_handled') return isNovelty && handled;
      if (cardFilter === 'novelty_pending') return isNovelty && !handled;
      return true;
    });

    if (!term) return out;
    return out.filter((r) => {
      const repl = replMap.get(replacementRowKey(r)) || {};
      const blob = [
        r.documento || '',
        r.nombre || '',
        r.novedadCodigo || '',
        r.novedadNombre || '',
        incapacidadDaysForRow(r) || '',
        r.novedad || '',
        r.sedeNombre || '',
        r.sedeCodigo || '',
        repl.supernumerarioNombre || '',
        repl.supernumerarioDocumento || '',
        repl.decision || ''
      ].join(' ').toLowerCase();
      return blob.includes(term);
    });
  }

  function sedeNameByCode(code, fallback = '') {
    const sedeCodigo = String(code || '').trim();
    if (!sedeCodigo) return String(fallback || '').trim() || '-';
    const sede = (sedes || []).find((s) => String(s?.codigo || '').trim() === sedeCodigo) || null;
    const byCatalog = String(sede?.nombre || '').trim();
    if (byCatalog) return byCatalog;
    return String(fallback || '').trim() || sedeCodigo;
  }

  function employeeInfoSnapshot(row) {
    const empleadoId = String(row?.empleadoId || '').trim();
    const documento = String(row?.documento || '').trim();
    const emp = (employees || []).find((e) => {
      if (empleadoId && String(e?.id || '').trim() === empleadoId) return true;
      if (documento && String(e?.documento || '').trim() === documento) return true;
      return false;
    }) || null;
    const sedeCodigo = String(row?.sedeCodigo || emp?.sedeCodigo || '').trim();
    const sedeNombre = sedeNameByCode(sedeCodigo, row?.sedeNombre || emp?.sedeNombre || '');
    const sede = (sedes || []).find((s) => String(s?.codigo || '').trim() === sedeCodigo) || null;
    return {
      documento: documento || String(emp?.documento || '-').trim() || '-',
      nombre: String(row?.nombre || emp?.nombre || '-').trim() || '-',
      telefono: String(row?.telefono || emp?.telefono || '-').trim() || '-',
      sede: sedeNombre,
      dependencia: String(sede?.dependenciaNombre || '-').trim() || '-',
      zona: String(sede?.zonaNombre || '-').trim() || '-'
    };
  }

  function infoButtonForRow(row) {
    const btn = el('button', { className: 'btn', type: 'button', title: 'Ver informacion del empleado', 'aria-label': 'Ver informacion del empleado' }, ['ⓘ']);
    btn.addEventListener('click', () => {
      const info = employeeInfoSnapshot(row);
      showInfoModal('Informacion del empleado', [
        `Cedula: ${info.documento}`,
        `Nombre: ${info.nombre}`,
        `Telefono: ${info.telefono}`,
        `Sede: ${info.sede}`,
        `Dependencia: ${info.dependencia}`,
        `Zona: ${info.zona}`
      ]);
    });
    return btn;
  }

  function noveltyTypeLabel(row) {
    const raw = String(displayNovedad(row) || row.novedadNombre || row.novedad || '').trim();
    const code = String(row.novedadCodigo || '').trim();
    if (!raw || raw === '1' || code === '1') return 'TRABAJANDO';
    const base = baseNovedadName(raw).toUpperCase();
    return base || 'OTRA NOVEDAD';
  }

  function noveltyTypeKey(row) {
    return normalize(noveltyTypeLabel(row)).replace(/\s+/g, '_');
  }

  function refreshNoveltyFilterOptions() {
    if (!noveltyFilter) return;
    const dayRows = (statsAttendance || []).filter((r) => String(r.fecha || '').trim() === today);
    const labels = Array.from(new Set(dayRows.map((r) => noveltyTypeLabel(r)).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    const current = String(noveltyFilter.value || 'all').trim();
    const options = [
      el('option', { value: 'all' }, ['Todas']),
      ...labels.map((label) => el('option', { value: normalize(label).replace(/\s+/g, '_') }, [label]))
    ];
    noveltyFilter.replaceChildren(...options);
    if ([...noveltyFilter.options].some((o) => o.value === current)) noveltyFilter.value = current;
  }

  async function saveReplacement(row, selectedDoc, btn, selectEl = null) {
    const selectedValue = String(selectedDoc || '').trim();
    const wantsAusentismo = selectedValue === '__ausentismo__' || !selectedValue;
    const selected = wantsAusentismo
      ? null
      : (supernumerarios || []).find((s) => String(s.documento || '').trim() === selectedValue) || null;
    if (selected) {
      const used = usedReplacementDocsForDate(row.fecha, row.empleadoId);
      if (used.has(String(selected.documento || '').trim())) {
        msg.textContent = 'Ese supernumerario ya fue usado en otro registro del mismo dia.';
        return;
      }
    }

    const assignment = {
      fecha: row.fecha,
      empleadoId: row.empleadoId,
      employeeId: row.empleadoId,
      documento: row.documento || null,
      nombre: row.nombre || null,
      sedeCodigo: row.sedeCodigo || null,
      sedeNombre: sedeNameByCode(row.sedeCodigo, row.sedeNombre || null),
      novedadCodigo: row.novedadCodigo || null,
      novedadNombre: row.novedadNombre || row.novedad || null,
      decision: selected ? 'reemplazo' : 'ausentismo',
      supernumerarioId: selected?.id || null,
      supernumerarioDocumento: selected?.documento || null,
      supernumerarioNombre: selected?.nombre || null
    };

    btn.disabled = true;
    const oldText = btn.textContent;
    btn.textContent = 'Guardando...';
    msg.textContent = 'Guardando reemplazo o ausentismo...';

    try {
      await deps.saveImportReplacements?.({
        importId: null,
        fechaOperacion: row.fecha,
        assignments: [assignment]
      });
      replacements = mergeReplacements(replacements, [assignment]);
      statsReplacements = mergeReplacements(statsReplacements, [assignment]);
      render();
      msg.textContent = selected ? 'Reemplazo guardado correctamente.' : 'Ausentismo guardado correctamente.';
      if (selectEl) selectEl.disabled = true;
      btn.disabled = true;
      btn.textContent = 'Guardado';
    } catch (err) {
      msg.textContent = `Error guardando reemplazo: ${err?.message || err}`;
    } finally {
      if (!btn.disabled) {
        btn.disabled = false;
        btn.textContent = oldText;
      }
    }
  }

  function render() {
    const replMap = replacementMap();
    refreshNoveltyFilterOptions();
    const baseRows = applyFilters([...attendance]);
    const rows = [...baseRows].sort((a, b) => {
      const va = sortValueForRow(a, sortKey, replMap);
      const vb = sortValueForRow(b, sortKey, replMap);
      if (va === vb) return 0;
      return va > vb ? sortDir : -sortDir;
    });
    const stats = calculateStats();

    tbody.replaceChildren(
      ...rows.map((r) => {
        const key = replacementRowKey(r);
        const repl = replMap.get(key) || null;
        const rowClass = classifyRow(r);
        const canAssign = canAssignReplacement(r);
        const opts = canAssign ? optionsForRow(r) : [];
        const isSuperRow = rowClass === 'super_replacement';
        const baseNovedadText = String(r.novedadNombre || displayNovedad(r) || '-').trim() || '-';
        const novedadText = isSuperRow ? `${baseNovedadText} · SUPERNUMERARIO` : baseNovedadText;
        const novedadStyle = novedadTextStyleByClass(rowClass);
        const diasVal = incapacidadDaysForRow(r);
        const diasTxt = diasVal != null ? String(diasVal) : '-';
        const replacementText = displayReplacementText(r, repl, rowClass, opts);

        if (isSuperRow) {
          return el('tr', { style: rowStyleByClass(rowClass) }, [
            el('td', {}, [r.fecha || '-']),
            el('td', {}, [r.hora || '-']),
            el('td', {}, [r.documento || '-']),
            el('td', {}, [r.nombre || '-']),
            el('td', {}, [el('span', { style: novedadStyle }, [novedadText])]),
            el('td', {}, [diasTxt]),
            el('td', {}, [el('span', { style: 'color:#1d4ed8;' }, [replacementText])]),
            el('td', {}, [infoButtonForRow(r)])
          ]);
        }

        if (!canAssign) {
          return el('tr', { style: rowStyleByClass(rowClass) }, [
            el('td', {}, [r.fecha || '-']),
            el('td', {}, [r.hora || '-']),
            el('td', {}, [r.documento || '-']),
            el('td', {}, [r.nombre || '-']),
            el('td', {}, [el('span', { style: novedadStyle }, [novedadText])]),
            el('td', {}, [diasTxt]),
            el('td', {}, [el('span', { className: 'text-muted' }, [replacementText])]),
            el('td', {}, [infoButtonForRow(r)])
          ]);
        }

        if (repl && String(repl.decision || '').trim() === 'ausentismo') {
          return el('tr', { style: rowStyleByClass(rowClass) }, [
            el('td', {}, [r.fecha || '-']),
            el('td', {}, [r.hora || '-']),
            el('td', {}, [r.documento || '-']),
            el('td', {}, [r.nombre || '-']),
            el('td', {}, [el('span', { style: novedadStyle }, [novedadText])]),
            el('td', {}, [diasTxt]),
            el('td', {}, [el('span', { style: 'color:#b91c1c;' }, [replacementText])]),
            el('td', {}, [infoButtonForRow(r)])
          ]);
        }

        if (repl && String(repl.decision || '').trim() === 'reemplazo') {
          return el('tr', { style: rowStyleByClass(rowClass) }, [
            el('td', {}, [r.fecha || '-']),
            el('td', {}, [r.hora || '-']),
            el('td', {}, [r.documento || '-']),
            el('td', {}, [r.nombre || '-']),
            el('td', {}, [el('span', { style: novedadStyle }, [novedadText])]),
            el('td', {}, [diasTxt]),
            el('td', {}, [el('span', { style: 'color:#15803d;' }, [replacementText])]),
            el('td', {}, [infoButtonForRow(r)])
          ]);
        }

        const select = el(
          'select',
          {
            className: 'input wa-repl-select',
            style: 'width:170px;min-width:170px;max-width:170px;padding:.42rem .5rem;font-size:.78rem;',
            disabled: !canAssign
          },
          [
            el('option', { value: '__ausentismo__' }, ['Confirmar ausentismo']),
            ...opts.map((o) => el('option', { value: o.documento }, [`${o.nombre} (${o.documento || '-'})`]))
          ]
        );
        if (repl?.decision === 'ausentismo') select.value = '__ausentismo__';
        if (repl?.supernumerarioDocumento) select.value = String(repl.supernumerarioDocumento);
        const selectedLabel = () => select.options[select.selectedIndex]?.text || '';
        select.title = selectedLabel();
        const selectedPreview = el(
          'span',
          { className: 'text-muted', style: 'font-size:.78rem;line-height:1.25;white-space:normal;word-break:break-word;' },
          [selectedLabel()]
        );

        const saveBtn = el(
          'button',
          {
            className: 'btn',
            type: 'button',
            disabled: !canAssign,
            style: 'padding:3px 7px;font-size:.74rem;line-height:1.05;min-height:24px;'
          },
          ['Confirmar']
        );
        saveBtn.addEventListener('click', () => saveReplacement(r, select.value, saveBtn, select));
        select.addEventListener('change', () => {
          const label = selectedLabel();
          select.title = label;
          selectedPreview.textContent = label;
          if (!saveBtn.disabled) saveReplacement(r, select.value, saveBtn, select);
        });

        const replacementCell = el('div', { style: 'display:grid;gap:4px;min-width:0;' }, [
          el('div', { style: 'display:flex;align-items:center;gap:6px;flex-wrap:nowrap;min-width:0;' }, [select, saveBtn]),
          selectedPreview
        ]);

        return el('tr', { style: rowStyleByClass(rowClass) }, [
          el('td', {}, [r.fecha || '-']),
          el('td', {}, [r.hora || '-']),
          el('td', {}, [r.documento || '-']),
          el('td', {}, [r.nombre || '-']),
          el('td', {}, [el('span', { style: novedadStyle }, [novedadText])]),
          el('td', {}, [diasTxt]),
          el('td', {}, [replacementCell]),
          el('td', {}, [infoButtonForRow(r)])
        ]);
      })
    );

    qs('#waPlanned', ui).textContent = String(stats.planned);
    qs('#waExpected', ui).textContent = String(stats.expected);
    qs('#waRegistered', ui).textContent = String(stats.registered);
    qs('#waUnique', ui).textContent = String(stats.unique);
    qs('#waPending', ui).textContent = String(stats.pending);
    qs('#waMissing', ui).textContent = String(stats.missing);
    qs('#waNoveltyTotal', ui).textContent = String(stats.noveltyTotal);
    qs('#waNoveltyHandled', ui).textContent = String(stats.noveltyHandled);
    qs('#waNoveltyPending', ui).textContent = String(stats.noveltyPending);
    msg.textContent = usingDashboardDocs
      ? `Total registros del día: ${rows.length}`
      : `Total registros del día: ${rows.length}`;
    updateCardFilterUI();
    updateSortIndicators();
  }

  function setCardFilter(next) {
    cardFilter = cardFilter === next ? 'all' : next;
    render();
  }

  function paintCard(cardEl, active) {
    if (!cardEl) return;
    cardEl.style.cursor = 'pointer';
    cardEl.style.outline = active ? '2px solid #0ea5e9' : 'none';
    cardEl.style.outlineOffset = active ? '2px' : '0';
    cardEl.style.background = active ? '#eef8ff' : '';
  }

  function updateCardFilterUI() {
    paintCard(statNoveltyTotal, cardFilter === 'novelty_total');
    paintCard(statNoveltyHandled, cardFilter === 'novelty_handled');
    paintCard(statNoveltyPending, cardFilter === 'novelty_pending');
  }

  function sortValueForRow(row, key, replMap) {
    if (key === 'fecha') return String(row.fecha || '');
    if (key === 'hora') return String(row.hora || '');
    if (key === 'documento') return String(row.documento || '');
    if (key === 'nombre') return String(row.nombre || '').toLowerCase();
    if (key === 'novedad') return String(displayNovedad(row) || row.novedadNombre || row.novedad || '').toLowerCase();
    if (key === 'dias') return Number(incapacidadDaysForRow(row) || 0);
    if (key === 'reemplazo') {
      const repl = replMap.get(replacementRowKey(row)) || null;
      const rowClass = classifyRow(row);
      const opts = canAssignReplacement(row) ? optionsForRow(row) : [];
      return normalize(displayReplacementText(row, repl, rowClass, opts));
    }
    return '';
  }

  function displayReplacementText(row, repl = null, rowClass = classifyRow(row), options = []) {
    if (rowClass === 'super_replacement') {
      const sedeTxt = sedeNameByCode(row.sedeCodigo, row.sedeNombre || '');
      return `REEMPLAZO EN SEDE: ${sedeTxt}`;
    }
    if (!canAssignReplacement(row)) return 'No aplica';

    const decision = String(repl?.decision || '').trim().toLowerCase();
    if (decision === 'ausentismo') return 'Ausentismo confirmado';
    if (decision === 'reemplazo') {
      const repName = String(repl?.supernumerarioNombre || '').trim();
      const repDoc = String(repl?.supernumerarioDocumento || '').trim();
      return repName && repDoc ? `${repName} (${repDoc})` : repName || repDoc || 'Reemplazo confirmado';
    }

    const first = Array.isArray(options) ? options[0] : null;
    if (first) return `Pendiente: ${String(first.nombre || '').trim()} (${String(first.documento || '-').trim()})`;
    return 'Confirmar ausentismo';
  }

  function updateSortIndicators() {
    ui.querySelectorAll('th[data-sort]').forEach((th) => {
      const base = th.dataset.baseLabel || th.textContent.replace(/\s[\^v▲▼]$/, '');
      th.dataset.baseLabel = base;
      const key = th.getAttribute('data-sort');
      th.textContent = sortKey === key ? `${base} ${sortDir === 1 ? '▲' : '▼'}` : base;
    });
  }

  function calculateStats() {
    const dayRows = (statsAttendance || []).filter((r) => String(r.fecha || '').trim() === today);
    const activeSedes = (sedes || []).filter((s) => String(s?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
    const expectedLocal = (employees || []).filter((e) => {
      if (String(e?.estado || '').trim().toLowerCase() !== 'activo') return false;
      const sedeCodigo = String(e?.sedeCodigo || '').trim();
      const sede = activeSedes.find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
      if (!isSedeScheduledForDate(sede, today)) return false;
      return !isSupernumerarioEmployee(e, supernumerarios);
    }).length;
    const expectedBase = (employees || []).filter((e) => {
      if (String(e?.estado || '').trim().toLowerCase() !== 'activo') return false;
      const sedeCodigo = String(e?.sedeCodigo || '').trim();
      if (!sedeCodigo) return false;
      const sede = activeSedes.find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
      if (!sede) return false;
      return !isSupernumerarioEmployee(e, supernumerarios);
    }).length;
    const plannedLocal = activeSedes.reduce((acc, s) => {
      if (!isSedeScheduledForDate(s, today)) return acc;
      const n = parseOperatorCount(s?.numeroOperarios);
      return acc + (Number.isFinite(n) && n > 0 ? n : 0);
    }, 0);
    const plannedBase = activeSedes.reduce((acc, s) => {
      const n = parseOperatorCount(s?.numeroOperarios);
      return acc + (Number.isFinite(n) && n > 0 ? n : 0);
    }, 0);
    const replMap = new Map();
    (statsReplacements || []).forEach((r) => {
      const key = replacementRowKey(r);
      replMap.set(key, r);
    });
    const registeredLocal = dayRows.filter((row) => !isSupernumerarioAttendance(row)).length;
    const attendanceLocal = dayRows.filter((row) => {
      if (isSupernumerarioAttendance(row)) return false;
      const key = replacementRowKey({ fecha: row.fecha, empleadoId: row.empleadoId });
      const repl = replMap.get(key) || null;
      const kind = classifyRow(row);
      if (kind === 'replace_yes') {
        return String(repl?.decision || '').trim() === 'reemplazo' && Boolean(repl?.supernumerarioId || repl?.supernumerarioDocumento || repl?.supernumerarioNombre);
      }
      return true;
    }).length;
    const absenteeismLocal = dayRows.filter((row) => {
      if (isSupernumerarioAttendance(row)) return false;
      const key = replacementRowKey({ fecha: row.fecha, empleadoId: row.empleadoId });
      const repl = replMap.get(key) || null;
      const kind = classifyRow(row);
      if (kind !== 'replace_yes') return false;
      return String(repl?.decision || '').trim() !== 'reemplazo';
    }).length;
    const noveltyTotal = dayRows.filter((r) => canAssignReplacement(r)).length;
    const noveltyHandled = dayRows.filter((r) => {
      if (!canAssignReplacement(r)) return false;
      const key = replacementRowKey(r);
      const repl = replMap.get(key);
      if (!repl) return false;
      const decision = String(repl.decision || '').trim();
      return decision === 'reemplazo' || decision === 'ausentismo';
    }).length;
    const noveltyPending = Math.max(0, noveltyTotal - noveltyHandled);
    const plannedMetric = dailyMetrics ? Number(dailyMetrics.planned || 0) : null;
    const expectedMetric = dailyMetrics ? Number(dailyMetrics.expected || 0) : null;
    const expected = expectedMetric != null && expectedMetric > 0 ? expectedMetric : (expectedLocal > 0 ? expectedLocal : expectedBase);
    const registered = registeredLocal;
    const attendance = Math.min(registered, attendanceLocal);
    const absenteeism = Math.min(Math.max(0, registered - attendance), absenteeismLocal);
    const pending = Math.max(0, expected - registered);

    return {
      planned: plannedMetric != null && plannedMetric > 0 ? plannedMetric : (plannedLocal > 0 ? plannedLocal : plannedBase),
      expected,
      registered,
      unique: attendance,
      pending,
      missing: absenteeism,
      noveltyTotal,
      noveltyHandled,
      noveltyPending
    };
  }

  function bindDateStreams() {
    unAttendance?.();
    unReplacements?.();
    unDailyMetrics?.();
    unDashboardAttendance?.();
    unDashboardReplacements?.();
    stopRefreshTimer();
    unDashboardAttendance = null;
    unDashboardReplacements = null;
    attendance = [];
    replacements = [];
    statsAttendance = [];
    statsReplacements = [];
    dailyMetrics = null;
    lastLegacyBackfillAt = 0;
    usingDashboardDocs = false;
    if (modeHint) modeHint.textContent = 'Vista completa del día';
    render();
    if (deps.streamDailyMetricsByDate) {
      unDailyMetrics = deps.streamDailyMetricsByDate(
        today,
        (row) => {
          dailyMetrics = row || null;
          loadLegacySnapshotIfNeeded();
          render();
        },
        () => {}
      );
    }
    if (deps.streamDashboardAttendanceByDate && deps.streamDashboardReplacementsByDate) {
      usingDashboardDocs = true;
      unDashboardAttendance = deps.streamDashboardAttendanceByDate(
        today,
        (rows) => {
          attendance = rows || [];
          statsAttendance = attendance;
          loadLegacySnapshotIfNeeded();
          render();
        },
        () => {}
      );
      unDashboardReplacements = deps.streamDashboardReplacementsByDate(
        today,
        (rows) => {
          replacements = mergeReplacements(replacements, rows || []);
          statsReplacements = mergeReplacements(statsReplacements, rows || []);
          render();
        },
        () => {}
      );
      startRefreshTimer();
      refreshCurrentDaySnapshot({ silent: true });
      return;
    }
    if (modeHint) modeHint.textContent = 'Modo de respaldo';
    startRefreshTimer();
    refreshCurrentDaySnapshot({ silent: false })
      .catch((err) => {
        msg.textContent = `Error cargando registro diario: ${err?.message || err}`;
      });
  }

  searchInput.addEventListener('input', () => { render(); });
  noveltyFilter?.addEventListener('change', () => { render(); });
  statNoveltyTotal?.addEventListener('click', () => setCardFilter('novelty_total'));
  statNoveltyHandled?.addEventListener('click', () => setCardFilter('novelty_handled'));
  statNoveltyPending?.addEventListener('click', () => setCardFilter('novelty_pending'));
  [statNoveltyTotal, statNoveltyHandled, statNoveltyPending].forEach((card, idx) => {
    if (!card) return;
    const key = idx === 0 ? 'novelty_total' : idx === 1 ? 'novelty_handled' : 'novelty_pending';
    card.addEventListener('keydown', (ev) => {
      if (ev.key !== 'Enter' && ev.key !== ' ') return;
      ev.preventDefault();
      setCardFilter(key);
    });
  });
  ui.querySelectorAll('th[data-sort]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else {
        sortKey = key;
        sortDir = key === 'hora' ? -1 : 1;
      }
      render();
    });
  });
  btnManualClose?.addEventListener('click', async () => {
    if (typeof deps.closeOperationDayManual !== 'function') {
      msg.textContent = 'Cierre manual no disponible en este entorno.';
      return;
    }
    const alreadyClosed = await deps.isOperationDayClosed?.(closureDay);
    if (alreadyClosed) {
      msg.textContent = `La fecha ${closureDay} ya esta cerrada.`;
      return;
    }
    const ok = globalThis.confirm?.(
      `Se cerrara el dia ${closureDay}. Este cierre bloquea cambios posteriores para esa fecha. Deseas continuar?`
    );
    if (!ok) return;
    btnManualClose.disabled = true;
    const oldTxt = btnManualClose.textContent;
    btnManualClose.textContent = 'Cerrando...';
    msg.textContent = 'Ejecutando cierre diario manual...';
    try {
      const res = await deps.closeOperationDayManual(closureDay);
      const r = Array.isArray(res?.results) ? res.results[0] : null;
      const status = String(r?.status || 'ok').trim();
      if (status === 'closed' || status === 'already_closed') {
        msg.textContent = `Consulta OK. Cierre diario ${status === 'closed' ? 'realizado' : 'ya existente'} para ${closureDay}.`;
      } else {
        msg.textContent = `Cierre ejecutado con estado: ${status}.`;
      }
      render();
    } catch (err) {
      msg.textContent = `Error en cierre manual: ${err?.message || err}`;
    } finally {
      btnManualClose.disabled = false;
      btnManualClose.textContent = oldTxt;
    }
  });

  if (deps.streamSupernumerarios) {
    unSupernumerarios = deps.streamSupernumerarios((rows) => {
      supernumerarios = rows || [];
      render();
    });
  }
  if (deps.streamNovedades) {
    unNovedades = deps.streamNovedades((rows) => {
      novedades = rows || [];
      render();
    });
  }
  if (deps.streamEmployees) {
    unEmployees = deps.streamEmployees((rows) => {
      employees = rows || [];
      render();
    });
  }
  if (deps.streamSedes) {
    unSedes = deps.streamSedes((rows) => {
      sedes = rows || [];
      render();
    });
  }
  if (deps.streamIncapacitadosByDate) {
    unIncapacitados = deps.streamIncapacitadosByDate(today, (rows) => {
      incapacitados = rows || [];
      render();
    });
  }

  bindDateStreams();
  mount.replaceChildren(ui);

  return () => {
    unAttendance?.();
    unReplacements?.();
    unDailyMetrics?.();
    unDashboardAttendance?.();
    unDashboardReplacements?.();
    unSupernumerarios?.();
    unNovedades?.();
    unEmployees?.();
    unSedes?.();
    unIncapacitados?.();
    stopRefreshTimer();
  };
};

function statCard(label, id, value, cardId = null) {
  const attrs = { className: 'wa-stat card', role: 'button', tabindex: '0' };
  if (cardId) attrs.id = cardId;
  return el('article', attrs, [
    el('small', { className: 'wa-stat__label' }, [label]),
    el('strong', { id, className: 'wa-stat__value' }, [value])
  ]);
}

function kpiItem(label, id, value) {
  return el('div', { className: 'wa-kpi' }, [
    el('small', { className: 'wa-kpi__label' }, [label]),
    el('strong', { id, className: 'wa-kpi__value' }, [value])
  ]);
}

function isEmployeeExpectedForDate(emp, selectedDate, sedeRows = []) {
  if (!selectedDate) return false;
  const ingreso = toISODate(emp?.fechaIngreso);
  if (!ingreso || ingreso > selectedDate) return false;

  const retiro = toISODate(emp?.fechaRetiro);
  if (String(emp?.estado || '').toLowerCase() === 'inactivo') {
    return Boolean(retiro && retiro >= selectedDate);
  }
  if (retiro && retiro < selectedDate) return false;

  const sedeCodigo = String(emp?.sedeCodigo || '').trim();
  if (!sedeCodigo) return false;
  const sede = (sedeRows || []).find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
  if (!isSedeScheduledForDate(sede, selectedDate)) return false;

  return true;
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

function isEmployeeActiveTodayStrict(emp, selectedDate) {
  return isPersonActiveForDate(emp, selectedDate);
}

function isSupernumerarioEmployee(emp, supernumerariosRows = []) {
  const doc = String(emp?.documento || '').trim();
  if (doc) {
    return (supernumerariosRows || []).some((s) => String(s?.documento || '').trim() === doc);
  }
  const id = String(emp?.id || '').trim();
  if (!id) return false;
  return (supernumerariosRows || []).some((s) => String(s?.id || '').trim() === id);
}

function isPersonActiveForDate(person, selectedDate) {
  if (!selectedDate) return false;
  const estado = String(person?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') return false;
  const ingreso = toISODate(person?.fechaIngreso);
  if (!ingreso || ingreso > selectedDate) return false;
  const retiro = toISODate(person?.fechaRetiro);
  if (retiro && retiro < selectedDate) return false;
  return true;
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
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Bogota',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  return fmt.format(new Date());
}

function addDaysToIsoDate(value, days = 1) {
  const iso = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;
  const [year, month, day] = iso.split('-').map((n) => Number(n));
  const utc = new Date(Date.UTC(year, (month || 1) - 1, day || 1));
  utc.setUTCDate(utc.getUTCDate() + Number(days || 0));
  const y = utc.getUTCFullYear();
  const m = String(utc.getUTCMonth() + 1).padStart(2, '0');
  const d = String(utc.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
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
