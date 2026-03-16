import { el, qs } from '../utils/dom.js';

export const Reports = (mount, deps = {}) => {
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Reportes']),
    el('p', { className: 'text-muted' }, ['Selecciona un reporte para consultarlo.']),
    el('div', { className: 'reports-grid mt-2', id: 'reportsCards' }, []),
    el('div', { className: 'divider' }, []),
    el('div', { id: 'reportContent' }, [el('p', { className: 'text-muted' }, ['Selecciona una tarjeta para abrir el reporte.'])]),
    el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
  ]);

  const reports = [
    { id: 'employees_current', title: 'Empleados', subtitle: 'Vigentes con cedula, nombre, cargo, zona, dependencia y sede' },
    { id: 'daily_registry', title: 'Registro diario', subtitle: 'Fecha, hora, cedula, nombre, sede, novedad y reemplazo/ausentismo' },
    { id: 'hiring_by_sede', title: 'Contratacion por Sedes', subtitle: 'Dependencia, zona, sede, planeados y contratados por sede' },
    { id: 'daily_absenteeism', title: 'Ausentismo diario', subtitle: 'Dependencia, zona, sede, planeados, contratados, ausentismo y total a pagar' }
  ];

  const cards = reports.map((r) =>
    el('button', { className: 'report-card', type: 'button', 'data-id': r.id }, [
      el('span', { className: 'report-card__title' }, [r.title]),
      el('span', { className: 'report-card__subtitle' }, [r.subtitle])
    ])
  );
  qs('#reportsCards', ui).replaceChildren(...cards);

  let selectedReportId = '';
  let generatedEmployeesRows = [];
  let generatedDailyRows = [];
  let generatedHiringRows = [];
  let generatedAbsenteeismRows = [];
  let running = false;
  let selectedDailyDate = new Date().toISOString().slice(0, 10);
  let selectedAbsenteeismDate = new Date().toISOString().slice(0, 10);

  function setMessage(text) {
    qs('#msg', ui).textContent = text || ' ';
  }

  function toISODate(value) {
    if (!value) return '';
    try {
      if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
      if (typeof value?.toDate === 'function') return value.toDate().toISOString().slice(0, 10);
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return '';
      return d.toISOString().slice(0, 10);
    } catch {
      return '';
    }
  }

  function formatHour(value) {
    try {
      const d = value?.toDate ? value.toDate() : value ? new Date(value) : null;
      if (!d || Number.isNaN(d.getTime())) return '-';
      return d.toLocaleTimeString('es-CO', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch {
      return '-';
    }
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
      let un = () => {};
      const done = (cb) => (value) => {
        if (settled) return;
        settled = true;
        try {
          un?.();
        } catch {}
        cb(value);
      };
      try {
        un =
          factory(
            done(resolve),
            done((err) => reject(err instanceof Error ? err : new Error(String(err || 'Error de consulta.'))))
          ) || (() => {});
      } catch (e) {
        reject(e instanceof Error ? e : new Error(String(e || 'Error de consulta.')));
        return;
      }
      setTimeout(() => {
        if (settled) return;
        settled = true;
        try {
          un?.();
        } catch {}
        reject(new Error('Tiempo de espera agotado al consultar datos.'));
      }, timeoutMs);
    });
  }

  function isCurrentEmployee(emp, todayISO) {
    const estado = String(emp?.estado || 'activo').trim().toLowerCase();
    const retiro = toISODate(emp?.fechaRetiro);
    if (estado === 'inactivo') return Boolean(retiro && retiro >= todayISO);
    if (estado === 'eliminado') return false;
    return true;
  }

  function normalizeEmployeesForReport(rawRows = [], sedeRows = []) {
    const sedeByCode = new Map((sedeRows || []).map((s) => [String(s.codigo || '').trim(), s || {}]).filter(([k]) => Boolean(k)));
    const todayISO = new Date().toISOString().slice(0, 10);
    return (rawRows || [])
      .filter((e) => isCurrentEmployee(e, todayISO))
      .map((e) => {
        const sedeCode = String(e.sedeCodigo || '').trim();
        const sede = sedeByCode.get(sedeCode) || {};
        return {
          cedula: String(e.documento || '').trim() || '-',
          nombre: String(e.nombre || '').trim() || '-',
          cargo: String(e.cargoNombre || e.cargoCodigo || '-').trim() || '-',
          zona: String(sede.zonaNombre || sede.zonaCodigo || '-').trim() || '-',
          dependencia: String(sede.dependenciaNombre || sede.dependenciaCodigo || '-').trim() || '-',
          sede: String(e.sedeNombre || sede.nombre || e.sedeCodigo || '-').trim() || '-'
        };
      })
      .sort((a, b) => {
        const byName = String(a.nombre || '').localeCompare(String(b.nombre || ''));
        if (byName !== 0) return byName;
        return String(a.cedula || '').localeCompare(String(b.cedula || ''));
      });
  }

  function normalizeDailyRegistryRows(fecha, attendanceRows = [], replacementsRows = []) {
    const replacementByEmployeeId = new Map();
    const replacementByDocumento = new Map();
    (replacementsRows || []).forEach((r) => {
      const empId = String(r.empleadoId || '').trim();
      const doc = String(r.documento || '').trim();
      if (empId) replacementByEmployeeId.set(empId, r);
      if (doc) replacementByDocumento.set(doc, r);
    });

    const out = (attendanceRows || []).map((a) => {
      const empId = String(a.empleadoId || '').trim();
      const doc = String(a.documento || '').trim();
      const rep = replacementByEmployeeId.get(empId) || replacementByDocumento.get(doc) || null;
      const novedad = String(rep?.novedadNombre || a.novedad || '-').trim() || '-';
      let decision = '-';
      const rawDecision = String(rep?.decision || '').trim().toLowerCase();
      if (rawDecision === 'reemplazo') {
        const who = String(rep?.supernumerarioNombre || rep?.supernumerarioDocumento || '').trim();
        decision = who ? `Reemplazo (${who})` : 'Reemplazo';
      } else if (rawDecision === 'ausentismo') {
        decision = 'Ausentismo';
      }

      return {
        fecha,
        hora: formatHour(a.createdAt),
        cedula: doc || '-',
        nombre: String(a.nombre || '-').trim() || '-',
        sede: String(a.sedeNombre || a.sedeCodigo || '-').trim() || '-',
        novedad,
        reemplazoAusentismo: decision,
        _ts: a.createdAt?.toMillis ? Number(a.createdAt.toMillis()) || 0 : 0
      };
    });

    out.sort((x, y) => {
      if (x._ts !== y._ts) return x._ts - y._ts;
      const byName = String(x.nombre || '').localeCompare(String(y.nombre || ''));
      if (byName !== 0) return byName;
      return String(x.cedula || '').localeCompare(String(y.cedula || ''));
    });
    return out.map(({ _ts, ...row }) => row);
  }

  function normalizeHiringRows(sedeRows = [], employeeRows = [], cargoRows = []) {
    const cargoByCode = new Map((cargoRows || []).map((c) => [String(c.codigo || '').trim(), c]).filter(([k]) => Boolean(k)));
    const todayISO = new Date().toISOString().slice(0, 10);
    const contractedBySede = new Map();

    (employeeRows || []).forEach((emp) => {
      if (!isCurrentEmployee(emp, todayISO)) return;
      const cargoCode = String(emp.cargoCodigo || '').trim();
      const cargo = cargoByCode.get(cargoCode) || null;
      const alignment = normalizeCargoAlignment(cargo?.alineacionCrud || cargo?.alineacion_crud || emp.cargoNombre || '');
      if (alignment === 'supernumerario') return;
      const sedeCode = String(emp.sedeCodigo || '').trim();
      if (!sedeCode) return;
      contractedBySede.set(sedeCode, (contractedBySede.get(sedeCode) || 0) + 1);
    });

    return (sedeRows || [])
      .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
      .map((sede) => {
        const sedeCode = String(sede.codigo || '').trim();
        const planned = Number(sede.numeroOperarios ?? 0);
        return {
          dependencia: String(sede.dependenciaNombre || sede.dependenciaCodigo || '-').trim() || '-',
          zona: String(sede.zonaNombre || sede.zonaCodigo || '-').trim() || '-',
          sede: String(sede.nombre || sede.codigo || '-').trim() || '-',
          empleadosPlaneados: Number.isFinite(planned) && planned > 0 ? planned : 0,
          empleadosContratados: Number(contractedBySede.get(sedeCode) || 0)
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

  function normalizeText(value) {
    return String(value || '')
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

  function buildNovedadReplacementRules(rows = []) {
    const byCode = new Map();
    const byName = new Map();
    (rows || []).forEach((row) => {
      const code = String(row.codigoNovedad || row.codigo || '').trim();
      const name = normalizeText(String(row.nombre || '').trim());
      const replacement = normalizeText(String(row.reemplazo || '').trim());
      const requiresReplacement = ['si', 'yes', 'true', '1', 'reemplazo'].includes(replacement);
      if (code) byCode.set(code, requiresReplacement);
      if (name) byName.set(name, requiresReplacement);
    });
    return { byCode, byName };
  }

  function baseNovedadName(raw) {
    return String(raw || '').replace(/\s*\(.*\)\s*$/, '').trim();
  }

  function attendanceRequiresReplacement(att = {}, rules = {}) {
    const code = String(att.novedadCodigo || (/^\d+$/.test(String(att.novedad || '').trim()) ? String(att.novedad || '').trim() : '')).trim();
    if (['1', '7'].includes(code)) return false;
    if (['2', '3', '4', '5', '8'].includes(code)) return true;
    if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
    const name = normalizeText(baseNovedadName(att.novedadNombre || att.novedad || ''));
    if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
    return false;
  }

  function normalizeAbsenteeismRows(fecha, sedeStatusRows = [], attendanceRows = [], replacementsRows = [], sedeRows = [], novedadRows = []) {
    const statusBySede = new Map((sedeStatusRows || []).map((row) => [String(row.sedeCodigo || '').trim(), row]).filter(([key]) => Boolean(key)));
    const sedeByCode = new Map((sedeRows || []).map((row) => [String(row.codigo || '').trim(), row]).filter(([key]) => Boolean(key)));
    const rules = buildNovedadReplacementRules(novedadRows);
    const replacementByEmployee = new Map();
    const replacementSuperByDateDoc = new Set();

    (replacementsRows || []).forEach((row) => {
      const employeeKey = `${row.fecha || ''}|${row.empleadoId || ''}`;
      if (String(row.empleadoId || '').trim()) replacementByEmployee.set(employeeKey, row);
      if (String(row.decision || '').trim().toLowerCase() === 'reemplazo') {
        const superDoc = String(row.supernumerarioDocumento || '').trim();
        if (superDoc) replacementSuperByDateDoc.add(`${row.fecha || ''}|${superDoc}`);
      }
    });

    const attendanceBySede = new Map();
    (attendanceRows || []).forEach((row) => {
      const attDoc = String(row.documento || '').trim();
      if (attDoc && replacementSuperByDateDoc.has(`${row.fecha || ''}|${attDoc}`)) return;
      const sedeCode = String(row.sedeCodigo || '').trim();
      if (!sedeCode) return;
      if (!attendanceBySede.has(sedeCode)) attendanceBySede.set(sedeCode, []);
      attendanceBySede.get(sedeCode).push(row);
    });

    const historicalSedeCodes = new Set([
      ...(sedeStatusRows || []).map((row) => String(row.sedeCodigo || '').trim()).filter(Boolean),
      ...(attendanceRows || []).map((row) => String(row.sedeCodigo || '').trim()).filter(Boolean)
    ]);

    return Array.from(historicalSedeCodes)
      .map((sedeCode) => {
        const status = statusBySede.get(sedeCode) || {};
        const sede = sedeByCode.get(sedeCode) || {};
        const attendance = attendanceBySede.get(sedeCode) || [];
        const planeados = parseOperatorCount(sede.numeroOperarios ?? status.operariosPlaneados ?? status.operariosEsperados ?? 0);
        const contratadosSnapshot = parseOperatorCount(status.operariosEsperados ?? 0);
        const contratados = contratadosSnapshot > 0 ? contratadosSnapshot : planeados;
        const noContratado = Math.max(0, planeados - contratados);
        const noRegistrado = parseOperatorCount(status.faltantes ?? 0);
        const novSinReemplazo = attendance.filter((att) => {
          if (att.asistio === true) return false;
          const replacement = replacementByEmployee.get(`${att.fecha || ''}|${att.empleadoId || ''}`);
          if (replacement && String(replacement.decision || '').trim().toLowerCase() === 'reemplazo') return false;
          return attendanceRequiresReplacement(att, rules);
        }).length;
        const ausentismoTotal = noRegistrado + novSinReemplazo;
        const totalPagar = Math.max(0, planeados - noContratado - ausentismoTotal);
        return {
          fecha,
          dependencia: String(sede.dependenciaNombre || sede.dependenciaCodigo || '-').trim() || '-',
          zona: String(sede.zonaNombre || sede.zonaCodigo || '-').trim() || '-',
          sede: String(status.sedeNombre || sede.nombre || sedeCode || '-').trim() || '-',
          planeados,
          contratados,
          noContratado,
          novedadSinReemplazo: novSinReemplazo,
          totalAusentismo: ausentismoTotal,
          totalPagar
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

  function renderEmployeesRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin empleados vigentes para mostrar.'])])];
    return rows.map((r) => el('tr', {}, [el('td', {}, [r.cedula]), el('td', {}, [r.nombre]), el('td', {}, [r.cargo]), el('td', {}, [r.zona]), el('td', {}, [r.dependencia]), el('td', {}, [r.sede])]));
  }

  function renderDailyRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin registros para la fecha seleccionada.'])])];
    return rows.map((r) => el('tr', {}, [el('td', {}, [r.fecha]), el('td', {}, [r.hora]), el('td', {}, [r.cedula]), el('td', {}, [r.nombre]), el('td', {}, [r.sede]), el('td', {}, [r.novedad]), el('td', {}, [r.reemplazoAusentismo])]));
  }

  function renderHiringRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 5, className: 'text-muted' }, ['Sin sedes activas para mostrar.'])])];
    return rows.map((r) =>
      el('tr', {}, [
        el('td', {}, [r.dependencia]),
        el('td', {}, [r.zona]),
        el('td', {}, [r.sede]),
        el('td', {}, [String(r.empleadosPlaneados)]),
        el('td', {}, [String(r.empleadosContratados)])
      ])
    );
  }

  function renderAbsenteeismRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 8, className: 'text-muted' }, ['Sin datos para la fecha seleccionada.'])])];
    return rows.map((r) =>
      el('tr', {}, [
        el('td', {}, [r.dependencia]),
        el('td', {}, [r.zona]),
        el('td', {}, [r.sede]),
        el('td', {}, [String(r.planeados)]),
        el('td', {}, [String(r.contratados)]),
        el('td', {}, [String(r.noContratado)]),
        el('td', {}, [String(r.totalAusentismo)]),
        el('td', {}, [String(r.totalPagar)])
      ])
    );
  }

  async function generateEmployeesReport() {
    if (running) return;
    running = true;
    const btnGenerate = qs('#btnGenerateEmployees', ui);
    const btnExport = qs('#btnExportEmployees', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const [rawEmployees, rawSedes] = await Promise.all([streamOnce((ok, fail) => deps.streamEmployees?.(ok, fail)), streamOnce((ok, fail) => deps.streamSedes?.(ok, fail))]);
      generatedEmployeesRows = normalizeEmployeesForReport(rawEmployees, rawSedes);
      const totalNode = qs('#employeesTotal', ui);
      if (totalNode) totalNode.textContent = `Total empleados vigentes: ${generatedEmployeesRows.length}`;
      const tbody = qs('#employeesTbody', ui);
      if (tbody) tbody.replaceChildren(...renderEmployeesRows(generatedEmployeesRows));
      if (btnExport) btnExport.disabled = generatedEmployeesRows.length === 0;
      setMessage(`Reporte generado. Registros: ${generatedEmployeesRows.length}`);
    } catch (e) {
      setMessage(`Error al generar reporte: ${e?.message || e}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function generateDailyReport() {
    if (running) return;
    const input = qs('#dailyDate', ui);
    const date = String(input?.value || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      setMessage('Selecciona una fecha valida para generar el reporte.');
      return;
    }
    running = true;
    selectedDailyDate = date;
    const btnGenerate = qs('#btnGenerateDaily', ui);
    const btnExport = qs('#btnExportDaily', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const [attendanceRows, replacementsRows] = await Promise.all([
        streamOnce((ok, fail) => deps.streamAttendanceByDate?.(date, ok, fail)),
        streamOnce((ok, fail) => deps.streamImportReplacementsByDate?.(date, ok, fail))
      ]);
      generatedDailyRows = normalizeDailyRegistryRows(date, attendanceRows, replacementsRows);
      const totalNode = qs('#dailyTotal', ui);
      if (totalNode) totalNode.textContent = `Total registros del dia: ${generatedDailyRows.length}`;
      const tbody = qs('#dailyTbody', ui);
      if (tbody) tbody.replaceChildren(...renderDailyRows(generatedDailyRows));
      if (btnExport) btnExport.disabled = generatedDailyRows.length === 0;
      setMessage(`Reporte generado para ${date}. Registros: ${generatedDailyRows.length}`);
    } catch (e) {
      setMessage(`Error al generar reporte diario: ${e?.message || e}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function generateHiringReport() {
    if (running) return;
    running = true;
    const btnGenerate = qs('#btnGenerateHiring', ui);
    const btnExport = qs('#btnExportHiring', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const [rawSedes, rawEmployees, rawCargos] = await Promise.all([
        streamOnce((ok, fail) => deps.streamSedes?.(ok, fail)),
        streamOnce((ok, fail) => deps.streamEmployees?.(ok, fail)),
        streamOnce((ok, fail) => deps.streamCargos?.(ok, fail))
      ]);
      generatedHiringRows = normalizeHiringRows(rawSedes, rawEmployees, rawCargos);
      const totals = generatedHiringRows.reduce((acc, row) => {
        acc.planeados += Number(row.empleadosPlaneados || 0);
        acc.contratados += Number(row.empleadosContratados || 0);
        return acc;
      }, { planeados: 0, contratados: 0 });
      const totalNode = qs('#hiringTotal', ui);
      if (totalNode) totalNode.textContent = `Sedes: ${generatedHiringRows.length} | Planeados: ${totals.planeados} | Contratados: ${totals.contratados}`;
      const tbody = qs('#hiringTbody', ui);
      if (tbody) tbody.replaceChildren(...renderHiringRows(generatedHiringRows));
      if (btnExport) btnExport.disabled = generatedHiringRows.length === 0;
      setMessage(`Reporte generado. Sedes: ${generatedHiringRows.length}`);
    } catch (e) {
      setMessage(`Error al generar reporte de contratacion: ${e?.message || e}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function generateAbsenteeismReport() {
    const date = String(qs('#absenteeismDate', ui)?.value || '').trim();
    if (!date) {
      setMessage('Selecciona una fecha valida para generar el reporte.');
      return;
    }
    if (running) return;
    running = true;
    selectedAbsenteeismDate = date;
    const btnGenerate = qs('#btnGenerateAbsenteeism', ui);
    const btnExport = qs('#btnExportAbsenteeism', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const dayClosed = await deps.isOperationDayClosed?.(date);
      if (!dayClosed) throw new Error('La fecha seleccionada no esta cerrada.');
      const [sedeStatusRows, attendanceRows, replacementsRows, sedeRows, novedadRows] = await Promise.all([
        deps.listSedeStatusRange?.(date, date) || [],
        deps.listAttendanceRange?.(date, date) || [],
        deps.listImportReplacementsRange?.(date, date) || [],
        streamOnce((ok, fail) => deps.streamSedes?.(ok, fail)),
        streamOnce((ok, fail) => deps.streamNovedades?.(ok, fail))
      ]);
      generatedAbsenteeismRows = normalizeAbsenteeismRows(date, sedeStatusRows, attendanceRows, replacementsRows, sedeRows, novedadRows);
      const totals = generatedAbsenteeismRows.reduce(
        (acc, row) => {
          acc.planeados += Number(row.planeados || 0);
          acc.contratados += Number(row.contratados || 0);
          acc.noContratado += Number(row.noContratado || 0);
          acc.ausentismo += Number(row.totalAusentismo || 0);
          acc.totalPagar += Number(row.totalPagar || 0);
          return acc;
        },
        { planeados: 0, contratados: 0, noContratado: 0, ausentismo: 0, totalPagar: 0 }
      );
      const totalNode = qs('#absenteeismTotal', ui);
      if (totalNode) {
        totalNode.textContent = `Sedes: ${generatedAbsenteeismRows.length} | Planeados: ${totals.planeados} | Contratados: ${totals.contratados} | No contratado: ${totals.noContratado} | Ausentismo: ${totals.ausentismo} | Total a pagar: ${totals.totalPagar}`;
      }
      const tbody = qs('#absenteeismTbody', ui);
      if (tbody) tbody.replaceChildren(...renderAbsenteeismRows(generatedAbsenteeismRows));
      if (btnExport) btnExport.disabled = generatedAbsenteeismRows.length === 0;
      setMessage(`Reporte generado para ${date}. Sedes: ${generatedAbsenteeismRows.length}`);
    } catch (e) {
      setMessage(`Error al generar reporte de ausentismo: ${e?.message || e}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function exportEmployeesExcel() {
    try {
      if (!generatedEmployeesRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportEmployees', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(generatedEmployeesRows.map((r) => ({ Cedula: r.cedula, Nombre: r.nombre, Cargo: r.cargo, Zona: r.zona, Dependencia: r.dependencia, Sede: r.sede })));
      ws['!cols'] = [{ wch: 18 }, { wch: 35 }, { wch: 28 }, { wch: 24 }, { wch: 26 }, { wch: 30 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Empleados');
      const date = new Date().toISOString().slice(0, 10);
      mod.writeFile(wb, `reporte_empleados_vigentes_${date}.xlsx`);
      setMessage(`Excel generado correctamente. Registros: ${generatedEmployeesRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportEmployees', ui);
      if (btn) {
        btn.disabled = generatedEmployeesRows.length === 0;
        btn.textContent = 'Generar Excel';
      }
    }
  }

  async function exportDailyExcel() {
    try {
      if (!generatedDailyRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportDaily', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(
        generatedDailyRows.map((r) => ({
          Fecha: r.fecha,
          Hora: r.hora,
          Cedula: r.cedula,
          Nombre: r.nombre,
          Sede: r.sede,
          Novedad: r.novedad,
          'Reemplazo/Ausentismo': r.reemplazoAusentismo
        }))
      );
      ws['!cols'] = [{ wch: 12 }, { wch: 12 }, { wch: 18 }, { wch: 30 }, { wch: 26 }, { wch: 26 }, { wch: 30 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Registro diario');
      mod.writeFile(wb, `reporte_registro_diario_${selectedDailyDate}.xlsx`);
      setMessage(`Excel generado correctamente para ${selectedDailyDate}. Registros: ${generatedDailyRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportDaily', ui);
      if (btn) {
        btn.disabled = generatedDailyRows.length === 0;
        btn.textContent = 'Generar Excel';
      }
    }
  }

  async function exportHiringExcel() {
    try {
      if (!generatedHiringRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportHiring', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(
        generatedHiringRows.map((r) => ({
          Dependencia: r.dependencia,
          Zona: r.zona,
          'Nombre Sede': r.sede,
          'Empleados Planeados': r.empleadosPlaneados,
          'Empleados Contratados': r.empleadosContratados
        }))
      );
      ws['!cols'] = [{ wch: 28 }, { wch: 24 }, { wch: 32 }, { wch: 20 }, { wch: 22 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Contratacion por sedes');
      const date = new Date().toISOString().slice(0, 10);
      mod.writeFile(wb, `reporte_contratacion_por_sedes_${date}.xlsx`);
      setMessage(`Excel generado correctamente. Sedes: ${generatedHiringRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportHiring', ui);
      if (btn) {
        btn.disabled = generatedHiringRows.length === 0;
        btn.textContent = 'Generar Excel';
      }
    }
  }

  async function exportAbsenteeismExcel() {
    try {
      if (!generatedAbsenteeismRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportAbsenteeism', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(
        generatedAbsenteeismRows.map((r) => ({
          Fecha: r.fecha,
          Dependencia: r.dependencia,
          Zona: r.zona,
          'Nombre Sede': r.sede,
          Planeados: r.planeados,
          Contratados: r.contratados,
          'No contratado': r.noContratado,
          'Novedad sin reemplazo': r.novedadSinReemplazo,
          'Total ausentismo': r.totalAusentismo,
          'Total a pagar': r.totalPagar
        }))
      );
      ws['!cols'] = [{ wch: 12 }, { wch: 28 }, { wch: 24 }, { wch: 32 }, { wch: 12 }, { wch: 12 }, { wch: 14 }, { wch: 22 }, { wch: 18 }, { wch: 16 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Ausentismo diario');
      mod.writeFile(wb, `reporte_ausentismo_diario_${selectedAbsenteeismDate}.xlsx`);
      setMessage(`Excel generado correctamente para ${selectedAbsenteeismDate}. Sedes: ${generatedAbsenteeismRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportAbsenteeism', ui);
      if (btn) {
        btn.disabled = generatedAbsenteeismRows.length === 0;
        btn.textContent = 'Generar Excel';
      }
    }
  }

  function renderEmployeesPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Empleados vigentes'])]),
        el('button', { id: 'btnGenerateEmployees', className: 'btn right', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportEmployees', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'employeesTotal', className: 'text-muted mt-2' }, ['Genera el reporte para ver resultados.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Cedula']), el('th', {}, ['Nombre']), el('th', {}, ['Cargo']), el('th', {}, ['Zona']), el('th', {}, ['Dependencia']), el('th', {}, ['Sede'])])]),
          el('tbody', { id: 'employeesTbody' }, [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateEmployees', ui)?.addEventListener('click', generateEmployeesReport);
    qs('#btnExportEmployees', ui)?.addEventListener('click', exportEmployeesExcel);
  }

  function renderDailyPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Registro diario'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Fecha']), el('input', { id: 'dailyDate', className: 'input', type: 'date', value: selectedDailyDate, style: 'max-width:180px' })]),
        el('button', { id: 'btnGenerateDaily', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportDaily', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'dailyTotal', className: 'text-muted mt-2' }, ['Selecciona la fecha y genera el reporte.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Fecha']), el('th', {}, ['Hora']), el('th', {}, ['Cedula']), el('th', {}, ['Nombre']), el('th', {}, ['Sede']), el('th', {}, ['Novedad']), el('th', {}, ['Reemplazo/Ausentismo'])])]),
          el('tbody', { id: 'dailyTbody' }, [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateDaily', ui)?.addEventListener('click', generateDailyReport);
    qs('#btnExportDaily', ui)?.addEventListener('click', exportDailyExcel);
  }

  function renderHiringPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Contratacion por Sedes'])]),
        el('button', { id: 'btnGenerateHiring', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportHiring', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'hiringTotal', className: 'text-muted mt-2' }, ['Genera el reporte para ver resultados.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Dependencia']), el('th', {}, ['Zona']), el('th', {}, ['Nombre Sede']), el('th', {}, ['Empleados Planeados']), el('th', {}, ['Empleados Contratados'])])]),
          el('tbody', { id: 'hiringTbody' }, [el('tr', {}, [el('td', { colSpan: 5, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateHiring', ui)?.addEventListener('click', generateHiringReport);
    qs('#btnExportHiring', ui)?.addEventListener('click', exportHiringExcel);
  }

  function renderAbsenteeismPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Ausentismo diario'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Fecha']), el('input', { id: 'absenteeismDate', className: 'input', type: 'date', value: selectedAbsenteeismDate, style: 'max-width:180px' })]),
        el('button', { id: 'btnGenerateAbsenteeism', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportAbsenteeism', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'absenteeismTotal', className: 'text-muted mt-2' }, ['Selecciona una fecha cerrada y genera el reporte.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Dependencia']), el('th', {}, ['Zona']), el('th', {}, ['Nombre Sede']), el('th', {}, ['Planeados']), el('th', {}, ['Contratados']), el('th', {}, ['No contratado']), el('th', {}, ['Total ausentismo']), el('th', {}, ['Total a pagar'])])]),
          el('tbody', { id: 'absenteeismTbody' }, [el('tr', {}, [el('td', { colSpan: 8, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateAbsenteeism', ui)?.addEventListener('click', generateAbsenteeismReport);
    qs('#btnExportAbsenteeism', ui)?.addEventListener('click', exportAbsenteeismExcel);
  }

  function openReport(reportId) {
    selectedReportId = String(reportId || '');
    generatedEmployeesRows = [];
    generatedDailyRows = [];
    generatedHiringRows = [];
    generatedAbsenteeismRows = [];
    ui.querySelectorAll('.report-card').forEach((n) => n.classList.toggle('is-active', n.dataset.id === selectedReportId));
    if (selectedReportId === 'employees_current') {
      renderEmployeesPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'daily_registry') {
      renderDailyPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'hiring_by_sede') {
      renderHiringPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'daily_absenteeism') {
      renderAbsenteeismPanel();
      setMessage(' ');
      return;
    }
    qs('#reportContent', ui).replaceChildren(el('p', { className: 'text-muted' }, ['Selecciona una tarjeta para abrir el reporte.']));
  }

  cards.forEach((card) => card.addEventListener('click', () => openReport(card.dataset.id || '')));

  mount.replaceChildren(ui);
  return () => {};
};
