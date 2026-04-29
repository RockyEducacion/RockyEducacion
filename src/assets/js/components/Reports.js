import { el, qs } from '../utils/dom.js';

export const Reports = (mount, deps = {}, options = {}) => {
  const variant = String(options?.variant || 'client').trim().toLowerCase();
  const reportSets = {
    client: {
      title: 'Reportes Cliente',
      subtitle: 'Reportes operativos y de servicio para entregar al cliente.',
      reports: [
        { id: 'daily_registry', title: 'Registro diario', subtitle: 'Fecha, hora, cedula, nombre, sede, novedad y reemplazo/ausentismo' },
        { id: 'daily_absenteeism', title: 'Ausentismo diario', subtitle: 'Dependencia, zona, sede, planeados, contratados, ausentismo y total a pagar' }
      ]
    },
    company: {
      title: 'Reportes Empresa',
      subtitle: '',
      reports: [
        { id: 'employees_current', title: 'Empleados', subtitle: 'Vigentes con cedula, nombre, cargo, zona, dependencia y sede' },
        { id: 'hiring_by_sede', title: 'Contratacion por Sedes', subtitle: 'Dependencia, zona, sede, planeados y contratados por sede' },
        { id: 'novelties_consolidated', title: 'Consolidado Novedades', subtitle: 'Periodo de tiempo con personas reportadas en novedades distintas de Trabajando y Compensatorio' }
      ]
    }
  };
  const selectedSet = reportSets[variant] || reportSets.client;
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, [selectedSet.title]),
    el('p', { className: 'text-muted' }, [selectedSet.subtitle]),
    el('div', { className: 'reports-grid mt-2', id: 'reportsCards' }, []),
    el('div', { className: 'divider' }, []),
    el('div', { id: 'reportContent' }, [el('p', { className: 'text-muted' }, ['Selecciona una tarjeta para abrir el reporte.'])]),
    el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
  ]);

  const reports = selectedSet.reports;

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
  let generatedNoveltyRows = [];
  let generatedAbsenteeismRows = [];
  let generatedPayrollRows = [];
  let generatedPayrollDays = [];
  let running = false;
  let selectedDailyDate = todayBogota();
  let selectedAbsenteeismDate = todayBogota();
  let selectedNoveltyDateFrom = `${todayBogota().slice(0, 7)}-01`;
  let selectedNoveltyDateTo = todayBogota();
  let selectedPayrollMonth = todayBogota().slice(0, 7);
  let employeesSortKey = 'nombre';
  let employeesSortDir = 1;
  let hiringSortKey = 'dependencia';
  let hiringSortDir = 1;
  let noveltiesSortKey = 'fecha';
  let noveltiesSortDir = -1;

  function setMessage(text) {
    qs('#msg', ui).textContent = text || ' ';
  }

  function todayBogota() {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
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

  function normalizeEmployeesForReport(rawRows = [], sedeRows = [], cargoRows = []) {
    const sedeByCode = new Map((sedeRows || []).map((s) => [String(s.codigo || '').trim(), s || {}]).filter(([k]) => Boolean(k)));
    const cargoByCode = new Map((cargoRows || []).map((c) => [String(c.codigo || '').trim(), c || {}]).filter(([k]) => Boolean(k)));
    const todayISO = todayBogota();
    return (rawRows || [])
      .filter((e) => isCurrentEmployee(e, todayISO))
      .map((e) => {
        const sedeCode = String(e.sedeCodigo || '').trim();
        const sede = sedeByCode.get(sedeCode) || {};
        const cargoCode = String(e.cargoCodigo || '').trim();
        const cargo = cargoByCode.get(cargoCode) || null;
        const alignment = normalizeCargoAlignment(cargo?.alineacionCrud || cargo?.alineacion_crud || e.cargoNombre || '');
        return {
          cedula: String(e.documento || '').trim() || '-',
          nombre: String(e.nombre || '').trim() || '-',
          cargo: String(e.cargoNombre || e.cargoCodigo || '-').trim() || '-',
          tipo: alignment === 'supernumerario' ? 'Supernumerario' : alignment === 'supervisor' ? 'Supervisor' : 'Empleado',
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

  function attendanceKey(item = {}) {
    return [
      String(item?.fecha || '').trim(),
      String(item?.employeeId || item?.empleadoId || '').trim(),
      String(item?.documento || '').trim()
    ].join('|');
  }

  function statusDetailState(row) {
    const estadoDia = String(row?.estadoDia || '').trim();
    const decision = String(row?.decisionCobertura || '').trim().toLowerCase();
    const tipoPersonal = String(row?.tipoPersonal || 'empleado').trim().toLowerCase();
    const reemplazaA = row?.reemplazaANombre || row?.reemplazaADocumento || '-';
    const reemplazadoPor = row?.reemplazadoPorNombre || row?.reemplazadoPorDocumento || '-';

    if (tipoPersonal === 'supernumerario' && estadoDia === 'trabajado_reemplazo') {
      return `Supernumerario reemplazando a ${reemplazaA}`;
    }
    if (decision === 'reemplazo') {
      return `Reemplazado por ${reemplazadoPor}`;
    }
    if (row?.asistio === true) {
      return tipoPersonal === 'supernumerario' ? 'Trabajo supernumerario' : 'Trabajo';
    }
    if (decision === 'ausentismo' || row?.cuentaPagoServicio === false) {
      return 'Ausentismo';
    }
    if (estadoDia === 'sin_registro') {
      return 'Sin registro';
    }
    if (estadoDia === 'incapacidad') {
      return 'Incapacidad';
    }
    if (estadoDia === 'vacaciones') {
      return 'Vacaciones';
    }
    if (estadoDia === 'compensatorio') {
      return 'Compensatorio';
    }
    if (estadoDia === 'ausente_con_novedad') {
      return 'Ausente con novedad';
    }
    if (estadoDia === 'ausente_sin_reemplazo') {
      return 'Ausentismo';
    }
    if (estadoDia === 'no_programado') {
      return 'No programado';
    }
    return estadoDia
      ? estadoDia.replace(/_/g, ' ').replace(/\b\w/g, (ch) => ch.toUpperCase())
      : '-';
  }

  function normalizeDailyRegistryRows(fecha, statusRows = [], attendanceRows = []) {
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

  function noveltyReplacementKeys(row = {}) {
    const fecha = String(row?.fecha || '').trim();
    const employeeId = String(row?.employeeId || row?.empleadoId || '').trim();
    const documento = String(row?.documento || '').trim();
    const keys = [];
    if (fecha && employeeId) keys.push(`${fecha}|id|${employeeId}`);
    if (fecha && documento) keys.push(`${fecha}|doc|${documento}`);
    return keys;
  }

  function buildNoveltyReplacementMap(rows = []) {
    const map = new Map();
    (rows || []).forEach((row) => {
      noveltyReplacementKeys(row).forEach((key) => {
        if (!key) return;
        map.set(key, row);
      });
    });
    return map;
  }

  function noveltyCoverageDetail(row = {}, replacementMap = new Map()) {
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

  function normalizeNoveltyConsolidatedRows(statusRows = [], replacementRows = []) {
    const replacementMap = buildNoveltyReplacementMap(replacementRows);
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
        cobertura: noveltyCoverageDetail(row, replacementMap)
      }))
      .sort((a, b) => {
        const byDate = String(a.fecha || '').localeCompare(String(b.fecha || ''));
        if (byDate !== 0) return byDate;
        const byName = String(a.nombre || '').localeCompare(String(b.nombre || ''));
        if (byName !== 0) return byName;
        return String(a.cedula || '').localeCompare(String(b.cedula || ''));
      });
  }

  function normalizeHiringRows(sedeRows = [], employeeRows = []) {
    const contractedBySede = new Map();

    (employeeRows || []).forEach((emp) => {
      const sedeCode = String(emp.sedeCodigo || '').trim();
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

  function monthBounds(value) {
    const raw = String(value || '').trim();
    if (!/^\d{4}-\d{2}$/.test(raw)) return null;
    const [year, month] = raw.split('-').map((v) => Number(v));
    const lastDay = new Date(year, month, 0).getDate();
    const from = `${raw}-01`;
    const to = `${raw}-${String(lastDay).padStart(2, '0')}`;
    const days = Array.from({ length: lastDay }, (_, idx) => `${raw}-${String(idx + 1).padStart(2, '0')}`);
    return { from, to, days, month: raw };
  }

  function buildPersonMaps(rows = []) {
    const byId = new Map();
    const byDoc = new Map();
    (rows || []).forEach((row) => {
      const id = String(row?.id || '').trim();
      const doc = String(row?.documento || '').trim();
      if (id) byId.set(id, row);
      if (doc) byDoc.set(doc, row);
    });
    return { byId, byDoc };
  }

  function resolvePerson({ id = '', doc = '', byId = new Map(), byDoc = new Map() } = {}) {
    const cleanId = String(id || '').trim();
    const cleanDoc = String(doc || '').trim();
    return (cleanId && byId.get(cleanId)) || (cleanDoc && byDoc.get(cleanDoc)) || null;
  }

  function normalizePayrollRows(monthValue, statusRows = [], sedeRows = []) {
    const bounds = monthBounds(monthValue);
    if (!bounds) throw new Error('Selecciona un mes valido.');

    const snapshotBySede = new Map();
    const workerMapByDaySede = new Map();
    const sedeCodesWithData = new Set();

    function ensureBucket(day, sedeCode) {
      const key = String(day || '') + '|' + String(sedeCode || '');
      if (!workerMapByDaySede.has(key)) workerMapByDaySede.set(key, new Map());
      return workerMapByDaySede.get(key);
    }

    (statusRows || []).forEach((row) => {
      const sedeCode = String(row?.sedeCodigo || '').trim();
      if (sedeCode && !snapshotBySede.has(sedeCode)) snapshotBySede.set(sedeCode, row);
      if (!shouldIncludePayrollMatrixRow(row)) return;
      const day = String(row?.fecha || '').trim();
      if (!day) return;
      const bucket = ensureBucket(day, sedeCode);
      const workerKey = String(row?.documento || row?.employeeId || row?.nombre || '').trim();
      if (!workerKey) return;
      if (sedeCode) sedeCodesWithData.add(sedeCode);
      bucket.set(workerKey, {
        doc: String(row?.documento || '').trim() || '-',
        name: String(row?.nombre || '').trim() || '-',
        label: buildPayrollMatrixLabel(row),
        sortName: normalizeText(String(row?.nombre || row?.documento || '-').trim())
      });
    });

    const rows = [];
    const exportRows = [];
    (sedeRows || [])
      .filter((sede) => {
        const sedeCode = String(sede?.codigo || '').trim();
        if (!sedeCode) return false;
        const active = String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo';
        return active || sedeCodesWithData.has(sedeCode);
      })
      .sort((a, b) => {
        const dep = String(a.dependenciaNombre || '').localeCompare(String(b.dependenciaNombre || ''));
        if (dep !== 0) return dep;
        const zone = String(a.zonaNombre || '').localeCompare(String(b.zonaNombre || ''));
        if (zone !== 0) return zone;
        return String(a.nombre || '').localeCompare(String(b.nombre || ''));
      })
      .forEach((sede) => {
        const sedeCode = String(sede.codigo || '').trim();
        const snapshot = snapshotBySede.get(sedeCode) || null;
        const planned = parseOperatorCount(sede.numeroOperarios ?? 0);
        const dependencia = String(snapshot?.dependenciaNombreSnapshot || sede.dependenciaNombre || sede.dependenciaCodigo || '-').trim() || '-';
        const zona = String(snapshot?.zonaNombreSnapshot || sede.zonaNombre || sede.zonaCodigo || '-').trim() || '-';
        const sedeNombre = String(snapshot?.sedeNombreSnapshot || sede.nombre || sede.codigo || '-').trim() || '-';
        const dailyLists = bounds.days.map((day) => {
          const workers = Array.from((workerMapByDaySede.get(day + '|' + sedeCode) || new Map()).values());
          workers.sort((a, b) => {
            const byName = String(a.sortName || '').localeCompare(String(b.sortName || ''));
            if (byName !== 0) return byName;
            return String(a.doc || '').localeCompare(String(b.doc || ''));
          });
          return workers;
        });
        const maxWorkers = dailyLists.reduce((max, list) => Math.max(max, list.length), 0);
        const slotCount = Math.max(planned, maxWorkers);
        if (slotCount <= 0) return;
        for (let slot = 0; slot < slotCount; slot += 1) {
          const isExtra = planned > 0 && slot >= planned;
          const row = {
            dependencia,
            zona,
            sede: sedeNombre,
            cupo: isExtra ? 'Extra ' + String(slot - planned + 1) : 'Cupo ' + String(slot + 1)
          };
          bounds.days.forEach((day, index) => {
            row['d' + String(index + 1)] = dailyLists[index]?.[slot]?.label || '';
          });
          rows.push(row);

          const exportRow = {
            Dependencia: row.dependencia,
            Zona: row.zona,
            Sede: row.sede,
            Cupo: row.cupo
          };
          bounds.days.forEach((day, index) => {
            exportRow['Dia ' + String(index + 1).padStart(2, '0')] = dailyLists[index]?.[slot]?.label || '';
          });
          exportRows.push(exportRow);
        }
      });

    return {
      days: bounds.days.map((day, index) => ({ iso: day, key: 'd' + String(index + 1), label: String(index + 1) })),
      rows,
      exportRows,
      month: bounds.month
    };
  }

  function shouldIncludePayrollMatrixRow(row = {}) {
    const type = String(row?.tipoPersonal || '').trim();
    if (type === 'supernumerario') return String(row?.estadoDia || '').trim() === 'trabajado_reemplazo';
    return row?.asistio === true;
  }

  function buildPayrollMatrixLabel(row = {}) {
    const name = String(row?.nombre || row?.documento || '-').trim() || '-';
    if (String(row?.tipoPersonal || '').trim() === 'supernumerario' && row?.reemplazaANombre) {
      return name + ' (Reemplaza a ' + row.reemplazaANombre + ')';
    }
    return name;
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
    if (['2', '3', '4', '5', '8', '9'].includes(code)) return true;
    if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
    const name = normalizeText(baseNovedadName(att.novedadNombre || att.novedad || ''));
    if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
    return false;
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

  function computeOperationalAbsenteeism(planeados, contratados, cubiertos) {
    const planned = Math.max(0, Number(planeados || 0));
    const contracted = Math.max(0, Number(contratados || 0));
    const covered = Math.max(0, Number(cubiertos || 0));
    if (planned <= 0) return 0;
    return Math.max(0, Math.min(planned, contracted) - covered);
  }

  function normalizeAbsenteeismRows(fecha, statusRows = [], sedeClosureRows = []) {
    const baseRows = (Array.isArray(statusRows) ? statusRows : []).filter((row) => String(row?.tipoPersonal || '').trim() === 'empleado');
    const sedeClosuresByCode = new Map((Array.isArray(sedeClosureRows) ? sedeClosureRows : []).map((row) => [String(row?.sedeCodigo || '').trim(), row]));
    const employeeRowsBySede = new Map();

    baseRows.forEach((row) => {
      const sedeCode = String(row?.sedeCodigo || '').trim();
      if (!sedeCode) return;
      if (!employeeRowsBySede.has(sedeCode)) employeeRowsBySede.set(sedeCode, []);
      employeeRowsBySede.get(sedeCode).push(row);
    });

    const fixedSnapshotCodes = (Array.isArray(sedeClosureRows) ? sedeClosureRows : [])
      .map((row) => String(row?.sedeCodigo || '').trim())
      .filter(Boolean);

    const allCodes = new Set([...fixedSnapshotCodes, ...Array.from(employeeRowsBySede.keys())]);

    return Array.from(allCodes)
      .map((sedeCode) => {
        const orderedRows = [...(employeeRowsBySede.get(sedeCode) || [])].sort((a, b) => String(a?.nombre || '').localeCompare(String(b?.nombre || '')));
        const scheduledRows = orderedRows.filter((row) => row?.servicioProgramado === true);
        const actualRows = orderedRows.filter((row) => row?.asistio === true || row?.asistio === false || row?.sourceIncapacityId || row?.sourceAttendanceId || row?.sourceReplacementId || row?.sourceAbsenteeismId);
        const sedeSnapshot = sedeClosuresByCode.get(sedeCode) || null;
        const firstRow = orderedRows[0] || null;
        const scheduled = Boolean(sedeSnapshot) || scheduledRows.length > 0;
        const planeados = parseOperatorCount(sedeSnapshot?.planeados);
        const contratados = scheduledRows.length;
        const noContratado = Math.max(0, planeados - contratados);
        const novedadSinReemplazo = scheduledRows.filter((row) => isNoveltyWithoutReplacement(row)).length;
        const cubiertos = scheduledRows.filter((row) => row?.cuentaPagoServicio === true).length;
        const totalAusentismo = computeOperationalAbsenteeism(planeados, contratados, cubiertos);
        const totalPagar = Math.max(0, planeados - noContratado - totalAusentismo);

        return {
          fecha,
          dependencia: String(sedeSnapshot?.dependenciaNombre || firstRow?.dependenciaNombreSnapshot || 'Sin dependencia').trim() || 'Sin dependencia',
          zona: String(sedeSnapshot?.zonaNombre || firstRow?.zonaNombreSnapshot || 'Sin zona').trim() || 'Sin zona',
          sede: String(sedeSnapshot?.sedeNombre || firstRow?.sedeNombreSnapshot || sedeCode || '-').trim() || '-',
          planeados,
          contratados,
          noContratado,
          novedadSinReemplazo,
          totalAusentismo,
          totalPagar,
          actualCount: actualRows.length
        };
      })
      .filter((row) => row.planeados > 0 || row.contratados > 0 || row.noContratado > 0 || row.totalAusentismo > 0 || row.totalPagar > 0 || row.actualCount > 0)
      .sort((a, b) => {
        const byDependency = String(a.dependencia || '').localeCompare(String(b.dependencia || ''));
        if (byDependency !== 0) return byDependency;
        const byZone = String(a.zona || '').localeCompare(String(b.zona || ''));
        if (byZone !== 0) return byZone;
        return String(a.sede || '').localeCompare(String(b.sede || ''));
      });
  }

  function renderEmployeesRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin empleados vigentes para mostrar.'])])];
    return rows.map((r) => el('tr', {}, [el('td', {}, [r.cedula]), el('td', {}, [r.nombre]), el('td', {}, [r.cargo]), el('td', {}, [r.tipo]), el('td', {}, [r.zona]), el('td', {}, [r.dependencia]), el('td', {}, [r.sede])]));
  }

  function getFilteredEmployeesRows() {
    const search = normalizeText(qs('#employeesSearch', ui)?.value || '');
    const dependency = String(qs('#employeesDependencyFilter', ui)?.value || '').trim();
    const sede = String(qs('#employeesSedeFilter', ui)?.value || '').trim();
    return (generatedEmployeesRows || []).filter((row) => {
      if (dependency && row.dependencia !== dependency) return false;
      if (sede && row.sede !== sede) return false;
      if (!search) return true;
      return normalizeText(`${row.cedula || ''} ${row.nombre || ''} ${row.cargo || ''} ${row.tipo || ''} ${row.zona || ''} ${row.dependencia || ''} ${row.sede || ''}`).includes(search);
    });
  }

  function renderEmployeesTable() {
    const tbody = qs('#employeesTbody', ui);
    if (!tbody) return;
    const filteredRows = getFilteredEmployeesRows();
    const rows = sortRows(filteredRows, employeesSortKey, employeesSortDir);
    if (!rows.length) {
      tbody.replaceChildren(el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin empleados para los filtros actuales.'])]));
    } else {
      tbody.replaceChildren(...renderEmployeesRows(rows));
    }
    const totalsNode = qs('#employeesFilteredTotal', ui);
    if (totalsNode) totalsNode.textContent = `Registros filtrados: ${rows.length}`;
    updateSortIndicators(ui, '#employeesTable th[data-sort-employees]', 'data-sort-employees', employeesSortKey, employeesSortDir);
  }

  function syncEmployeesDependencyOptions(rows = []) {
    const select = qs('#employeesDependencyFilter', ui);
    if (!select) return;
    const previous = String(select.value || '').trim();
    const options = Array.from(new Set((rows || []).map((row) => String(row?.dependencia || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    select.replaceChildren(
      el('option', { value: '' }, ['Todas']),
      ...options.map((value) => el('option', { value, selected: value === previous }, [value]))
    );
    select.value = options.includes(previous) ? previous : '';
  }

  function syncEmployeesSedeOptions(rows = []) {
    const select = qs('#employeesSedeFilter', ui);
    if (!select) return;
    const previous = String(select.value || '').trim();
    const options = Array.from(new Set((rows || []).map((row) => String(row?.sede || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    select.replaceChildren(
      el('option', { value: '' }, ['Todas']),
      ...options.map((value) => el('option', { value, selected: value === previous }, [value]))
    );
    select.value = options.includes(previous) ? previous : '';
  }

  function renderDailyRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin registros para la fecha seleccionada.'])])];
    return rows.map((r) => el('tr', {}, [el('td', {}, [r.fecha]), el('td', {}, [r.hora]), el('td', {}, [r.cedula]), el('td', {}, [r.nombre]), el('td', {}, [r.sede]), el('td', {}, [r.novedad]), el('td', {}, [r.estado])]));
  }

  function renderNoveltyRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin novedades para el periodo seleccionado.'])])];
    return rows.map((r) => el('tr', {}, [
      el('td', {}, [r.fecha]),
      el('td', {}, [r.cedula]),
      el('td', {}, [r.nombre]),
      el('td', {}, [r.sede]),
      el('td', {}, [r.novedad]),
      el('td', {}, [r.cobertura])
    ]));
  }

  function renderNoveltiesTable() {
    const tbody = qs('#noveltiesTbody', ui);
    if (!tbody) return;
    const rows = sortRows(generatedNoveltyRows, noveltiesSortKey, noveltiesSortDir);
    tbody.replaceChildren(...renderNoveltyRows(rows));
    updateSortIndicators(ui, '#noveltiesTable th[data-sort-novelties]', 'data-sort-novelties', noveltiesSortKey, noveltiesSortDir);
  }

  function renderHiringRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin sedes activas para mostrar.'])])];
    return rows.map((r) =>
      el('tr', {}, [
        el('td', {}, [r.dependencia]),
        el('td', {}, [r.zona]),
        el('td', {}, [r.sede]),
        el('td', {}, [String(r.empleadosPlaneados)]),
        el('td', {}, [String(r.empleadosContratados)]),
        el('td', {}, [String(r.diferencia)])
      ])
    );
  }

  function getFilteredHiringRows() {
    const search = normalizeText(qs('#hiringSearch', ui)?.value || '');
    const dependency = String(qs('#hiringDependencyFilter', ui)?.value || '').trim();
    const sede = String(qs('#hiringSedeFilter', ui)?.value || '').trim();
    return (generatedHiringRows || []).filter((row) => {
      if (dependency && row.dependencia !== dependency) return false;
      if (sede && row.sede !== sede) return false;
      if (!search) return true;
      return normalizeText(`${row.dependencia || ''} ${row.zona || ''} ${row.sede || ''}`).includes(search);
    });
  }

  function renderHiringTable() {
    const tbody = qs('#hiringTbody', ui);
    if (!tbody) return;
    const filteredRows = getFilteredHiringRows();
    const rows = sortRows(filteredRows, hiringSortKey, hiringSortDir);
    if (!rows.length) {
      tbody.replaceChildren(el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin sedes para los filtros actuales.'])]));
    } else {
      tbody.replaceChildren(...renderHiringRows(rows));
    }
    const totalsNode = qs('#hiringFilteredTotal', ui);
    if (totalsNode) totalsNode.textContent = `Sedes filtradas: ${rows.length}`;
    updateSortIndicators(ui, '#hiringTable th[data-sort-hiring]', 'data-sort-hiring', hiringSortKey, hiringSortDir);
  }

  function syncHiringDependencyOptions(rows = []) {
    const select = qs('#hiringDependencyFilter', ui);
    if (!select) return;
    const previous = String(select.value || '').trim();
    const options = Array.from(new Set((rows || []).map((row) => String(row?.dependencia || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    select.replaceChildren(
      el('option', { value: '' }, ['Todas']),
      ...options.map((value) => el('option', { value, selected: value === previous }, [value]))
    );
    select.value = options.includes(previous) ? previous : '';
  }

  function syncHiringSedeOptions(rows = []) {
    const select = qs('#hiringSedeFilter', ui);
    if (!select) return;
    const previous = String(select.value || '').trim();
    const options = Array.from(new Set((rows || []).map((row) => String(row?.sede || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
    select.replaceChildren(
      el('option', { value: '' }, ['Todas']),
      ...options.map((value) => el('option', { value, selected: value === previous }, [value]))
    );
    select.value = options.includes(previous) ? previous : '';
  }

  function renderAbsenteeismRows(rows = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 9, className: 'text-muted' }, ['Sin datos para la fecha seleccionada.'])])];
    return rows.map((r) =>
      el('tr', {}, [
        el('td', {}, [r.dependencia]),
        el('td', {}, [r.zona]),
        el('td', {}, [r.sede]),
        el('td', {}, [String(r.planeados)]),
        el('td', {}, [String(r.contratados)]),
        el('td', {}, [String(r.noContratado)]),
        el('td', {}, [String(r.novedadSinReemplazo)]),
        el('td', {}, [String(r.totalAusentismo)]),
        el('td', {}, [String(r.totalPagar)])
      ])
    );
  }

  function renderPayrollRows(rows = [], days = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 4 + days.length, className: 'text-muted' }, ['Sin datos para el mes seleccionado.'])])];
    return rows.map((r) => el('tr', {}, [
      el('td', {}, [r.dependencia]),
      el('td', {}, [r.zona]),
      el('td', {}, [r.sede]),
      el('td', {}, [r.cupo]),
      ...days.map((day) => el('td', {}, [r[day.key] || '']))
    ]));
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
      const [rawEmployees, rawSedes, rawCargos] = await Promise.all([streamOnce((ok, fail) => deps.streamEmployees?.(ok, fail)), streamOnce((ok, fail) => deps.streamSedes?.(ok, fail)), streamOnce((ok, fail) => deps.streamCargos?.(ok, fail))]);
      generatedEmployeesRows = normalizeEmployeesForReport(rawEmployees, rawSedes, rawCargos);
      const totals = generatedEmployeesRows.reduce((acc, row) => {
        if (row.tipo === 'Supernumerario') acc.supernumerarios += 1;
        else if (row.tipo === 'Supervisor') acc.supervisores += 1;
        else acc.empleados += 1;
        return acc;
      }, { empleados: 0, supernumerarios: 0, supervisores: 0 });
      const totalNode = qs('#employeesTotal', ui);
      if (totalNode) totalNode.textContent = `Total registros vigentes: ${generatedEmployeesRows.length} | Empleados: ${totals.empleados} | Supernumerarios: ${totals.supernumerarios} | Supervisores: ${totals.supervisores}`;
      syncEmployeesDependencyOptions(generatedEmployeesRows);
      syncEmployeesSedeOptions(generatedEmployeesRows);
      renderEmployeesTable();
      if (btnExport) btnExport.disabled = generatedEmployeesRows.length === 0;
      setMessage(' ');
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
      const isClosed = await deps.isOperationDayClosed?.(date);
      if (!isClosed) throw new Error('Solo se pueden generar reportes historicos de dias cerrados.');
      const [statusRows, attendanceRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(date, date) || [],
        deps.listAttendanceRange?.(date, date) || []
      ]);
      generatedDailyRows = normalizeDailyRegistryRows(date, statusRows, attendanceRows);
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
      const [rawSedes, rawEmployees] = await Promise.all([
        streamOnce((ok, fail) => deps.streamSedes?.(ok, fail)),
        streamOnce((ok, fail) => deps.streamActiveBaseEmployees?.(ok, fail))
      ]);
      generatedHiringRows = normalizeHiringRows(rawSedes, rawEmployees);
      const totals = generatedHiringRows.reduce((acc, row) => {
        acc.planeados += Number(row.empleadosPlaneados || 0);
        acc.contratados += Number(row.empleadosContratados || 0);
        acc.diferencia += Number(row.diferencia || 0);
        return acc;
      }, { planeados: 0, contratados: 0, diferencia: 0 });
      const totalNode = qs('#hiringTotal', ui);
      if (totalNode) totalNode.textContent = `Sedes: ${generatedHiringRows.length} | Planeados: ${totals.planeados} | Contratados: ${totals.contratados} | Diferencia: ${totals.diferencia}`;
      syncHiringDependencyOptions(generatedHiringRows);
      syncHiringSedeOptions(generatedHiringRows);
      renderHiringTable();
      if (btnExport) btnExport.disabled = generatedHiringRows.length === 0;
      setMessage(' ');
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

  async function generateNoveltyReport() {
    if (running) return;
    const dateFrom = String(qs('#noveltiesDateFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#noveltiesDateTo', ui)?.value || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateFrom) || !/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      setMessage('Selecciona un rango valido para generar el consolidado de novedades.');
      return;
    }
    if (dateFrom > dateTo) {
      setMessage('La fecha inicial no puede ser mayor que la fecha final.');
      return;
    }
    running = true;
    selectedNoveltyDateFrom = dateFrom;
    selectedNoveltyDateTo = dateTo;
    const btnGenerate = qs('#btnGenerateNovelties', ui);
    const btnExport = qs('#btnExportNovelties', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const [statusRows, replacementRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(dateFrom, dateTo) || [],
        deps.listImportReplacementsRange?.(dateFrom, dateTo) || []
      ]);
      generatedNoveltyRows = normalizeNoveltyConsolidatedRows(statusRows, replacementRows);
      const peopleCount = new Set(generatedNoveltyRows.map((row) => `${row.cedula}|${row.nombre}`)).size;
      const totalNode = qs('#noveltiesTotal', ui);
      if (totalNode) totalNode.textContent = `Periodo: ${dateFrom} a ${dateTo} | Registros: ${generatedNoveltyRows.length} | Personas: ${peopleCount}`;
      renderNoveltiesTable();
      if (btnExport) btnExport.disabled = generatedNoveltyRows.length === 0;
      setMessage(' ');
    } catch (e) {
      setMessage(`Error al generar consolidado de novedades: ${e?.message || e}`);
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
      const [statusRows, sedeClosureRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(date, date) || [],
        deps.listDailySedeClosuresRange?.(date, date) || []
      ]);
      generatedAbsenteeismRows = normalizeAbsenteeismRows(date, statusRows, sedeClosureRows);
      const totals = generatedAbsenteeismRows.reduce(
        (acc, row) => {
          acc.planeados += Number(row.planeados || 0);
          acc.contratados += Number(row.contratados || 0);
          acc.noContratado += Number(row.noContratado || 0);
          acc.novedadSinReemplazo += Number(row.novedadSinReemplazo || 0);
          acc.ausentismo += Number(row.totalAusentismo || 0);
          acc.totalPagar += Number(row.totalPagar || 0);
          return acc;
        },
        { planeados: 0, contratados: 0, noContratado: 0, novedadSinReemplazo: 0, ausentismo: 0, totalPagar: 0 }
      );
      const totalNode = qs('#absenteeismTotal', ui);
      if (totalNode) {
        totalNode.textContent = `Sedes: ${generatedAbsenteeismRows.length} | Planeados: ${totals.planeados} | Contratados: ${totals.contratados} | No contratado: ${totals.noContratado} | Novedad sin reemplazo: ${totals.novedadSinReemplazo} | Ausentismo: ${totals.ausentismo} | Total a pagar: ${totals.totalPagar}`;
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

  async function generatePayrollReport() {
    if (running) return;
    const month = String(qs('#payrollMonth', ui)?.value || '').trim();
    if (!/^\d{4}-\d{2}$/.test(month)) {
      setMessage('Selecciona un mes valido para generar el reporte.');
      return;
    }
    running = true;
    selectedPayrollMonth = month;
    const btnGenerate = qs('#btnGeneratePayroll', ui);
    const btnExport = qs('#btnExportPayroll', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const bounds = monthBounds(month);
      const [statusRows, sedeRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(bounds.from, bounds.to) || [],
        streamOnce((ok, fail) => deps.streamSedes?.(ok, fail))
      ]);
      const normalized = normalizePayrollRows(month, statusRows, sedeRows);
      generatedPayrollRows = normalized.exportRows;
      generatedPayrollDays = normalized.days;
      const totalNode = qs('#payrollTotal', ui);
      if (totalNode) totalNode.textContent = `Mes: ${normalized.month} | Filas: ${normalized.rows.length} | Dias: ${normalized.days.length}`;
      const headRow = qs('#payrollHeadRow', ui);
      if (headRow) {
        headRow.replaceChildren(
          el('th', {}, ['Dependencia']),
          el('th', {}, ['Zona']),
          el('th', {}, ['Sede']),
          el('th', {}, ['Cupo']),
          ...normalized.days.map((day) => el('th', {}, [day.label]))
        );
      }
      const tbody = qs('#payrollTbody', ui);
      if (tbody) tbody.replaceChildren(...renderPayrollRows(normalized.rows, normalized.days));
      if (btnExport) btnExport.disabled = generatedPayrollRows.length === 0;
      setMessage(`Reporte de nomina generado para ${month}. Filas: ${normalized.rows.length}`);
    } catch (e) {
      setMessage(`Error al generar reporte de nomina: ${e?.message || e}`);
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
      const ws = mod.utils.json_to_sheet(generatedEmployeesRows.map((r) => ({ Cedula: r.cedula, Nombre: r.nombre, Cargo: r.cargo, Tipo: r.tipo, Zona: r.zona, Dependencia: r.dependencia, Sede: r.sede })));
      ws['!cols'] = [{ wch: 18 }, { wch: 35 }, { wch: 28 }, { wch: 18 }, { wch: 24 }, { wch: 26 }, { wch: 30 }];
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
          Estado: r.estado
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
          'Empleados Contratados': r.empleadosContratados,
          Diferencia: r.diferencia
        }))
      );
      ws['!cols'] = [{ wch: 28 }, { wch: 24 }, { wch: 32 }, { wch: 20 }, { wch: 22 }, { wch: 14 }];
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

  async function exportNoveltyExcel() {
    try {
      if (!generatedNoveltyRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportNovelties', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(
        generatedNoveltyRows.map((r) => ({
          Fecha: r.fecha,
          Cedula: r.cedula,
          Nombre: r.nombre,
          Sede: r.sede,
          Novedad: r.novedad,
          'Reemplazo/Ausentismo': r.cobertura
        }))
      );
      ws['!cols'] = [{ wch: 12 }, { wch: 18 }, { wch: 30 }, { wch: 30 }, { wch: 28 }, { wch: 24 }];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Consolidado Novedades');
      mod.writeFile(wb, `reporte_consolidado_novedades_${selectedNoveltyDateFrom}_a_${selectedNoveltyDateTo}.xlsx`);
      setMessage(`Excel generado correctamente. Registros: ${generatedNoveltyRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportNovelties', ui);
      if (btn) {
        btn.disabled = generatedNoveltyRows.length === 0;
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

  async function exportPayrollExcel() {
    try {
      if (!generatedPayrollRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportPayroll', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const ws = mod.utils.json_to_sheet(generatedPayrollRows);
      const cols = [
        { wch: 24 },
        { wch: 18 },
        { wch: 28 },
        { wch: 14 },
        ...generatedPayrollDays.map(() => ({ wch: 26 }))
      ];
      ws['!cols'] = cols;
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Nomina');
      mod.writeFile(wb, `reporte_nomina_${selectedPayrollMonth}.xlsx`);
      setMessage(`Excel de nomina generado para ${selectedPayrollMonth}. Filas: ${generatedPayrollRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel de nomina: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportPayroll', ui);
      if (btn) {
        btn.disabled = generatedPayrollRows.length === 0;
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
      el('div', { className: 'form-row mt-2' }, [
        el('div', {}, [
          el('label', { className: 'label' }, ['Buscar']),
          el('input', { id: 'employeesSearch', className: 'input', placeholder: 'Cedula, nombre, cargo, dependencia o sede...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Dependencia']),
          el('select', { id: 'employeesDependencyFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Sede']),
          el('select', { id: 'employeesSedeFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'employeesTable' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-employees': 'cedula', style: 'cursor:pointer' }, ['Cedula']),
            el('th', { 'data-sort-employees': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
            el('th', { 'data-sort-employees': 'cargo', style: 'cursor:pointer' }, ['Cargo']),
            el('th', { 'data-sort-employees': 'tipo', style: 'cursor:pointer' }, ['Tipo']),
            el('th', { 'data-sort-employees': 'zona', style: 'cursor:pointer' }, ['Zona']),
            el('th', { 'data-sort-employees': 'dependencia', style: 'cursor:pointer' }, ['Dependencia']),
            el('th', { 'data-sort-employees': 'sede', style: 'cursor:pointer' }, ['Sede'])
          ])]),
          el('tbody', { id: 'employeesTbody' }, [el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ]),
      el('p', { id: 'employeesTotal', className: 'text-muted mt-2' }, ['Genera el reporte para ver resultados.']),
      el('p', { id: 'employeesFilteredTotal', className: 'text-muted' }, ['Registros filtrados: 0'])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateEmployees', ui)?.addEventListener('click', generateEmployeesReport);
    qs('#btnExportEmployees', ui)?.addEventListener('click', exportEmployeesExcel);
    qs('#employeesSearch', ui)?.addEventListener('input', renderEmployeesTable);
    qs('#employeesDependencyFilter', ui)?.addEventListener('change', renderEmployeesTable);
    qs('#employeesSedeFilter', ui)?.addEventListener('change', renderEmployeesTable);
    ui.querySelectorAll('#employeesTable th[data-sort-employees]').forEach((th) => {
      th.addEventListener('click', () => {
        const key = String(th.getAttribute('data-sort-employees') || '').trim();
        if (!key) return;
        if (employeesSortKey === key) employeesSortDir *= -1;
        else {
          employeesSortKey = key;
          employeesSortDir = 1;
        }
        renderEmployeesTable();
      });
    });
    updateSortIndicators(ui, '#employeesTable th[data-sort-employees]', 'data-sort-employees', employeesSortKey, employeesSortDir);
  }

  function renderDailyPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Registro diario'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Fecha']), el('input', { id: 'dailyDate', className: 'input', type: 'date', value: selectedDailyDate, style: 'max-width:180px' })]),
        el('button', { id: 'btnGenerateDaily', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportDaily', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'dailyTotal', className: 'text-muted mt-2' }, ['Selecciona una fecha cerrada y genera el reporte.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Fecha']), el('th', {}, ['Hora']), el('th', {}, ['Cedula']), el('th', {}, ['Nombre']), el('th', {}, ['Sede']), el('th', {}, ['Novedad']), el('th', {}, ['Estado'])])]),
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
        el('button', { id: 'btnGenerateHiring', className: 'btn right', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportHiring', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('div', { className: 'form-row mt-2' }, [
        el('div', {}, [
          el('label', { className: 'label' }, ['Buscar']),
          el('input', { id: 'hiringSearch', className: 'input', placeholder: 'Dependencia, zona o sede...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Dependencia']),
          el('select', { id: 'hiringDependencyFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ]),
        el('div', {}, [
          el('label', { className: 'label' }, ['Sede']),
          el('select', { id: 'hiringSedeFilter', className: 'input' }, [el('option', { value: '' }, ['Todas'])])
        ])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'hiringTable' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-hiring': 'dependencia', style: 'cursor:pointer' }, ['Dependencia']),
            el('th', { 'data-sort-hiring': 'zona', style: 'cursor:pointer' }, ['Zona']),
            el('th', { 'data-sort-hiring': 'sede', style: 'cursor:pointer' }, ['Nombre Sede']),
            el('th', { 'data-sort-hiring': 'empleadosPlaneados', style: 'cursor:pointer' }, ['Empleados Planeados']),
            el('th', { 'data-sort-hiring': 'empleadosContratados', style: 'cursor:pointer' }, ['Empleados Contratados']),
            el('th', { 'data-sort-hiring': 'diferencia', style: 'cursor:pointer' }, ['Diferencia'])
          ])]),
          el('tbody', { id: 'hiringTbody' }, [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ]),
      el('p', { id: 'hiringTotal', className: 'text-muted mt-2' }, ['Genera el reporte para ver resultados.']),
      el('p', { id: 'hiringFilteredTotal', className: 'text-muted' }, ['Sedes filtradas: 0'])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateHiring', ui)?.addEventListener('click', generateHiringReport);
    qs('#btnExportHiring', ui)?.addEventListener('click', exportHiringExcel);
    qs('#hiringSearch', ui)?.addEventListener('input', renderHiringTable);
    qs('#hiringDependencyFilter', ui)?.addEventListener('change', renderHiringTable);
    qs('#hiringSedeFilter', ui)?.addEventListener('change', renderHiringTable);
    ui.querySelectorAll('#hiringTable th[data-sort-hiring]').forEach((th) => {
      th.addEventListener('click', () => {
        const key = String(th.getAttribute('data-sort-hiring') || '').trim();
        if (!key) return;
        if (hiringSortKey === key) hiringSortDir *= -1;
        else {
          hiringSortKey = key;
          hiringSortDir = 1;
        }
        renderHiringTable();
      });
    });
    updateSortIndicators(ui, '#hiringTable th[data-sort-hiring]', 'data-sort-hiring', hiringSortKey, hiringSortDir);
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

  function updateSortIndicators(scope, selector, attr, key, dir) {
    scope.querySelectorAll(selector).forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v\u25B2\u25BC]$/, '');
      th.dataset.baseLabel = base;
      th.textContent = String(th.getAttribute(attr) || '').trim() === key ? `${base} ${dir === 1 ? '\u25B2' : '\u25BC'}` : base;
    });
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
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Dependencia']), el('th', {}, ['Zona']), el('th', {}, ['Nombre Sede']), el('th', {}, ['Planeados']), el('th', {}, ['Contratados']), el('th', {}, ['No contratado']), el('th', {}, ['Novedad sin reemplazo']), el('th', {}, ['Total ausentismo']), el('th', {}, ['Total a pagar'])])]),
          el('tbody', { id: 'absenteeismTbody' }, [el('tr', {}, [el('td', { colSpan: 9, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateAbsenteeism', ui)?.addEventListener('click', generateAbsenteeismReport);
    qs('#btnExportAbsenteeism', ui)?.addEventListener('click', exportAbsenteeismExcel);
  }

  function renderPayrollPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Nomina mensual'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Mes']), el('input', { id: 'payrollMonth', className: 'input', type: 'month', value: selectedPayrollMonth, style: 'max-width:180px' })]),
        el('button', { id: 'btnGeneratePayroll', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportPayroll', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'payrollTotal', className: 'text-muted mt-2' }, ['Selecciona el mes y genera el reporte.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', { id: 'payrollHeadRow' }, [
            el('th', {}, ['Dependencia']),
            el('th', {}, ['Zona']),
            el('th', {}, ['Sede']),
            el('th', {}, ['Cupo'])
          ])]),
          el('tbody', { id: 'payrollTbody' }, [el('tr', {}, [el('td', { colSpan: 4, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGeneratePayroll', ui)?.addEventListener('click', generatePayrollReport);
    qs('#btnExportPayroll', ui)?.addEventListener('click', exportPayrollExcel);
  }

  function renderNoveltiesPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Consolidado Novedades'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Desde']), el('input', { id: 'noveltiesDateFrom', className: 'input', type: 'date', value: selectedNoveltyDateFrom, max: todayBogota(), style: 'max-width:180px' })]),
        el('div', {}, [el('label', { className: 'label' }, ['Hasta']), el('input', { id: 'noveltiesDateTo', className: 'input', type: 'date', value: selectedNoveltyDateTo, max: todayBogota(), style: 'max-width:180px' })]),
        el('button', { id: 'btnGenerateNovelties', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportNovelties', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table', id: 'noveltiesTable' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-novelties': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
            el('th', { 'data-sort-novelties': 'cedula', style: 'cursor:pointer' }, ['Cedula']),
            el('th', { 'data-sort-novelties': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
            el('th', { 'data-sort-novelties': 'sede', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-novelties': 'novedad', style: 'cursor:pointer' }, ['Novedad']),
            el('th', { 'data-sort-novelties': 'cobertura', style: 'cursor:pointer' }, ['Reemplazo/Ausentismo'])
          ])]),
          el('tbody', { id: 'noveltiesTbody' }, [el('tr', {}, [el('td', { colSpan: 6, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ]),
      el('p', { id: 'noveltiesTotal', className: 'text-muted mt-2' }, ['Selecciona el periodo y genera el reporte.'])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateNovelties', ui)?.addEventListener('click', generateNoveltyReport);
    qs('#btnExportNovelties', ui)?.addEventListener('click', exportNoveltyExcel);
    ui.querySelectorAll('#noveltiesTable th[data-sort-novelties]').forEach((th) => {
      th.addEventListener('click', () => {
        const key = String(th.getAttribute('data-sort-novelties') || '').trim();
        if (!key) return;
        if (noveltiesSortKey === key) noveltiesSortDir *= -1;
        else {
          noveltiesSortKey = key;
          noveltiesSortDir = key === 'fecha' ? -1 : 1;
        }
        renderNoveltiesTable();
      });
    });
    updateSortIndicators(ui, '#noveltiesTable th[data-sort-novelties]', 'data-sort-novelties', noveltiesSortKey, noveltiesSortDir);
  }

  function openReport(reportId) {
    selectedReportId = String(reportId || '');
    generatedEmployeesRows = [];
    generatedDailyRows = [];
    generatedHiringRows = [];
    generatedNoveltyRows = [];
    generatedAbsenteeismRows = [];
    generatedPayrollRows = [];
    generatedPayrollDays = [];
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
    if (selectedReportId === 'novelties_consolidated') {
      renderNoveltiesPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'daily_absenteeism') {
      renderAbsenteeismPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'monthly_payroll') {
      renderPayrollPanel();
      setMessage(' ');
      return;
    }
    qs('#reportContent', ui).replaceChildren(el('p', { className: 'text-muted' }, ['Selecciona una tarjeta para abrir el reporte.']));
  }

  cards.forEach((card) => card.addEventListener('click', () => openReport(card.dataset.id || '')));

  mount.replaceChildren(ui);
  return () => {};
};
