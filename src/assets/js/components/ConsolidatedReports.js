import { el, qs } from '../utils/dom.js';

export const ConsolidatedReports = (mount, deps = {}) => {
  const selectedSet = {
    title: 'Reportes consolidados',
    subtitle: 'Reportes de analisis por periodo, contratacion y novedades.',
    reports: [
      { id: 'employees_current', title: 'Empleados', subtitle: 'Vigentes con cedula, nombre, cargo, zona, dependencia y sede' },
      { id: 'attendance_without_fs', title: 'Consolidado asistencia (Sin FS)', subtitle: 'Rango de fechas con sede trabajada, AUS-novedad y total de asistencias' },
      { id: 'services_without_fs', title: 'Consolidado servicios (Sin FS)', subtitle: 'Rango de fechas con servicios planeados, cedulas atendidas y ausentismos confirmados' },
      { id: 'hiring_by_sede', title: 'Contratacion por Sedes', subtitle: 'Dependencia, zona, sede, planeados y contratados por sede' },
      { id: 'novelties_consolidated', title: 'Consolidado Novedades', subtitle: 'Periodo de tiempo con personas reportadas en novedades distintas de Trabajando y Compensatorio' }
    ]
  };
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
  let generatedServicesWithoutFsRows = [];
  let generatedServicesWithoutFsDays = [];
  let generatedAttendanceWithoutFsRows = [];
  let generatedAttendanceWithoutFsDays = [];
  let running = false;
  let selectedDailyDate = todayBogota();
  let selectedAbsenteeismDate = todayBogota();
  let selectedServicesWithoutFsDateFrom = `${todayBogota().slice(0, 7)}-01`;
  let selectedServicesWithoutFsDateTo = todayBogota();
  let selectedNoveltyDateFrom = `${todayBogota().slice(0, 7)}-01`;
  let selectedNoveltyDateTo = todayBogota();
  let selectedAttendanceWithoutFsDateFrom = `${todayBogota().slice(0, 7)}-01`;
  let selectedAttendanceWithoutFsDateTo = todayBogota();
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
          sede: String(sede.nombre || e.sedeNombre || e.sedeCodigo || '-').trim() || '-'
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

  const colombiaHolidayCache = new Map();

  function makeUtcDate(year, month, day) {
    return new Date(Date.UTC(year, month - 1, day));
  }

  function addUtcDays(date, days) {
    const next = new Date(date.getTime());
    next.setUTCDate(next.getUTCDate() + Number(days || 0));
    return next;
  }

  function moveToFollowingMondayUtc(date) {
    const next = new Date(date.getTime());
    const day = next.getUTCDay();
    if (day === 1) return next;
    const delta = day === 0 ? 1 : 8 - day;
    next.setUTCDate(next.getUTCDate() + delta);
    return next;
  }

  function formatUtcDate(date) {
    return date.toISOString().slice(0, 10);
  }

  function easterSundayUtc(year) {
    const a = year % 19;
    const b = Math.floor(year / 100);
    const c = year % 100;
    const d = Math.floor(b / 4);
    const e = b % 4;
    const f = Math.floor((b + 8) / 25);
    const g = Math.floor((b - f + 1) / 3);
    const h = (19 * a + b - d - g + 15) % 30;
    const i = Math.floor(c / 4);
    const k = c % 4;
    const l = (32 + 2 * e + 2 * i - h - k) % 7;
    const m = Math.floor((a + 11 * h + 22 * l) / 451);
    const month = Math.floor((h + l - 7 * m + 114) / 31);
    const day = ((h + l - 7 * m + 114) % 31) + 1;
    return makeUtcDate(year, month, day);
  }

  function getColombiaHolidaySet(year) {
    if (colombiaHolidayCache.has(year)) return colombiaHolidayCache.get(year);

    const easter = easterSundayUtc(year);
    const holidays = new Set([
      formatUtcDate(makeUtcDate(year, 1, 1)),
      formatUtcDate(makeUtcDate(year, 5, 1)),
      formatUtcDate(makeUtcDate(year, 7, 20)),
      formatUtcDate(makeUtcDate(year, 8, 7)),
      formatUtcDate(makeUtcDate(year, 12, 8)),
      formatUtcDate(makeUtcDate(year, 12, 25)),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 1, 6))),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 3, 19))),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 6, 29))),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 8, 15))),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 10, 12))),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 11, 1))),
      formatUtcDate(moveToFollowingMondayUtc(makeUtcDate(year, 11, 11))),
      formatUtcDate(addUtcDays(easter, -3)),
      formatUtcDate(addUtcDays(easter, -2)),
      formatUtcDate(addUtcDays(easter, 43)),
      formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 64))),
      formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 71))),
      formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 68)))
    ]);

    colombiaHolidayCache.set(year, holidays);
    return holidays;
  }

  function isColombiaHolidayDate(selectedDate) {
    const iso = toISODate(selectedDate);
    if (!iso) return false;
    const year = Number(iso.slice(0, 4));
    if (!Number.isFinite(year)) return false;
    return getColombiaHolidaySet(year).has(iso);
  }

  function buildDateRange(dateFrom, dateTo) {
    const start = String(dateFrom || '').trim();
    const end = String(dateTo || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end) || start > end) return null;
    const days = [];
    const cursor = new Date(`${start}T00:00:00Z`);
    const limit = new Date(`${end}T00:00:00Z`);
    while (cursor.getTime() <= limit.getTime()) {
      days.push(cursor.toISOString().slice(0, 10));
      cursor.setUTCDate(cursor.getUTCDate() + 1);
    }
    return { from: start, to: end, days };
  }

  function shiftIsoDate(iso, offsetDays) {
    const clean = String(iso || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(clean)) return '';
    const cursor = new Date(`${clean}T00:00:00Z`);
    cursor.setUTCDate(cursor.getUTCDate() + Number(offsetDays || 0));
    return cursor.toISOString().slice(0, 10);
  }

  function weekdayShortLabel(iso) {
    const labels = ['Dom', 'Lun', 'Mar', 'Mie', 'Jue', 'Vie', 'Sab'];
    const [year, month, day] = String(iso || '').split('-').map((part) => Number(part));
    const index = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
    return labels[index] || '';
  }

  function buildAttendanceWithoutFsDays(dateFrom, dateTo) {
    const range = buildDateRange(dateFrom, dateTo);
    if (!range) throw new Error('Selecciona un rango de fechas valido.');
    return range.days.map((iso, index) => {
      const weekday = weekdayShortLabel(iso);
      const isHoliday = isColombiaHolidayDate(iso);
      const isSaturday = weekday === 'Sab';
      const isSunday = weekday === 'Dom';
      const specialLabel = isHoliday ? 'FES' : isSunday ? 'DOM' : isSaturday ? 'SAB' : weekday.toUpperCase();
      return {
        iso,
        key: 'd' + String(index + 1),
        label: `${iso.slice(-2)} ${specialLabel}`,
        exportLabel: `${iso} ${specialLabel}`,
        isHoliday,
        isSaturday,
        isSunday,
        isSpecial: isHoliday || isSaturday || isSunday
      };
    });
  }

  function specialDayStyle(day = {}) {
    if (day.isHoliday) return 'background:#dbeafe;';
    if (day.isSunday) return 'background:#fee2e2;';
    if (day.isSaturday) return 'background:#fef3c7;';
    return '';
  }

  function isEmployeeActiveInRange(emp, dateFrom, dateTo) {
    const estado = String(emp?.estado || 'activo').trim().toLowerCase();
    if (estado === 'eliminado') return false;
    const ingreso = toISODate(emp?.fechaIngreso);
    const retiro = toISODate(emp?.fechaRetiro);
    if (ingreso && ingreso > dateTo) return false;
    if (retiro && retiro < dateFrom) return false;
    return true;
  }

  function buildEmployeeStatusLookupKey(day, kind, value) {
    const cleanDay = String(day || '').trim();
    const cleanKind = String(kind || '').trim();
    const cleanValue = String(value || '').trim();
    if (!cleanDay || !cleanKind || !cleanValue) return '';
    return `${cleanDay}|${cleanKind}|${cleanValue}`;
  }

  function resolveAttendanceWithoutFsAbsenceCode(row = {}) {
    const direct = String(row?.novedadCodigo || '').trim();
    if (direct) return direct;
    const raw = String(row?.novedadNombre || '').trim();
    const match = raw.match(/\d+/);
    if (match?.[0]) return match[0];
    const fallback = String(row?.estadoDia || '').trim().toUpperCase();
    return fallback || 'SN';
  }

  function shouldUseAbsenceMarker(row = {}) {
    if (!row) return false;
    if (row.asistio === true) return false;
    if (row.servicioProgramado === true) return true;
    const estado = String(row?.estadoDia || '').trim();
    return ['incapacidad', 'vacaciones', 'compensatorio', 'ausente_con_novedad', 'ausente_sin_reemplazo', 'sin_registro'].includes(estado);
  }

  function isAttendanceWithoutFsWorked(row = {}) {
    if (!row) return false;
    if (row.asistio === true) return true;
    return String(row?.tipoPersonal || '').trim() === 'supernumerario'
      && String(row?.estadoDia || '').trim() === 'trabajado_reemplazo';
  }

  function normalizeAttendanceWithoutFsRows(dateFrom, dateTo, employeeRows = [], statusRows = [], cargoRows = []) {
    const days = buildAttendanceWithoutFsDays(dateFrom, dateTo);
    const cargoByCode = new Map((cargoRows || []).map((cargo) => [String(cargo?.codigo || '').trim(), cargo || {}]).filter(([key]) => Boolean(key)));
    const statusByKey = new Map();
    const lastAttendanceBeforeRangeById = new Map();
    const lastAttendanceBeforeRangeByDoc = new Map();
    const replacementSuperKeysInRange = new Set();
    const syntheticSupernumerariosByKey = new Map();
    (statusRows || []).forEach((row) => {
      const day = String(row?.fecha || '').trim();
      const employeeId = String(row?.employeeId || '').trim();
      const document = String(row?.documento || '').trim();
      const idKey = buildEmployeeStatusLookupKey(day, 'id', employeeId);
      const docKey = buildEmployeeStatusLookupKey(day, 'doc', document);
      if (idKey && !statusByKey.has(idKey)) statusByKey.set(idKey, row);
      if (docKey && !statusByKey.has(docKey)) statusByKey.set(docKey, row);
      if (day >= dateFrom && day <= dateTo && String(row?.tipoPersonal || '').trim() === 'supernumerario' && String(row?.estadoDia || '').trim() === 'trabajado_reemplazo') {
        const employeeKey = employeeId ? `id:${employeeId}` : document ? `doc:${document}` : '';
        if (employeeId) replacementSuperKeysInRange.add(`id:${employeeId}`);
        if (document) replacementSuperKeysInRange.add(`doc:${document}`);
        if (employeeKey && !syntheticSupernumerariosByKey.has(employeeKey)) {
          syntheticSupernumerariosByKey.set(employeeKey, {
            id: employeeId || null,
            documento: document || null,
            nombre: String(row?.nombre || '-').trim() || '-',
            cargoCodigo: null,
            cargoNombre: 'Supernumerario',
            estado: 'activo'
          });
        }
      }
      if (day && day < dateFrom && isAttendanceWithoutFsWorked(row)) {
        const sedeCode = String(row?.sedeCodigo || '').trim();
        if (employeeId && sedeCode) lastAttendanceBeforeRangeById.set(employeeId, sedeCode);
        if (document && sedeCode) lastAttendanceBeforeRangeByDoc.set(document, sedeCode);
      }
    });

    const reportEmployees = [];
    const reportEmployeeKeys = new Set();
    (employeeRows || [])
      .filter((employee) => isEmployeeActiveInRange(employee, dateFrom, dateTo))
      .forEach((employee) => {
        const employeeId = String(employee?.id || '').trim();
        const document = String(employee?.documento || '').trim();
        const cargoCode = String(employee?.cargoCodigo || '').trim();
        const cargo = cargoByCode.get(cargoCode) || null;
        const isSupernumerario = normalizeCargoAlignment(cargo?.alineacionCrud || cargo?.alineacion_crud || employee?.cargoNombre || '') === 'supernumerario';
        if (isSupernumerario && !replacementSuperKeysInRange.has(`id:${employeeId}`) && !replacementSuperKeysInRange.has(`doc:${document}`)) return;
        const key = employeeId ? `id:${employeeId}` : document ? `doc:${document}` : '';
        if (key && reportEmployeeKeys.has(key)) return;
        if (key) reportEmployeeKeys.add(key);
        reportEmployees.push(employee);
      });
    syntheticSupernumerariosByKey.forEach((employee, key) => {
      if (reportEmployeeKeys.has(key)) return;
      reportEmployeeKeys.add(key);
      reportEmployees.push(employee);
    });

    const rows = reportEmployees
      .sort((a, b) => {
        const byName = String(a?.nombre || '').localeCompare(String(b?.nombre || ''));
        if (byName !== 0) return byName;
        return String(a?.documento || '').localeCompare(String(b?.documento || ''));
      })
      .map((employee) => {
        const employeeId = String(employee?.id || '').trim();
        const document = String(employee?.documento || '').trim();
        const cargoCode = String(employee?.cargoCodigo || '').trim();
        const cargo = cargoByCode.get(cargoCode) || null;
        const isSupernumerario = normalizeCargoAlignment(cargo?.alineacionCrud || cargo?.alineacion_crud || employee?.cargoNombre || '') === 'supernumerario';
        const displayName = String(employee?.nombre || '-').trim() || '-';
        const row = {
          cedula: document || '-',
          nombre: isSupernumerario ? `${displayName} SNUM` : displayName,
          isSupernumerario,
          asistencias: 0
        };
        let lastAttendanceSedeCode = lastAttendanceBeforeRangeById.get(employeeId) || lastAttendanceBeforeRangeByDoc.get(document) || '';

        days.forEach((day) => {
          const dayStatus =
            statusByKey.get(buildEmployeeStatusLookupKey(day.iso, 'id', employeeId))
            || statusByKey.get(buildEmployeeStatusLookupKey(day.iso, 'doc', document))
            || null;

          let value = '';
          if (isAttendanceWithoutFsWorked(dayStatus)) {
            value = String(dayStatus?.sedeCodigo || '').trim() || '-';
            row.asistencias += 1;
            if (value && value !== '-') lastAttendanceSedeCode = value;
          } else if (shouldUseAbsenceMarker(dayStatus)) {
            value = `AUS-${resolveAttendanceWithoutFsAbsenceCode(dayStatus)}`;
          } else if (day.isSpecial && lastAttendanceSedeCode && !row.isSupernumerario) {
            value = lastAttendanceSedeCode;
            row.asistencias += 1;
          }

          row[day.key] = value;
        });

        return row;
      });

    return { rows, days };
  }

  function buildServiceWithoutFsKey(row = {}) {
    const sedeCode = String(row?.sedeCodigo || '').trim();
    const employeeId = String(row?.employeeId || '').trim();
    const document = String(row?.documento || '').trim();
    const identity = employeeId || document;
    if (!sedeCode || !identity) return '';
    return `${sedeCode}|${identity}`;
  }

  function buildServiceWithoutFsIdentity(row = {}) {
    const employeeId = String(row?.employeeId || '').trim();
    const document = String(row?.documento || '').trim();
    return employeeId || document || '';
  }

  function formatServiceWithoutFsReplacementDocument(document) {
    const cleanDocument = String(document || '').trim();
    if (!cleanDocument) return '';
    return `${cleanDocument} SN`;
  }

  function resolveServiceWithoutFsWorkedDocument(row = {}) {
    const replacementDoc = String(row?.reemplazadoPorDocumento || '').trim();
    if (replacementDoc) return formatServiceWithoutFsReplacementDocument(replacementDoc);
    if (row?.asistio === true) return String(row?.documento || '').trim();
    return '';
  }

  function resolveServiceWithoutFsOwnDocument(row = {}) {
    return String(row?.documento || '').trim();
  }

  function resolveServiceWithoutFsNovedadCode(row = {}) {
    return String(row?.novedadCodigo || '').trim();
  }

  function isServiceWithoutFsCompensatory(row = {}) {
    return String(row?.estadoDia || '').trim().toLowerCase() === 'compensatorio';
  }

  function isConfirmedServiceWithoutFsAbsence(row = {}) {
    return String(row?.decisionCobertura || '').trim().toLowerCase() === 'ausentismo';
  }

  function isServiceWithoutFsAutoCrossNovedad8(row = {}) {
    return String(row?.tipoPersonal || '').trim() === 'empleado'
      && row?.servicioProgramado === true
      && resolveServiceWithoutFsNovedadCode(row) === '8'
      && isConfirmedServiceWithoutFsAbsence(row);
  }

  function isServiceWithoutFsUnassignedSupernumerario(row = {}) {
    if (String(row?.tipoPersonal || '').trim() !== 'supernumerario') return false;
    if (row?.asistio !== true) return false;
    if (String(row?.estadoDia || '').trim() === 'trabajado_reemplazo') return false;
    if (String(row?.sourceReplacementId || '').trim()) return false;
    if (String(row?.reemplazaAEmployeeId || row?.reemplazaADocumento || '').trim()) return false;
    return Boolean(String(row?.documento || '').trim());
  }

  function serviceWithoutFsReplacementKeys(row = {}) {
    const day = String(row?.fecha || '').trim();
    const employeeId = String(row?.employeeId || row?.empleadoId || '').trim();
    const document = String(row?.documento || '').trim();
    const keys = [];
    if (day && employeeId) keys.push(`${day}|id|${employeeId}`);
    if (day && document) keys.push(`${day}|doc|${document}`);
    return keys;
  }

  function normalizeServicesWithoutFsStatusRows(statusRows = [], replacementRows = []) {
    const replacementByKey = new Map();
    const usedReplacementIds = new Set();
    (replacementRows || []).forEach((replacement) => {
      if (String(replacement?.decision || '').trim().toLowerCase() !== 'reemplazo') return;
      if (!String(replacement?.supernumerarioDocumento || '').trim()) return;
      serviceWithoutFsReplacementKeys(replacement).forEach((key) => {
        if (key && !replacementByKey.has(key)) replacementByKey.set(key, replacement);
      });
    });

    const rows = (statusRows || []).map((row) => {
      if (String(row?.tipoPersonal || '').trim() !== 'empleado') return row;
      const replacement = serviceWithoutFsReplacementKeys(row).map((key) => replacementByKey.get(key)).find(Boolean) || null;
      if (!replacement) return row;
      if (replacement.id) usedReplacementIds.add(String(replacement.id));
      return {
        ...row,
        sedeCodigo: replacement.sedeCodigo || row.sedeCodigo || null,
        sedeNombreSnapshot: replacement.sedeNombre || row.sedeNombreSnapshot || null,
        novedadCodigo: replacement.novedadCodigo || row.novedadCodigo || null,
        novedadNombre: replacement.novedadNombre || row.novedadNombre || null,
        decisionCobertura: 'reemplazo',
        reemplazadoPorEmployeeId: replacement.supernumerarioId || row.reemplazadoPorEmployeeId || null,
        reemplazadoPorDocumento: replacement.supernumerarioDocumento || row.reemplazadoPorDocumento || null,
        reemplazadoPorNombre: replacement.supernumerarioNombre || row.reemplazadoPorNombre || null,
        servicioProgramado: true,
        servicioCubierto: true,
        cuentaPagoServicio: true,
        sourceReplacementId: replacement.id || row.sourceReplacementId || null
      };
    });

    (replacementRows || []).forEach((replacement) => {
      if (String(replacement?.decision || '').trim().toLowerCase() !== 'reemplazo') return;
      if (replacement.id && usedReplacementIds.has(String(replacement.id))) return;
      const day = String(replacement?.fecha || '').trim();
      const sedeCode = String(replacement?.sedeCodigo || '').trim();
      const document = String(replacement?.documento || '').trim();
      const employeeId = String(replacement?.employeeId || replacement?.empleadoId || '').trim();
      const superDoc = String(replacement?.supernumerarioDocumento || '').trim();
      if (!day || !sedeCode || !superDoc || (!document && !employeeId)) return;
      rows.push({
        id: replacement.id || `${day}_${employeeId || document}`,
        fecha: day,
        employeeId: employeeId || null,
        documento: document || null,
        nombre: replacement.nombre || '-',
        tipoPersonal: 'empleado',
        sedeCodigo: sedeCode,
        sedeNombreSnapshot: replacement.sedeNombre || null,
        estadoDia: 'ausente_con_novedad',
        asistio: false,
        novedadCodigo: replacement.novedadCodigo || null,
        novedadNombre: replacement.novedadNombre || null,
        decisionCobertura: 'reemplazo',
        reemplazadoPorEmployeeId: replacement.supernumerarioId || null,
        reemplazadoPorDocumento: superDoc,
        reemplazadoPorNombre: replacement.supernumerarioNombre || null,
        servicioProgramado: true,
        servicioCubierto: true,
        cuentaPagoServicio: true,
        sourceReplacementId: replacement.id || null
      });
    });

    return rows;
  }

  function hasThreePreviousServiceWithoutFsAus(values = []) {
    const recentValues = (values || [])
      .slice(-3)
      .map((value) => String(value || '').trim().toUpperCase());
    return recentValues.length === 3 && recentValues.every((value) => value === 'AUS');
  }

  function resolveSpecialServiceWithoutFsValue(previousValues = []) {
    if (hasThreePreviousServiceWithoutFsAus(previousValues)) return 'AUS';
    const prev = String(previousValues[previousValues.length - 1] || '').trim();
    return prev === 'NOCON' ? 'NOCON' : '';
  }

  function resolveSundayServiceWithoutFsCarryValue(previousValues = []) {
    if (hasThreePreviousServiceWithoutFsAus(previousValues)) return 'AUS';
    const prev = String(previousValues[previousValues.length - 1] || '').trim();
    if (isServiceWithoutFsDocumentValue(prev)) return prev;
    return resolveSpecialServiceWithoutFsValue(previousValues);
  }

  function resolveWeekendServiceWithoutFsCarryValue(previousValues = [], day = '', sedeCode = '', historyByDocument = new Map(), validationDocument = '') {
    if (hasThreePreviousServiceWithoutFsAus(previousValues)) return 'AUS';
    const prev = String(previousValues[previousValues.length - 1] || '').trim();
    const fallbackBaseDocument = findPreviousServiceWithoutFsBaseVisibleDocument(previousValues, 3);
    const documentToValidate = String(validationDocument || fallbackBaseDocument).trim();
    if (documentToValidate) {
      if (!isServiceWithoutFsDocumentAssignedToSedeOnDate(documentToValidate, day, sedeCode, historyByDocument)) return '';
      if (isServiceWithoutFsDocumentValue(prev)) return prev;
      return findRecentServiceWithoutFsOnlySupernumerarioDocument(previousValues, 3);
    }
    return findRecentServiceWithoutFsOnlySupernumerarioDocument(previousValues, 3);
  }

  function resolveSaturdayServiceWithoutFsValue(row = {}) {
    if (!row) return '';
    const ownDoc = resolveServiceWithoutFsOwnDocument(row);
    const novedadCode = resolveServiceWithoutFsNovedadCode(row);
    const replacementDoc = String(row?.reemplazadoPorDocumento || '').trim();
    if (replacementDoc) return { value: formatServiceWithoutFsReplacementDocument(replacementDoc), counts: true };
    if (row?.asistio === true || isServiceWithoutFsCompensatory(row) || novedadCode === '8') {
      return { value: ownDoc, counts: Boolean(ownDoc) };
    }
    if (['2', '3', '4', '5', '6', '9'].includes(novedadCode)) {
      return { value: 'AUS', counts: false };
    }
    if (ownDoc) return { value: ownDoc, counts: true };
    return { value: 'NOCON', counts: false };
  }

  function resolveSundayHolidayServiceWithoutFsValue(row = {}) {
    if (!row) return '';
    const novedadCode = resolveServiceWithoutFsNovedadCode(row);
    const replacementDoc = String(row?.reemplazadoPorDocumento || '').trim();
    if (replacementDoc) return { value: formatServiceWithoutFsReplacementDocument(replacementDoc), counts: true };
    const ownDoc = resolveServiceWithoutFsOwnDocument(row);
    if (row?.asistio === true || isServiceWithoutFsCompensatory(row) || novedadCode === '8') {
      return { value: ownDoc, counts: Boolean(ownDoc) };
    }
    if (['2', '3', '4', '5', '6', '9'].includes(novedadCode) || isConfirmedServiceWithoutFsAbsence(row)) {
      return { value: 'AUS', counts: false };
    }
    if (ownDoc) return { value: ownDoc, counts: true };
    return { value: 'NOCON', counts: false };
  }

  function isServiceWithoutFsDocumentValue(value) {
    const normalized = String(value || '').trim().toUpperCase();
    return Boolean(normalized && normalized !== 'AUS' && normalized !== 'NOCON');
  }

  function isServiceWithoutFsSupernumerarioDocumentValue(value) {
    return /\bSN$/i.test(String(value || '').trim());
  }

  function normalizeServiceWithoutFsDocumentIdentity(value) {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const digits = raw.replace(/\D+/g, '');
    return digits || raw.toUpperCase();
  }

  function findLastServiceWithoutFsDocumentValue(values = []) {
    for (let index = (values || []).length - 1; index >= 0; index -= 1) {
      const candidate = String(values[index] || '').trim();
      if (isServiceWithoutFsDocumentValue(candidate)) return candidate;
    }
    return '';
  }

  function findPreviousServiceWithoutFsBaseVisibleDocument(values = [], maxLookback = Infinity) {
    const start = (values || []).length - 1;
    const end = Number.isFinite(maxLookback) ? Math.max(0, start - maxLookback + 1) : 0;
    for (let index = start; index >= end; index -= 1) {
      const candidate = String(values[index] || '').trim();
      if (!isServiceWithoutFsDocumentValue(candidate)) continue;
      if (isServiceWithoutFsSupernumerarioDocumentValue(candidate)) continue;
      return candidate;
    }
    return '';
  }

  function findRecentServiceWithoutFsOnlySupernumerarioDocument(values = [], maxLookback = 3) {
    const start = (values || []).length - 1;
    const end = Math.max(0, start - maxLookback + 1);
    let latestSupernumerarioDocument = '';
    for (let index = start; index >= end; index -= 1) {
      const candidate = String(values[index] || '').trim();
      if (!isServiceWithoutFsDocumentValue(candidate)) continue;
      if (!isServiceWithoutFsSupernumerarioDocumentValue(candidate)) return '';
      if (!latestSupernumerarioDocument) latestSupernumerarioDocument = candidate;
    }
    return latestSupernumerarioDocument;
  }

  function resolveServiceWithoutFsBaseDocumentForDay(documentsByDay = [], dayIndex = -1) {
    if (!Array.isArray(documentsByDay) || dayIndex < 0 || dayIndex >= documentsByDay.length) return '';
    const currentDoc = String(documentsByDay[dayIndex] || '').trim();
    if (currentDoc) return currentDoc;

    let previousDoc = '';
    for (let index = dayIndex - 1; index >= 0; index -= 1) {
      const candidate = String(documentsByDay[index] || '').trim();
      if (!candidate) continue;
      previousDoc = candidate;
      break;
    }

    let nextDoc = '';
    for (let index = dayIndex + 1; index < documentsByDay.length; index += 1) {
      const candidate = String(documentsByDay[index] || '').trim();
      if (!candidate) continue;
      nextDoc = candidate;
      break;
    }

    if (previousDoc && nextDoc) {
      const previousIdentity = normalizeServiceWithoutFsDocumentIdentity(previousDoc);
      const nextIdentity = normalizeServiceWithoutFsDocumentIdentity(nextDoc);
      if (previousIdentity && nextIdentity && previousIdentity === nextIdentity) return previousDoc;
    }
    return '';
  }

  function findPreviousServiceWithoutFsBaseDocument(documentsByDay = [], dayIndex = -1) {
    if (!Array.isArray(documentsByDay) || dayIndex < 0) return '';
    for (let index = Math.min(dayIndex - 1, documentsByDay.length - 1); index >= 0; index -= 1) {
      const candidate = String(documentsByDay[index] || '').trim();
      if (candidate) return candidate;
    }
    return '';
  }

  function resolveServiceWithoutFsBaseDocumentForHistoryDate(documentsByDay = [], dayIndex = -1, day = '', sedeCode = '', historyByDocument = new Map()) {
    if (!Array.isArray(documentsByDay) || dayIndex < 0) return '';
    const currentDoc = String(documentsByDay[dayIndex] || '').trim();
    const candidate = currentDoc || findPreviousServiceWithoutFsBaseDocument(documentsByDay, dayIndex);
    if (!candidate) return '';
    return isServiceWithoutFsDocumentAssignedToSedeOnDate(candidate, day, sedeCode, historyByDocument) ? candidate : '';
  }

  function isServiceWithoutFsDocumentAssignedToSedeOnDate(documentValue = '', day = '', sedeCode = '', historyByDocument = new Map()) {
    const identity = normalizeServiceWithoutFsDocumentIdentity(documentValue);
    const cleanDay = String(day || '').trim();
    const cleanSedeCode = String(sedeCode || '').trim();
    if (!identity || !cleanDay || !cleanSedeCode) return false;
    const historyRows = historyByDocument.get(identity) || [];
    return historyRows.some((row) => {
      const ingreso = toISODate(row?.fechaIngreso || row?.fecha_ingreso);
      if (!ingreso || ingreso > cleanDay) return false;
      const retiro = toISODate(row?.fechaRetiro || row?.fecha_retiro);
      if (retiro && retiro < cleanDay) return false;
      return String(row?.sedeCodigo || row?.sede_codigo || '').trim() === cleanSedeCode;
    });
  }

  function normalizeServicesWithoutFsRows(dateFrom, dateTo, statusRows = [], sedeRows = [], historyRows = [], replacementRows = []) {
    const days = buildAttendanceWithoutFsDays(dateFrom, dateTo);
    const visibleDaySet = new Set(days.map((day) => day.iso));
    const effectiveStatusRows = normalizeServicesWithoutFsStatusRows(statusRows, replacementRows);
    const historyByDocument = new Map();
    (historyRows || []).forEach((row) => {
      const document = normalizeServiceWithoutFsDocumentIdentity(row?.documento || row?.document);
      if (!document) return;
      if (!historyByDocument.has(document)) historyByDocument.set(document, []);
      historyByDocument.get(document).push(row);
    });
    const contextStart = (effectiveStatusRows || []).reduce((min, row) => {
      const day = String(row?.fecha || '').trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return min;
      if (!min || day < min) return day;
      return min;
    }, dateFrom);
    const contextDays = buildAttendanceWithoutFsDays(contextStart || dateFrom, dateTo);
    const contextDayIndexByIso = new Map(contextDays.map((day, index) => [day.iso, index]));
    const siteKeyDelimiter = '||';
    const buildSiteKey = (sedeCode, dependencyCode = '', zoneCode = '') => [
      String(sedeCode || '').trim(),
      String(dependencyCode || '').trim(),
      String(zoneCode || '').trim()
    ].join(siteKeyDelimiter);
    const bucketKey = (day, siteKey) => `${day}|${siteKey}`;
    const parseBucketKey = (key) => {
      const separatorIndex = String(key || '').indexOf('|');
      if (separatorIndex < 0) return { day: '', siteKey: '' };
      return {
        day: String(key).slice(0, separatorIndex),
        siteKey: String(key).slice(separatorIndex + 1)
      };
    };
    const sedeByCode = new Map(
      (sedeRows || [])
        .filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
        .map((row) => [String(row?.codigo || '').trim(), row])
        .filter(([key]) => Boolean(key))
    );
    const plannedBySede = new Map();
    const sedeCodeBySiteKey = new Map();
    const scheduledByDaySede = new Map();
    const baseRowsByDaySede = new Map();
    const baseDocumentsBySedeSlot = new Map();
    const historicalBeforeRangeBySedeSlot = new Map();
    const visibleSnapshotBySede = new Map();
    const contextSnapshotBySede = new Map();
    const unassignedSupernumerariosByDaySede = new Map();
    const daySedeKey = (day, sedeCode) => `${String(day || '').trim()}|${String(sedeCode || '').trim()}`;
    const consumeUnassignedSupernumerarioDocument = (day, sedeCode) => {
      const key = daySedeKey(day, sedeCode);
      const bucket = unassignedSupernumerariosByDaySede.get(key) || [];
      while (bucket.length > 0) {
        const next = bucket.shift();
        const doc = String(next?.documento || '').trim();
        if (doc) return doc;
      }
      return '';
    };
    const replaceServiceWithoutFsGapWithSupernumerario = (value, day, sedeCode) => {
      const normalized = String(value || '').trim().toUpperCase();
      if (normalized !== 'AUS' && normalized !== 'NOCON') return { value, counts: false };
      const crossDoc = consumeUnassignedSupernumerarioDocument(day, sedeCode);
      if (!crossDoc) return { value, counts: false };
      return { value: formatServiceWithoutFsReplacementDocument(crossDoc), counts: true };
    };

    (effectiveStatusRows || []).forEach((row) => {
      const day = String(row?.fecha || '').trim();
      const sedeCode = String(row?.sedeCodigo || '').trim();
      if (!day || !sedeCode || !visibleDaySet.has(day)) return;
      if (!isServiceWithoutFsUnassignedSupernumerario(row)) return;
      const documento = String(row?.documento || '').trim();
      const employeeId = String(row?.employeeId || '').trim();
      const identity = employeeId || documento;
      if (!identity || !documento) return;
      const key = daySedeKey(day, sedeCode);
      if (!unassignedSupernumerariosByDaySede.has(key)) unassignedSupernumerariosByDaySede.set(key, []);
      const bucket = unassignedSupernumerariosByDaySede.get(key);
      if (bucket.some((item) => String(item?.identity || '').trim() === identity)) return;
      bucket.push({
        identity,
        documento,
        nombre: String(row?.nombre || '').trim()
      });
    });

    unassignedSupernumerariosByDaySede.forEach((bucket) => {
      bucket.sort((a, b) => {
        const byName = String(a?.nombre || '').localeCompare(String(b?.nombre || ''));
        if (byName !== 0) return byName;
        return String(a?.documento || '').localeCompare(String(b?.documento || ''));
      });
    });

    (effectiveStatusRows || [])
      .filter((row) => String(row?.tipoPersonal || '').trim() === 'empleado')
      .forEach((row) => {
        const sedeCode = String(row?.sedeCodigo || '').trim();
        const day = String(row?.fecha || '').trim();
        if (!sedeCode || !day) return;
        const sedeFallback = sedeByCode.get(sedeCode) || {};
        const snapshot = {
          sedeCodigo: sedeCode,
          dependenciaNombre: String(row?.dependenciaNombreSnapshot || sedeFallback?.dependenciaNombre || '').trim(),
          dependenciaCodigo: String(row?.dependenciaCodigoSnapshot || sedeFallback?.dependenciaCodigo || '').trim(),
          zonaNombre: String(row?.zonaNombreSnapshot || sedeFallback?.zonaNombre || '').trim(),
          zonaCodigo: String(row?.zonaCodigoSnapshot || sedeFallback?.zonaCodigo || '').trim(),
          nombre: String(row?.sedeNombreSnapshot || sedeFallback?.nombre || '').trim()
        };
        const siteKey = buildSiteKey(sedeCode, snapshot.dependenciaCodigo || snapshot.dependenciaNombre, snapshot.zonaCodigo || snapshot.zonaNombre);
        sedeCodeBySiteKey.set(siteKey, sedeCode);
        if (!contextSnapshotBySede.has(siteKey)) contextSnapshotBySede.set(siteKey, snapshot);
        if (visibleDaySet.has(day) && !visibleSnapshotBySede.has(siteKey)) visibleSnapshotBySede.set(siteKey, snapshot);
        const baseBucketKey = bucketKey(day, siteKey);
        if (!baseRowsByDaySede.has(baseBucketKey)) baseRowsByDaySede.set(baseBucketKey, []);
        baseRowsByDaySede.get(baseBucketKey).push(row);
        if (row?.servicioProgramado !== true) return;
        const scheduledBucketKey = bucketKey(day, siteKey);
        if (!scheduledByDaySede.has(scheduledBucketKey)) scheduledByDaySede.set(scheduledBucketKey, []);
        scheduledByDaySede.get(scheduledBucketKey).push(row);
      });

    Array.from(sedeByCode.entries()).forEach(([sedeCode, sede]) => {
      const siteKey = buildSiteKey(sedeCode, sede?.dependenciaCodigo || sede?.dependenciaNombre, sede?.zonaCodigo || sede?.zonaNombre);
      sedeCodeBySiteKey.set(siteKey, sedeCode);
      plannedBySede.set(siteKey, parseOperatorCount(sede?.numeroOperarios));
    });

    scheduledByDaySede.forEach((rows, key) => {
      rows.sort((a, b) => {
        const byName = String(a?.nombre || '').localeCompare(String(b?.nombre || ''));
        if (byName !== 0) return byName;
        return String(a?.documento || '').localeCompare(String(b?.documento || ''));
      });
      const { day, siteKey } = parseBucketKey(key);
      if (!visibleDaySet.has(day)) return;
      const currentPlanned = Number(plannedBySede.get(siteKey) || 0);
      if (rows.length > currentPlanned) plannedBySede.set(siteKey, rows.length);
    });

    const assignedByDaySede = new Map();
    const baseAssignedByDaySede = new Map();
    Array.from(plannedBySede.keys()).forEach((siteKey) => {
      const slotCount = Math.max(0, Number(plannedBySede.get(siteKey) || 0));
      const sedeDays = Array.from(scheduledByDaySede.keys())
        .map((key) => parseBucketKey(key))
        .filter((parts) => parts.siteKey === siteKey)
        .map((parts) => parts.day)
        .sort();
      const baseSedeDays = Array.from(baseRowsByDaySede.keys())
        .map((key) => parseBucketKey(key))
        .filter((parts) => parts.siteKey === siteKey)
        .map((parts) => parts.day)
        .sort();
      const slotIdentities = Array.from({ length: slotCount }, () => '');
      const baseSlotIdentities = Array.from({ length: slotCount }, () => '');

      sedeDays.forEach((day) => {
        const currentBucketKey = bucketKey(day, siteKey);
        const sourceRows = [...(scheduledByDaySede.get(currentBucketKey) || [])];
        const assignedRows = Array.from({ length: slotCount }, () => null);

        slotIdentities.forEach((identity, slotIndex) => {
          if (!identity) return;
          const matchIndex = sourceRows.findIndex((row) => buildServiceWithoutFsIdentity(row) === identity);
          if (matchIndex < 0) return;
          assignedRows[slotIndex] = sourceRows.splice(matchIndex, 1)[0];
        });

        sourceRows.forEach((row) => {
          const emptyIndex = assignedRows.findIndex((candidate) => candidate == null);
          if (emptyIndex < 0) return;
          assignedRows[emptyIndex] = row;
        });

        assignedRows.forEach((row, slotIndex) => {
          const identity = buildServiceWithoutFsIdentity(row);
          if (identity) slotIdentities[slotIndex] = identity;
        });

        assignedByDaySede.set(currentBucketKey, assignedRows);
      });

      baseSedeDays.forEach((day) => {
        const currentBucketKey = bucketKey(day, siteKey);
        const sourceRows = [...(baseRowsByDaySede.get(currentBucketKey) || [])];
        sourceRows.sort((a, b) => {
          const byName = String(a?.nombre || '').localeCompare(String(b?.nombre || ''));
          if (byName !== 0) return byName;
          return String(a?.documento || '').localeCompare(String(b?.documento || ''));
        });
        const assignedRows = Array.from({ length: slotCount }, () => null);
        const scheduledRows = assignedByDaySede.get(currentBucketKey) || [];

        scheduledRows.forEach((row, slotIndex) => {
          if (slotIndex < 0 || slotIndex >= slotCount || !row) return;
          const identity = buildServiceWithoutFsIdentity(row);
          if (!identity) return;
          const matchIndex = sourceRows.findIndex((candidate) => buildServiceWithoutFsIdentity(candidate) === identity);
          if (matchIndex < 0) return;
          assignedRows[slotIndex] = sourceRows.splice(matchIndex, 1)[0];
        });

        baseSlotIdentities.forEach((identity, slotIndex) => {
          if (!identity) return;
          if (assignedRows[slotIndex]) return;
          const matchIndex = sourceRows.findIndex((row) => buildServiceWithoutFsIdentity(row) === identity);
          if (matchIndex < 0) return;
          assignedRows[slotIndex] = sourceRows.splice(matchIndex, 1)[0];
        });

        sourceRows.forEach((row) => {
          const emptyIndex = assignedRows.findIndex((candidate) => candidate == null);
          if (emptyIndex < 0) return;
          assignedRows[emptyIndex] = row;
        });

        assignedRows.forEach((row, slotIndex) => {
          const identity = buildServiceWithoutFsIdentity(row);
          if (identity) baseSlotIdentities[slotIndex] = identity;
        });

        baseAssignedByDaySede.set(currentBucketKey, assignedRows);
      });

      Array.from({ length: slotCount }, (_, slotIndex) => {
        baseDocumentsBySedeSlot.set(
          `${siteKey}#${slotIndex}`,
          contextDays.map((day) => {
            const baseRows = baseAssignedByDaySede.get(bucketKey(day.iso, siteKey)) || [];
            const baseRow = baseRows[slotIndex] || null;
            return resolveServiceWithoutFsOwnDocument(baseRow);
          })
        );
      });
    });

    Array.from(plannedBySede.keys()).forEach((siteKey) => {
      const slotCount = Math.max(0, Number(plannedBySede.get(siteKey) || 0));
      const sedeCode = sedeCodeBySiteKey.get(siteKey) || String(siteKey).split(siteKeyDelimiter)[0] || '';
      const sede = sedeByCode.get(sedeCode) || {};
      const isWeekdayOnlySede = String(sede?.jornada || 'lun_vie').trim().toLowerCase() === 'lun_vie';
      Array.from({ length: slotCount }, (_, slotIndex) => {
        const slotKey = `${siteKey}#${slotIndex}`;
        const history = [];
        const baseDocumentsByDay = baseDocumentsBySedeSlot.get(slotKey) || [];

        contextDays.forEach((day) => {
          if (day.iso >= dateFrom) return;
          const scheduled = assignedByDaySede.get(bucketKey(day.iso, siteKey)) || [];
          const current = scheduled[slotIndex] || null;
          const contextDayIndex = Number(contextDayIndexByIso.get(day.iso));
          const validBaseDocForDay = resolveServiceWithoutFsBaseDocumentForDay(baseDocumentsByDay, contextDayIndex);
          const activeBaseDocForDay = resolveServiceWithoutFsBaseDocumentForHistoryDate(baseDocumentsByDay, contextDayIndex, day.iso, sedeCode, historyByDocument);
          let value = '';

          if (day.isSpecial) {
            if (current) {
              const resolved = day.isSaturday
                ? resolveSaturdayServiceWithoutFsValue(current)
                : resolveSundayHolidayServiceWithoutFsValue(current);
              value = String(resolved?.value || '').trim();
            } else {
              const weekendCarryValue = isWeekdayOnlySede && day.isSpecial
                ? resolveWeekendServiceWithoutFsCarryValue(history, day.iso, sedeCode, historyByDocument, activeBaseDocForDay)
                : '';
              const ausCarryValue = hasThreePreviousServiceWithoutFsAus(history) ? 'AUS' : '';
              const baseDocForSpecialDay = activeBaseDocForDay;
              const specialFallbackValue = isWeekdayOnlySede
                ? resolveSpecialServiceWithoutFsValue(history)
                : (day.isSunday
                  ? resolveSundayServiceWithoutFsCarryValue(history)
                  : resolveSpecialServiceWithoutFsValue(history));
              value = ausCarryValue || weekendCarryValue || baseDocForSpecialDay || specialFallbackValue || 'NOCON';
            }
          } else if (!current) {
            value = 'NOCON';
          } else {
            const workedDoc = resolveServiceWithoutFsWorkedDocument(current);
            if (workedDoc) value = workedDoc;
            else if (isConfirmedServiceWithoutFsAbsence(current)) value = 'AUS';
            else value = 'NOCON';
          }

          history.push(value);
        });

        historicalBeforeRangeBySedeSlot.set(slotKey, history);
      });
    });

    const rows = Array.from(plannedBySede.entries())
      .sort((a, b) => {
        const left = visibleSnapshotBySede.get(a[0]) || contextSnapshotBySede.get(a[0]) || sedeByCode.get(a[0]) || {};
        const right = visibleSnapshotBySede.get(b[0]) || contextSnapshotBySede.get(b[0]) || sedeByCode.get(b[0]) || {};
        const byDependencyCode = String(left?.dependenciaCodigo || '').localeCompare(String(right?.dependenciaCodigo || ''));
        if (byDependencyCode !== 0) return byDependencyCode;
        const byDependency = String(left?.dependenciaNombre || '').localeCompare(String(right?.dependenciaNombre || ''));
        if (byDependency !== 0) return byDependency;
        const byZoneCode = String(left?.zonaCodigo || '').localeCompare(String(right?.zonaCodigo || ''));
        if (byZoneCode !== 0) return byZoneCode;
        const byZone = String(left?.zonaNombre || '').localeCompare(String(right?.zonaNombre || ''));
        if (byZone !== 0) return byZone;
        const byName = String(left?.nombre || '').localeCompare(String(right?.nombre || ''));
        if (byName !== 0) return byName;
        return String(sedeCodeBySiteKey.get(a[0]) || '').localeCompare(String(sedeCodeBySiteKey.get(b[0]) || ''));
      })
      .flatMap(([siteKey, plannedCount]) => {
        const sedeCode = sedeCodeBySiteKey.get(siteKey) || String(siteKey).split(siteKeyDelimiter)[0] || '';
        const sede = {
          ...(sedeByCode.get(sedeCode) || {}),
          ...(contextSnapshotBySede.get(siteKey) || {}),
          ...(visibleSnapshotBySede.get(siteKey) || {})
        };
        const basePlannedCount = Math.max(0, parseOperatorCount(sede?.numeroOperarios));
        const isWeekdayOnlySede = String(sede?.jornada || 'lun_vie').trim().toLowerCase() === 'lun_vie';
        const count = Math.max(0, Number(plannedCount || 0));
        return Array.from({ length: count }, (_, slotIndex) => {
          const serviceNumber = slotIndex + 1;
          const isAdditionalService = serviceNumber > basePlannedCount;
          let serviceName = `Servicio ${serviceNumber}${isAdditionalService ? ' AD' : ''}`;
          const slotKey = `${siteKey}#${slotIndex}`;
          const historicalValues = [...(historicalBeforeRangeBySedeSlot.get(slotKey) || [])];
          const baseDocumentsByDay = baseDocumentsBySedeSlot.get(slotKey) || [];
          let dynamicServiceDoc = findLastServiceWithoutFsDocumentValue(historicalValues);
          const serviceDoc = dynamicServiceDoc || 'NOCON';

          const row = {
            dependencia: String(sede?.dependenciaNombre || sede?.dependenciaCodigo || '-').trim() || '-',
            zona: String(sede?.zonaNombre || sede?.zonaCodigo || '-').trim() || '-',
            sede: String(sede?.nombre || sedeCode || '-').trim() || '-',
            servicio: serviceName,
            servicioDocumento: serviceDoc,
            asistencias: 0
          };
          row.__serviceWithoutFsDayPriorities = {};
          row.__serviceWithoutFsDayCounts = {};
          const previousValues = [...historicalValues];

          days.forEach((day, dayIndex) => {
            const scheduled = assignedByDaySede.get(bucketKey(day.iso, siteKey)) || [];
            const current = scheduled[slotIndex] || null;
            const contextDayIndex = Number(contextDayIndexByIso.get(day.iso));
            const validBaseDocForDay = resolveServiceWithoutFsBaseDocumentForDay(baseDocumentsByDay, contextDayIndex);
            const activeBaseDocForDay = resolveServiceWithoutFsBaseDocumentForHistoryDate(baseDocumentsByDay, contextDayIndex, day.iso, sedeCode, historyByDocument);
            let value = '';
            let valuePriority = 0;
            let valueCounts = false;

            if (day.isSpecial) {
              if (current) {
                const resolved = day.isSaturday
                  ? resolveSaturdayServiceWithoutFsValue(current)
                  : resolveSundayHolidayServiceWithoutFsValue(current);
                value = String(resolved?.value || '').trim();
                if (resolved?.counts) {
                  row.asistencias += 1;
                  valueCounts = true;
                  valuePriority = String(current?.reemplazadoPorDocumento || '').trim() ? 2 : 1;
                }
              } else {
                const weekendCarryValue = isWeekdayOnlySede && day.isSpecial
                  ? resolveWeekendServiceWithoutFsCarryValue(previousValues, day.iso, sedeCode, historyByDocument, activeBaseDocForDay)
                  : '';
                const ausCarryValue = hasThreePreviousServiceWithoutFsAus(previousValues) ? 'AUS' : '';
                const baseDocForSpecialDay = activeBaseDocForDay;
                const specialFallbackValue = isWeekdayOnlySede
                  ? resolveSpecialServiceWithoutFsValue(previousValues)
                  : (day.isSunday
                    ? resolveSundayServiceWithoutFsCarryValue(previousValues)
                    : resolveSpecialServiceWithoutFsValue(previousValues));
                value = ausCarryValue || weekendCarryValue || baseDocForSpecialDay || specialFallbackValue || 'NOCON';
                if (value && value !== 'AUS' && value !== 'NOCON') {
                  row.asistencias += 1;
                  valueCounts = true;
                }
              }
            } else if (!current) {
              value = 'NOCON';
            } else {
              const workedDoc = resolveServiceWithoutFsWorkedDocument(current);
              if (workedDoc) {
                value = workedDoc;
                row.asistencias += 1;
                valueCounts = true;
                valuePriority = String(current?.reemplazadoPorDocumento || '').trim() ? 2 : 1;
              } else if (isConfirmedServiceWithoutFsAbsence(current)) {
                const crossDoc = isServiceWithoutFsAutoCrossNovedad8(current)
                  ? consumeUnassignedSupernumerarioDocument(day.iso, sedeCode)
                  : '';
                if (crossDoc) {
                  value = formatServiceWithoutFsReplacementDocument(crossDoc);
                  row.asistencias += 1;
                  valueCounts = true;
                  valuePriority = 1;
                } else {
                  value = 'AUS';
                }
              } else {
                value = 'NOCON';
              }
            }

            if (!current || isServiceWithoutFsAutoCrossNovedad8(current)) {
              const crossGap = replaceServiceWithoutFsGapWithSupernumerario(value, day.iso, sedeCode);
              value = crossGap.value;
              if (crossGap.counts) {
                row.asistencias += 1;
                valueCounts = true;
                valuePriority = Math.max(valuePriority, 1);
              }
            }
            row[day.key] = value;
            row.__serviceWithoutFsDayPriorities[day.key] = valuePriority;
            row.__serviceWithoutFsDayCounts[day.key] = valueCounts;
            if (isServiceWithoutFsDocumentValue(value)) dynamicServiceDoc = value;
            previousValues.push(value);
          });

          return row;
        });
      });

    days.forEach((day) => {
      const usedDocuments = new Map();
      rows.forEach((row) => {
        const value = String(row?.[day.key] || '').trim();
        if (!isServiceWithoutFsDocumentValue(value)) return;
        const identity = normalizeServiceWithoutFsDocumentIdentity(value);
        if (!identity) return;
        const currentPriority = Number(row?.__serviceWithoutFsDayPriorities?.[day.key] || 0);
        const previousUse = usedDocuments.get(identity);
        if (previousUse) {
          const previousPriority = Number(previousUse.row?.__serviceWithoutFsDayPriorities?.[day.key] || 0);
          if (currentPriority > previousPriority) {
            previousUse.row[day.key] = 'NOCON';
            if (previousUse.row?.__serviceWithoutFsDayCounts?.[day.key]) {
              previousUse.row.asistencias = Math.max(0, Number(previousUse.row.asistencias || 0) - 1);
              previousUse.row.__serviceWithoutFsDayCounts[day.key] = false;
            }
            usedDocuments.set(identity, { row });
            return;
          }
          row[day.key] = 'NOCON';
          if (row?.__serviceWithoutFsDayCounts?.[day.key]) {
            row.asistencias = Math.max(0, Number(row.asistencias || 0) - 1);
            row.__serviceWithoutFsDayCounts[day.key] = false;
          }
          return;
        }
        usedDocuments.set(identity, { row });
      });
    });

    return { rows, days };
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

  function renderAttendanceWithoutFsRows(rows = [], days = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 3 + days.length, className: 'text-muted' }, ['Sin empleados activos para el rango seleccionado.'])])];
    return rows.map((r) => el('tr', {}, [
      el('td', { style: 'white-space:nowrap;' }, [r.cedula]),
      el('td', { style: `white-space:nowrap;${r.isSupernumerario ? 'color:#1d4ed8;font-weight:700;background:#eff6ff;' : ''}`, title: r.isSupernumerario ? 'Supernumerario' : '' }, [r.nombre]),
      ...days.map((day) => el('td', { style: `white-space:nowrap;${specialDayStyle(day)}` }, [r[day.key] || ''])),
      el('td', { style: 'white-space:nowrap;text-align:right;' }, [String(r.asistencias || 0)])
    ]));
  }

  function renderServicesWithoutFsRows(rows = [], days = []) {
    if (!rows.length) return [el('tr', {}, [el('td', { colSpan: 5 + days.length, className: 'text-muted' }, ['Sin servicios planeados para el rango seleccionado.'])])];
    return rows.map((r) => el('tr', {}, [
      el('td', { style: 'white-space:nowrap;' }, [r.dependencia]),
      el('td', { style: 'white-space:nowrap;' }, [r.zona]),
      el('td', { style: 'white-space:nowrap;' }, [r.sede]),
      el('td', { style: 'white-space:nowrap;' }, [r.servicio]),
      ...days.map((day) => el('td', { style: `white-space:nowrap;text-align:center;${specialDayStyle(day)}` }, [r[day.key] || ''])),
      el('td', { style: 'white-space:nowrap;text-align:right;' }, [String(r.asistencias || 0)])
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

  async function generateAttendanceWithoutFsReport() {
    if (running) return;
    const dateFrom = String(qs('#attendanceWithoutFsDateFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#attendanceWithoutFsDateTo', ui)?.value || '').trim();
    const range = buildDateRange(dateFrom, dateTo);
    if (!range) {
      setMessage('Selecciona un rango valido para generar el consolidado de asistencia.');
      return;
    }
    running = true;
    selectedAttendanceWithoutFsDateFrom = dateFrom;
    selectedAttendanceWithoutFsDateTo = dateTo;
    const btnGenerate = qs('#btnGenerateAttendanceWithoutFs', ui);
    const btnExport = qs('#btnExportAttendanceWithoutFs', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const contextFrom = shiftIsoDate(dateFrom, -31) || dateFrom;
      const [rawEmployees, statusRows, rawCargos] = await Promise.all([
        streamOnce((ok, fail) => deps.streamEmployees?.(ok, fail)),
        deps.listEmployeeDailyStatusRange?.(contextFrom, dateTo) || [],
        streamOnce((ok, fail) => deps.streamCargos?.(ok, fail))
      ]);
      const normalized = normalizeAttendanceWithoutFsRows(dateFrom, dateTo, rawEmployees, statusRows, rawCargos);
      generatedAttendanceWithoutFsRows = normalized.rows;
      generatedAttendanceWithoutFsDays = normalized.days;
      const headRow = qs('#attendanceWithoutFsHeadRow', ui);
      if (headRow) {
        headRow.replaceChildren(
          el('th', {}, ['Cedula']),
          el('th', {}, ['Nombre']),
          ...normalized.days.map((day) => el('th', { style: specialDayStyle(day) }, [day.label])),
          el('th', {}, ['Asistencias'])
        );
      }
      const tbody = qs('#attendanceWithoutFsTbody', ui);
      if (tbody) tbody.replaceChildren(...renderAttendanceWithoutFsRows(normalized.rows, normalized.days));
      const totalAttendance = normalized.rows.reduce((acc, row) => acc + Number(row.asistencias || 0), 0);
      const totalNode = qs('#attendanceWithoutFsTotal', ui);
      if (totalNode) totalNode.textContent = `Periodo: ${dateFrom} a ${dateTo} | Empleados: ${normalized.rows.length} | Dias: ${normalized.days.length} | Asistencias: ${totalAttendance}`;
      if (btnExport) btnExport.disabled = normalized.rows.length === 0;
      setMessage(`Consolidado generado para ${dateFrom} a ${dateTo}. Empleados: ${normalized.rows.length}`);
    } catch (e) {
      setMessage(`Error al generar consolidado de asistencia: ${e?.message || e}`);
    } finally {
      running = false;
      if (btnGenerate) {
        btnGenerate.disabled = false;
        btnGenerate.textContent = 'Generar reporte';
      }
    }
  }

  async function generateServicesWithoutFsReport() {
    if (running) return;
    const dateFrom = String(qs('#servicesWithoutFsDateFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#servicesWithoutFsDateTo', ui)?.value || '').trim();
    const range = buildDateRange(dateFrom, dateTo);
    if (!range) {
      setMessage('Selecciona un rango valido para generar el consolidado de servicios.');
      return;
    }
    running = true;
    selectedServicesWithoutFsDateFrom = dateFrom;
    selectedServicesWithoutFsDateTo = dateTo;
    const btnGenerate = qs('#btnGenerateServicesWithoutFs', ui);
    const btnExport = qs('#btnExportServicesWithoutFs', ui);
    try {
      if (btnGenerate) {
        btnGenerate.disabled = true;
        btnGenerate.textContent = 'Generando...';
      }
      const contextFrom = shiftIsoDate(dateFrom, -31) || dateFrom;
      const [statusRows, rawSedes, rawHistory, replacementRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(contextFrom, dateTo) || [],
        streamOnce((ok, fail) => deps.streamSedes?.(ok, fail)),
        deps.streamEmployeeCargoHistoryAll ? streamOnce((ok) => deps.streamEmployeeCargoHistoryAll?.(ok, 50000)) : [],
        deps.listImportReplacementsRange?.(contextFrom, dateTo) || []
      ]);
      const normalized = normalizeServicesWithoutFsRows(dateFrom, dateTo, statusRows, rawSedes, rawHistory, replacementRows);
      generatedServicesWithoutFsRows = normalized.rows;
      generatedServicesWithoutFsDays = normalized.days;
      const headRow = qs('#servicesWithoutFsHeadRow', ui);
      if (headRow) {
        headRow.replaceChildren(
          el('th', {}, ['Dependencia']),
          el('th', {}, ['Zona']),
          el('th', {}, ['Sede']),
          el('th', {}, ['Servicio planeado']),
          ...normalized.days.map((day) => el('th', { style: specialDayStyle(day) }, [day.label])),
          el('th', {}, ['Asistencias'])
        );
      }
      const tbody = qs('#servicesWithoutFsTbody', ui);
      if (tbody) tbody.replaceChildren(...renderServicesWithoutFsRows(normalized.rows, normalized.days));
      const totalAttendance = normalized.rows.reduce((acc, row) => acc + Number(row.asistencias || 0), 0);
      const totalNode = qs('#servicesWithoutFsTotal', ui);
      if (totalNode) totalNode.textContent = `Periodo: ${dateFrom} a ${dateTo} | Servicios: ${normalized.rows.length} | Dias: ${normalized.days.length} | Asistencias: ${totalAttendance}`;
      if (btnExport) btnExport.disabled = normalized.rows.length === 0;
      setMessage(`Consolidado de servicios generado para ${dateFrom} a ${dateTo}. Servicios: ${normalized.rows.length}`);
    } catch (e) {
      setMessage(`Error al generar consolidado de servicios: ${e?.message || e}`);
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

  async function exportAttendanceWithoutFsExcel() {
    try {
      if (!generatedAttendanceWithoutFsRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportAttendanceWithoutFs', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const exportRows = generatedAttendanceWithoutFsRows.map((row) => {
        const item = {
          Cedula: row.cedula,
          Nombre: row.nombre
        };
        generatedAttendanceWithoutFsDays.forEach((day) => {
          item[day.exportLabel] = row[day.key] || '';
        });
        item.Asistencias = Number(row.asistencias || 0);
        return item;
      });
      const ws = mod.utils.json_to_sheet(exportRows);
      ws['!cols'] = [
        { wch: 18 },
        { wch: 32 },
        ...generatedAttendanceWithoutFsDays.map(() => ({ wch: 14 })),
        { wch: 12 }
      ];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Consolidado asistencia');
      mod.writeFile(wb, `consolidado_asistencia_sin_fs_${selectedAttendanceWithoutFsDateFrom}_a_${selectedAttendanceWithoutFsDateTo}.xlsx`);
      setMessage(`Excel generado correctamente. Empleados: ${generatedAttendanceWithoutFsRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel del consolidado de asistencia: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportAttendanceWithoutFs', ui);
      if (btn) {
        btn.disabled = generatedAttendanceWithoutFsRows.length === 0;
        btn.textContent = 'Generar Excel';
      }
    }
  }

  async function exportServicesWithoutFsExcel() {
    try {
      if (!generatedServicesWithoutFsRows.length) throw new Error('Primero genera el reporte.');
      const btn = qs('#btnExportServicesWithoutFs', ui);
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const exportRows = generatedServicesWithoutFsRows.map((row) => {
        const item = {
          Dependencia: row.dependencia,
          Zona: row.zona,
          Sede: row.sede,
          'Servicio planeado': row.servicio
        };
        generatedServicesWithoutFsDays.forEach((day) => {
          item[day.exportLabel] = row[day.key] || '';
        });
        item.Asistencias = Number(row.asistencias || 0);
        return item;
      });
      const ws = mod.utils.json_to_sheet(exportRows);
      ws['!cols'] = [
        { wch: 24 },
        { wch: 18 },
        { wch: 28 },
        { wch: 30 },
        ...generatedServicesWithoutFsDays.map(() => ({ wch: 14 })),
        { wch: 12 }
      ];
      const wb = mod.utils.book_new();
      mod.utils.book_append_sheet(wb, ws, 'Consolidado servicios');
      mod.writeFile(wb, `consolidado_servicios_sin_fs_${selectedServicesWithoutFsDateFrom}_a_${selectedServicesWithoutFsDateTo}.xlsx`);
      setMessage(`Excel generado correctamente. Servicios: ${generatedServicesWithoutFsRows.length}`);
    } catch (e) {
      setMessage(`Error al generar Excel del consolidado de servicios: ${e?.message || e}`);
    } finally {
      const btn = qs('#btnExportServicesWithoutFs', ui);
      if (btn) {
        btn.disabled = generatedServicesWithoutFsRows.length === 0;
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

  function renderAttendanceWithoutFsPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Consolidado asistencia (Sin FS)'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Desde']), el('input', { id: 'attendanceWithoutFsDateFrom', className: 'input', type: 'date', value: selectedAttendanceWithoutFsDateFrom, max: todayBogota(), style: 'max-width:180px' })]),
        el('div', {}, [el('label', { className: 'label' }, ['Hasta']), el('input', { id: 'attendanceWithoutFsDateTo', className: 'input', type: 'date', value: selectedAttendanceWithoutFsDateTo, max: todayBogota(), style: 'max-width:180px' })]),
        el('button', { id: 'btnGenerateAttendanceWithoutFs', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportAttendanceWithoutFs', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'attendanceWithoutFsTotal', className: 'text-muted mt-2' }, ['Selecciona el rango y genera el reporte.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', { id: 'attendanceWithoutFsHeadRow' }, [
            el('th', {}, ['Cedula']),
            el('th', {}, ['Nombre']),
            el('th', {}, ['Asistencias'])
          ])]),
          el('tbody', { id: 'attendanceWithoutFsTbody' }, [el('tr', {}, [el('td', { colSpan: 3, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateAttendanceWithoutFs', ui)?.addEventListener('click', generateAttendanceWithoutFsReport);
    qs('#btnExportAttendanceWithoutFs', ui)?.addEventListener('click', exportAttendanceWithoutFsExcel);
  }

  function renderServicesWithoutFsPanel() {
    const content = el('section', {}, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [el('h3', { style: 'margin:0;' }, ['Reporte: Consolidado servicios (Sin FS)'])]),
        el('div', {}, [el('label', { className: 'label' }, ['Desde']), el('input', { id: 'servicesWithoutFsDateFrom', className: 'input', type: 'date', value: selectedServicesWithoutFsDateFrom, max: todayBogota(), style: 'max-width:180px' })]),
        el('div', {}, [el('label', { className: 'label' }, ['Hasta']), el('input', { id: 'servicesWithoutFsDateTo', className: 'input', type: 'date', value: selectedServicesWithoutFsDateTo, max: todayBogota(), style: 'max-width:180px' })]),
        el('button', { id: 'btnGenerateServicesWithoutFs', className: 'btn', type: 'button' }, ['Generar reporte']),
        el('button', { id: 'btnExportServicesWithoutFs', className: 'btn btn--primary', type: 'button', disabled: true }, ['Generar Excel'])
      ]),
      el('p', { id: 'servicesWithoutFsTotal', className: 'text-muted mt-2' }, ['Selecciona el rango y genera el reporte.']),
      el('div', { className: 'table-wrap mt-2' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', { id: 'servicesWithoutFsHeadRow' }, [
            el('th', {}, ['Dependencia']),
            el('th', {}, ['Zona']),
            el('th', {}, ['Sede']),
            el('th', {}, ['Servicio planeado']),
            el('th', {}, ['Asistencias'])
          ])]),
          el('tbody', { id: 'servicesWithoutFsTbody' }, [el('tr', {}, [el('td', { colSpan: 5, className: 'text-muted' }, ['Sin generar.'])])])
        ])
      ])
    ]);
    qs('#reportContent', ui).replaceChildren(content);
    qs('#btnGenerateServicesWithoutFs', ui)?.addEventListener('click', generateServicesWithoutFsReport);
    qs('#btnExportServicesWithoutFs', ui)?.addEventListener('click', exportServicesWithoutFsExcel);
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
    generatedServicesWithoutFsRows = [];
    generatedServicesWithoutFsDays = [];
    generatedAttendanceWithoutFsRows = [];
    generatedAttendanceWithoutFsDays = [];
    generatedDailyRows = [];
    generatedHiringRows = [];
    generatedNoveltyRows = [];
    generatedAbsenteeismRows = [];
    ui.querySelectorAll('.report-card').forEach((n) => n.classList.toggle('is-active', n.dataset.id === selectedReportId));
    if (selectedReportId === 'employees_current') {
      renderEmployeesPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'services_without_fs') {
      renderServicesWithoutFsPanel();
      setMessage(' ');
      return;
    }
    if (selectedReportId === 'attendance_without_fs') {
      renderAttendanceWithoutFsPanel();
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
    qs('#reportContent', ui).replaceChildren(el('p', { className: 'text-muted' }, ['Selecciona una tarjeta para abrir el reporte.']));
  }

  cards.forEach((card) => card.addEventListener('click', () => openReport(card.dataset.id || '')));

  mount.replaceChildren(ui);
  return () => {};
};
