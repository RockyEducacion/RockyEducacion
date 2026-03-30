import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

dotenv.config({ path: '.env' });

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !serviceRoleKey) {
  throw new Error('Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en whatsapp-backend/.env');
}

const supabase = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const from = String(process.argv[2] || '2026-03-16').trim();
const to = String(process.argv[3] || from).trim();

if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
  throw new Error('Debes enviar fechas validas en formato YYYY-MM-DD.');
}
if (from > to) throw new Error('La fecha inicial no puede ser mayor que la fecha final.');

function addOneDay(value) {
  const [year, month, day] = value.split('-').map(Number);
  const dt = new Date(Date.UTC(year, month - 1, day));
  dt.setUTCDate(dt.getUTCDate() + 1);
  return dt.toISOString().slice(0, 10);
}

function makeUtcDate(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day));
}

function addUtcDays(date, days) {
  const next = new Date(date.getTime());
  next.setUTCDate(next.getUTCDate() + Number(days || 0));
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

function moveToFollowingMondayUtc(date) {
  const isoDow = date.getUTCDay() === 0 ? 7 : date.getUTCDay();
  if (isoDow === 1) return date;
  return addUtcDays(date, 8 - isoDow);
}

const holidayCache = new Map();
function getColombiaHolidaySet(year) {
  if (holidayCache.has(year)) return holidayCache.get(year);
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
    formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 39))),
    formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 60))),
    formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 68)))
  ]);
  holidayCache.set(year, holidays);
  return holidays;
}

function isColombiaHolidayDate(selectedDate) {
  const iso = String(selectedDate || '').trim();
  if (!iso) return false;
  return getColombiaHolidaySet(Number(iso.slice(0, 4))).has(iso);
}

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = String(selectedDate || '').trim();
  if (!iso || !sede) return false;
  const [year, month, day] = iso.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
  if (isColombiaHolidayDate(iso)) return false;
  if (jornada === 'lun_sab') return weekday >= 1 && weekday <= 6;
  return weekday >= 1 && weekday <= 5;
}

function normalizeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .replace(/s+/g, ' ')
    .trim()
    .toLowerCase();
}

function buildNovedadReplacementRules(rows = []) {
  const byCode = new Map();
  const byName = new Map();
  for (const row of rows || []) {
    const code = String(row?.codigo_novedad || row?.codigo || '').trim();
    const name = normalizeText(row?.nombre || '');
    const replacement = normalizeText(row?.reemplazo || '');
    const needs = ['si', 'yes', 'true', '1', 'reemplazo'].includes(replacement);
    if (code) byCode.set(code, needs);
    if (name) byName.set(name, needs);
  }
  return { byCode, byName };
}

function metricAttendanceNovedadCode(row = {}) {
  const raw = String(row?.novedad || '').trim();
  return /^d+$/.test(raw) ? raw : '';
}

function baseNovedadName(row = {}) {
  return String(row?.novedad || '').replace(/s*(.*)s*$/, '').trim();
}

function metricAttendanceRequiresReplacement(row = {}, rules = {}) {
  const code = metricAttendanceNovedadCode(row);
  if (['1', '7'].includes(code)) return false;
  if (['2', '3', '4', '5', '8', '9'].includes(code)) return true;
  if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
  const name = normalizeText(baseNovedadName(row));
  if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
  return false;
}

function metricReplacementKey(row = {}) {
  const fecha = String(row?.fecha || '').trim();
  const employeeId = String(row?.empleado_id || row?.empleadoId || '').trim();
  const documento = String(row?.documento || '').trim();
  return [fecha, employeeId || documento].join('|');
}

function dedupeAttendanceRows(rows = []) {
  const map = new Map();
  for (const row of rows || []) {
    const key = metricReplacementKey(row) + '|' + String(row?.id || '');
    const logicalKey = String(row?.fecha || '').trim() + '|' + (String(row?.empleado_id || row?.empleadoId || '').trim() || String(row?.documento || '').trim() || String(row?.id || '').trim());
    const existing = map.get(logicalKey);
    if (!existing) {
      map.set(logicalKey, row);
      continue;
    }
    const existingTs = String(existing?.created_at || '');
    const rowTs = String(row?.created_at || '');
    if (rowTs > existingTs) map.set(logicalKey, row);
  }
  return Array.from(map.values());
}

function mapEmployeeRow(row = {}) {
  return {
    id: row.id || null,
    documento: row.documento || null,
    sedeCodigo: row.sede_codigo || row.sedeCodigo || null,
    fechaIngreso: row.fecha_ingreso || row.fechaIngreso || null,
    fechaRetiro: row.fecha_retiro || row.fechaRetiro || null,
    estado: row.estado || 'activo',
    cargoCodigo: row.cargo_codigo || row.cargoCodigo || null,
    cargoNombre: row.cargo_nombre || row.cargoNombre || null
  };
}

function toISODate(value) {
  if (!value) return null;
  const raw = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const dt = new Date(raw);
  return Number.isNaN(dt.getTime()) ? null : dt.toISOString().slice(0, 10);
}

function normalizeCargoAlignment(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'supernumerario') return 'supernumerario';
  if (raw === 'supervisor') return 'supervisor';
  return 'empleado';
}

function isEmployeeSupernumerario(emp, cargoMap) {
  const code = String(emp?.cargoCodigo || '').trim();
  const cargo = cargoMap.get(code) || null;
  if (cargo && normalizeCargoAlignment(cargo?.alineacion_crud) === 'supernumerario') return true;
  return String(emp?.cargoNombre || '').trim().toLowerCase().includes('supernumer');
}

function isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes) {
  const ingreso = toISODate(emp?.fechaIngreso);
  if (!ingreso || ingreso > day) return false;
  const retiro = toISODate(emp?.fechaRetiro);
  const estado = String(emp?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') {
    if (!(retiro && retiro >= day)) return false;
  } else if (retiro && retiro < day) {
    return false;
  }
  const sedeCode = String(emp?.sedeCodigo || '').trim();
  return !!sedeCode && activeSedeCodes.has(sedeCode);
}

async function computeDailySedeClosureSnapshot(day) {
  const [{ data: attendance }, { data: replacements }, { data: sedesRows }, { data: employeesRows }, { data: cargosRows }, { data: novedadesRows }] = await Promise.all([
    supabase.from('attendance').select('*').eq('fecha', day),
    supabase.from('import_replacements').select('*').eq('fecha', day),
    supabase.from('sedes').select('*'),
    supabase.from('employees').select('*'),
    supabase.from('cargos').select('codigo,alineacion_crud,nombre'),
    supabase.from('novedades').select('codigo,codigo_novedad,nombre,reemplazo')
  ]);

  const sedes = (sedesRows || [])
    .filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((row) => isSedeScheduledForDate(row, day));
  const activeSedeCodes = new Set(sedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row?.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules(novedadesRows || []);
  const replacementMap = new Map((replacements || []).map((row) => [metricReplacementKey(row), row]));
  const replacementSuperDocs = new Set(
    (replacements || [])
      .filter((row) => String(row?.decision || '').trim().toLowerCase() === 'reemplazo')
      .map((row) => day + '|' + String(row?.supernumerario_documento || row?.supernumerarioDocumento || '').trim())
      .filter((value) => !value.endsWith('|'))
  );

  const employeeById = new Map();
  const employeeByDoc = new Map();
  const contractedBySede = new Map();
  const supernumerarioDocs = new Set();

  for (const raw of employeesRows || []) {
    const emp = mapEmployeeRow(raw);
    const empId = String(emp?.id || '').trim();
    const doc = String(emp?.documento || '').trim();
    if (empId) employeeById.set(empId, emp);
    if (doc) employeeByDoc.set(doc, emp);
    if (doc && isEmployeeSupernumerario(emp, cargoMap) && isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes)) {
      supernumerarioDocs.add(doc);
    }
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes)) continue;
    if (isEmployeeSupernumerario(emp, cargoMap)) continue;
    const sedeCode = String(emp?.sedeCodigo || '').trim();
    if (!contractedBySede.has(sedeCode)) contractedBySede.set(sedeCode, new Set());
    contractedBySede.get(sedeCode).add(doc || empId);
  }

  const registeredBySede = new Map();
  const novSinReemplazoBySede = new Map();
  for (const row of dedupeAttendanceRows(attendance || [])) {
    const doc = String(row?.documento || '').trim();
    if (doc && replacementSuperDocs.has(day + '|' + doc)) continue;
    if (doc && supernumerarioDocs.has(doc)) continue;
    const empId = String(row?.empleado_id || row?.empleadoId || '').trim();
    const employee = (empId && employeeById.get(empId)) || (doc && employeeByDoc.get(doc)) || null;
    if (isEmployeeSupernumerario(employee, cargoMap)) continue;
    const sedeCode = String(row?.sede_codigo || row?.sedeCodigo || employee?.sedeCodigo || '').trim();
    if (!sedeCode || !activeSedeCodes.has(sedeCode)) continue;
    if (!registeredBySede.has(sedeCode)) registeredBySede.set(sedeCode, new Set());
    registeredBySede.get(sedeCode).add(doc || empId || String(row?.id || '').trim());
    const repl = replacementMap.get(metricReplacementKey(row)) || null;
    const hasReplacement = String(repl?.decision || '').trim().toLowerCase() === 'reemplazo';
    if (row?.asistio === false && metricAttendanceRequiresReplacement(row, replacementRules) && !hasReplacement) {
      novSinReemplazoBySede.set(sedeCode, Number(novSinReemplazoBySede.get(sedeCode) || 0) + 1);
    }
  }

  return sedes.map((sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planeados = Number(sede?.numero_operarios ?? 0) || 0;
    const baseContracted = Number(contractedBySede.get(sedeCode)?.size || 0);
    const registrados = Number(registeredBySede.get(sedeCode)?.size || 0);
    const externalRegistered = Math.max(0, registrados - baseContracted);
    const contratados = Math.min(planeados, baseContracted + externalRegistered);
    const faltantes = Math.max(0, planeados - registrados);
    const sobrantes = Math.max(0, registrados - planeados);
    return {
      id: day + '_' + sedeCode,
      fecha: day,
      sede_codigo: sedeCode,
      sede_nombre: sede?.nombre || sedeCode || null,
      zona_codigo: sede?.zona_codigo || null,
      zona_nombre: sede?.zona_nombre || null,
      dependencia_codigo: sede?.dependencia_codigo || null,
      dependencia_nombre: sede?.dependencia_nombre || null,
      planeados,
      contratados,
      registrados,
      faltantes,
      sobrantes
    };
  });
}

let current = from;
while (current <= to) {
  const { data: closure, error: closureError } = await supabase
    .from('daily_closures')
    .select('fecha,locked,status')
    .eq('fecha', current)
    .maybeSingle();
  if (closureError) throw closureError;
  const isClosed = closure?.locked === true || String(closure?.status || '').trim().toLowerCase() === 'closed';
  if (!isClosed) {
    console.log('Saltando', current, ': no esta cerrado.');
    current = addOneDay(current);
    continue;
  }
  const snapshot = await computeDailySedeClosureSnapshot(current);
  if (snapshot.length) {
    const { error } = await supabase.from('daily_sede_closures').upsert(snapshot, { onConflict: 'id' });
    if (error) throw error;
  }
  console.log(current, 'rows:', snapshot.length);
  current = addOneDay(current);
}
