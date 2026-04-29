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
if (from > to) {
  throw new Error('La fecha inicial no puede ser mayor que la final.');
}

function addOneDay(value) {
  const [year, month, day] = value.split('-').map(Number);
  const dt = new Date(Date.UTC(year, month - 1, day));
  dt.setUTCDate(dt.getUTCDate() + 1);
  return dt.toISOString().slice(0, 10);
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
    formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 39))),
    formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 60))),
    formatUtcDate(moveToFollowingMondayUtc(addUtcDays(easter, 68)))
  ]);
  colombiaHolidayCache.set(year, holidays);
  return holidays;
}

function isColombiaHolidayDate(selectedDate) {
  const iso = String(selectedDate || '').trim();
  if (!iso) return false;
  const year = Number(iso.slice(0, 4));
  return getColombiaHolidaySet(year).has(iso);
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

const { data: sedesRows, error: sedesError } = await supabase
  .from('sedes')
  .select('codigo,nombre,numero_operarios,estado,jornada');
if (sedesError) throw sedesError;

for (let day = from; day <= to; day = addOneDay(day)) {
  const { data: closure, error: closureError } = await supabase
    .from('daily_closures')
    .select('*')
    .eq('fecha', day)
    .maybeSingle();
  if (closureError) throw closureError;
  const isClosed = closure?.locked === true || String(closure?.status || '').trim().toLowerCase() === 'closed';
  if (!isClosed) {
    console.log('Saltando', day, ': no esta cerrado.');
    continue;
  }

  const { data: statusRows, error: statusError } = await supabase
    .from('employee_daily_status')
    .select('sede_codigo,tipo_personal,servicio_programado,asistio,cuenta_pago_servicio')
    .eq('fecha', day);
  if (statusError) throw statusError;

  const scheduledRows = (statusRows || []).filter((row) => String(row?.tipo_personal || '').trim() === 'empleado' && row?.servicio_programado === true);
  const actualRows = (statusRows || []).filter((row) => row?.asistio === true || row?.asistio === false);
  const sedes = (sedesRows || [])
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));

  const bySede = new Map();
  for (const row of scheduledRows) {
    const sedeCode = String(row?.sede_codigo || '').trim();
    if (!sedeCode) continue;
    const bucket = bySede.get(sedeCode) || {
      contratados: 0,
      asistencias: 0
    };
    bucket.contratados += 1;
    if (row?.cuenta_pago_servicio === true) bucket.asistencias += 1;
    bySede.set(sedeCode, bucket);
  }

  const summary = sedes.reduce((acc, sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planned = Number(sede?.numero_operarios ?? 0) || 0;
    const counts = bySede.get(sedeCode) || { contratados: 0, asistencias: 0 };
    const ausentismos = computeOperationalAbsenteeism(planned, counts.contratados, counts.asistencias);
    acc.planeados += planned;
    acc.contratados += counts.contratados;
    acc.asistencias += counts.asistencias;
    acc.faltan += Math.max(0, planned - counts.contratados);
    acc.sobran += Math.max(0, counts.contratados - planned);
    acc.ausentismos += ausentismos;
    return acc;
  }, {
    planeados: 0,
    contratados: 0,
    asistencias: 0,
    faltan: 0,
    sobran: 0,
    ausentismos: 0
  });

  if (summary.planeados === 0 && summary.contratados === 0 && actualRows.length) {
    summary.asistencias = actualRows.filter((row) => row?.asistio === true).length;
    summary.ausentismos = 0;
    summary.faltan = 0;
    summary.sobran = actualRows.length;
  }

  const payload = {
    id: day,
    fecha: day,
    status: 'closed',
    locked: true,
    planeados: summary.planeados,
    contratados: summary.contratados,
    asistencias: summary.asistencias,
    ausentismos: summary.ausentismos,
    faltan: summary.faltan,
    sobran: summary.sobran,
    no_contratados: Math.max(0, summary.planeados - summary.contratados),
    closed_by_uid: closure?.closed_by_uid || null,
    closed_by_email: closure?.closed_by_email || 'cron@system'
  };

  const { error: upsertError } = await supabase
    .from('daily_closures')
    .upsert(payload, { onConflict: 'id' });
  if (upsertError) throw upsertError;

  console.log(day, JSON.stringify(payload));
}

function computeOperationalAbsenteeism(planeados, contratados, cubiertos) {
  const planned = Math.max(0, Number(planeados || 0));
  const contracted = Math.max(0, Number(contratados || 0));
  const covered = Math.max(0, Number(cubiertos || 0));
  if (planned <= 0) return 0;
  return Math.max(0, Math.min(planned, contracted) - covered);
}
