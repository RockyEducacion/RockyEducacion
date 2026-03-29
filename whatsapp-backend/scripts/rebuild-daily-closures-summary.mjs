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

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = String(selectedDate || '').trim();
  if (!iso || !sede) return false;
  const [year, month, day] = iso.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
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

  const sedes = (sedesRows || [])
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));

  const bySede = new Map();
  for (const row of statusRows || []) {
    if (String(row?.tipo_personal || '').trim() !== 'empleado') continue;
    if (row?.servicio_programado !== true) continue;
    const sedeCode = String(row?.sede_codigo || '').trim();
    if (!sedeCode) continue;
    const bucket = bySede.get(sedeCode) || {
      contratados: 0,
      asistencias: 0,
      ausentismos: 0
    };
    bucket.contratados += 1;
    if (row?.cuenta_pago_servicio === true) bucket.asistencias += 1;
    if (row?.cuenta_pago_servicio === false) bucket.ausentismos += 1;
    bySede.set(sedeCode, bucket);
  }

  const summary = sedes.reduce((acc, sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planned = Number(sede?.numero_operarios ?? 0) || 0;
    const counts = bySede.get(sedeCode) || { contratados: 0, asistencias: 0, ausentismos: 0 };
    acc.planeados += planned;
    acc.contratados += counts.contratados;
    acc.asistencias += counts.asistencias;
    acc.faltan += Math.max(0, planned - counts.contratados);
    acc.sobran += Math.max(0, counts.contratados - planned);
    acc.ausentismos += counts.ausentismos;
    return acc;
  }, {
    planeados: 0,
    contratados: 0,
    asistencias: 0,
    faltan: 0,
    sobran: 0,
    ausentismos: 0
  });

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
