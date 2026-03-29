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

function buildDailyRecordId(fecha, documento, empleadoId) {
  const safeDate = String(fecha || '').trim() || 'sin-fecha';
  const safeDocument = String(documento || '').trim() || 'sin-documento';
  const safeEmployee = String(empleadoId || '').trim() || 'sin-empleado';
  return `${safeDate}_${safeDocument}_${safeEmployee}`;
}

for (let day = from; day <= to; day = addOneDay(day)) {
  console.log(`Normalizando ${day}...`);

  const { data: closure, error: closureError } = await supabase
    .from('daily_closures')
    .select('fecha, locked, status')
    .eq('fecha', day)
    .maybeSingle();
  if (closureError) throw closureError;
  const isClosed = closure?.locked === true || String(closure?.status || '').trim().toLowerCase() === 'closed';
  if (!isClosed) {
    console.log(`  Saltando ${day}: el dia no esta cerrado.`);
    continue;
  }

  const { error: refreshError } = await supabase.rpc('refresh_employee_daily_status', { p_fecha: day });
  if (refreshError) throw refreshError;

  const { data: statusRows, error: statusError } = await supabase
    .from('employee_daily_status')
    .select('employee_id, documento, nombre, sede_codigo, sede_nombre_snapshot, tipo_personal, servicio_programado, cuenta_pago_servicio, novedad_codigo, novedad_nombre')
    .eq('fecha', day)
    .eq('tipo_personal', 'empleado')
    .eq('servicio_programado', true)
    .eq('cuenta_pago_servicio', false);
  if (statusError) throw statusError;

  let changed = 0;
  for (const row of statusRows || []) {
    const recordId = buildDailyRecordId(day, row?.documento, row?.employee_id);
    const novedadCodigo = String(row?.novedad_codigo || '').trim() || '8';
    const novedadNombre = String(row?.novedad_nombre || '').trim() || 'AUSENCIA NO JUSTIFICADA';

    const { error: replacementError } = await supabase.from('import_replacements').upsert({
      id: recordId,
      fecha_operacion: day,
      fecha: day,
      empleado_id: row?.employee_id || null,
      documento: row?.documento || null,
      nombre: row?.nombre || null,
      sede_codigo: row?.sede_codigo || null,
      sede_nombre: row?.sede_nombre_snapshot || null,
      novedad_codigo: novedadCodigo,
      novedad_nombre: novedadNombre,
      decision: 'ausentismo',
      actor_uid: null,
      actor_email: 'cron@system'
    }, { onConflict: 'id' });
    if (replacementError) throw replacementError;

    const { error: absenteeismError } = await supabase.from('absenteeism').upsert({
      id: recordId,
      fecha: day,
      empleado_id: row?.employee_id || null,
      documento: row?.documento || null,
      nombre: row?.nombre || null,
      sede_codigo: row?.sede_codigo || null,
      sede_nombre: row?.sede_nombre_snapshot || null,
      estado: 'confirmado',
      created_by_uid: null,
      created_by_email: 'cron@system'
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
    changed += 1;
  }

  const { error: refreshAfterError } = await supabase.rpc('refresh_employee_daily_status', { p_fecha: day });
  if (refreshAfterError) throw refreshAfterError;
  const { error: snapshotError } = await supabase.rpc('refresh_operational_snapshots_from_employee_daily_status', { p_fecha: day });
  if (snapshotError) throw snapshotError;

  console.log(`  Normalizados ${changed} registros en ${day}.`);
}