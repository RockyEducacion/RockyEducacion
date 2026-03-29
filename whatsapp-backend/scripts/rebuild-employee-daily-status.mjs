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
const to = String(process.argv[3] || '2026-03-28').trim();
const refreshSnapshots = process.argv.includes('--snapshots');

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

console.log(`Reconstruyendo employee_daily_status desde ${from} hasta ${to}...`);
const { data: rebuiltRows, error: rebuildError } = await supabase.rpc('refresh_employee_daily_status_range', {
  p_fecha_desde: from,
  p_fecha_hasta: to
});
if (rebuildError) throw rebuildError;
console.log('Filas reconstruidas reportadas por RPC:', rebuiltRows ?? 0);

if (refreshSnapshots) {
  console.log('Refrescando snapshots operativos por fecha...');
  let current = from;
  while (current <= to) {
    const { error } = await supabase.rpc('refresh_operational_snapshots_from_employee_daily_status', {
      p_fecha: current
    });
    if (error) throw error;
    console.log('Snapshots refrescados para', current);
    current = addOneDay(current);
  }
}

const { count, error: countError } = await supabase
  .from('employee_daily_status')
  .select('*', { count: 'exact', head: true })
  .gte('fecha', from)
  .lte('fecha', to);
if (countError) throw countError;

const { data: sample, error: sampleError } = await supabase
  .from('employee_daily_status')
  .select('fecha,documento,nombre,tipo_personal,estado_dia,sede_codigo,decision_cobertura,cuenta_pago_servicio,paga_nomina,closed')
  .gte('fecha', from)
  .lte('fecha', to)
  .order('fecha', { ascending: true })
  .limit(5);
if (sampleError) throw sampleError;

console.log('Conteo final employee_daily_status en rango:', count ?? 0);
console.log('Muestra:', JSON.stringify(sample || [], null, 2));
