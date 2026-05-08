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

const from = '2026-03-12';
const to = '2026-03-31';

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

console.log('Proceso completado.');
