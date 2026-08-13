import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

dotenv.config({ path: '.env' });

const from = String(process.argv[2] || '').trim();
const to = String(process.argv[3] || from).trim();
if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
  throw new Error('Debes enviar fechas validas en formato YYYY-MM-DD.');
}
if (from > to) {
  throw new Error('La fecha inicial no puede ser mayor que la fecha final.');
}

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !serviceRoleKey) {
  throw new Error('Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en whatsapp-backend/.env');
}

const supabase = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const { closeOperationDay } = await import('../src/app.js');

function addOneDay(value) {
  const [year, month, day] = value.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

const results = [];
for (let day = from; day <= to; day = addOneDay(day)) {
  const { error: reopenError } = await supabase
    .from('daily_closures')
    .update({ locked: false, status: 'reprocessing' })
    .eq('fecha', day);
  if (reopenError) throw reopenError;

  const { error: metricsError } = await supabase
    .from('daily_metrics')
    .update({ closed: false })
    .eq('fecha', day);
  if (metricsError) throw metricsError;

  const { error: sedeClosureError } = await supabase
    .from('daily_sede_closures')
    .delete()
    .eq('fecha', day);
  if (sedeClosureError) throw sedeClosureError;

  const result = await closeOperationDay(day);
  results.push(result);
  console.log(JSON.stringify(result));
}

console.log(JSON.stringify({ ok: true, from, to, results }, null, 2));
