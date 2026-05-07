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

function todayBogotaISO() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

function addOneDay(value) {
  const [year, month, day] = String(value || '').split('-').map(Number);
  const dt = new Date(Date.UTC(year, (month || 1) - 1, day || 1));
  dt.setUTCDate(dt.getUTCDate() + 1);
  return dt.toISOString().slice(0, 10);
}

function isoDatePart(value) {
  return String(value || '').slice(0, 10);
}

const { data: historyRows, error: historyError } = await supabase
  .from('employee_cargo_history')
  .select('*')
  .order('employee_id', { ascending: true })
  .order('created_at', { ascending: true });
if (historyError) throw historyError;

const byEmployee = new Map();
for (const row of historyRows || []) {
  const key = String(row.employee_id || '').trim();
  if (!key) continue;
  if (!byEmployee.has(key)) byEmployee.set(key, []);
  byEmployee.get(key).push(row);
}

const rowUpdates = [];
const employeeUpdates = [];
let refreshFrom = null;

for (const [employeeId, rows] of byEmployee.entries()) {
  for (let index = 1; index < rows.length; index += 1) {
    const previous = rows[index - 1];
    const current = rows[index];
    const previousIngreso = isoDatePart(previous.fecha_ingreso);
    const currentIngreso = isoDatePart(current.fecha_ingreso);
    const candidateIngreso = current.created_at || current.updated_at || null;
    const candidateIngresoDate = isoDatePart(candidateIngreso);
    if (!candidateIngreso || !candidateIngresoDate) continue;
    if (currentIngreso && previousIngreso && currentIngreso <= previousIngreso) {
      rowUpdates.push({
        id: current.id,
        employeeId,
        documento: current.documento || previous.documento || null,
        source: current.source || null,
        from: current.fecha_ingreso,
        to: candidateIngreso
      });
      current.fecha_ingreso = candidateIngreso;
      if (!refreshFrom || candidateIngresoDate < refreshFrom) refreshFrom = candidateIngresoDate;
    }
  }

  const openRows = rows.filter((row) => row.fecha_retiro == null);
  if (openRows.length !== 1) continue;
  const activeRow = openRows[0];
  if (String(activeRow.source || '').trim() !== 'cargo_change') continue;
  employeeUpdates.push({
    employeeId,
    documento: activeRow.documento || null,
    fecha_ingreso: activeRow.fecha_ingreso
  });
}

if (!rowUpdates.length) {
  console.log('No se encontraron traslapes historicos para corregir.');
  process.exit(0);
}

console.log('Registros de historial a corregir:', JSON.stringify(rowUpdates, null, 2));

for (const update of rowUpdates) {
  const { error } = await supabase
    .from('employee_cargo_history')
    .update({ fecha_ingreso: update.to })
    .eq('id', update.id);
  if (error) throw error;
}

for (const update of employeeUpdates) {
  const { error } = await supabase
    .from('employees')
    .update({ fecha_ingreso: update.fecha_ingreso })
    .eq('id', update.employeeId);
  if (error) throw error;
}

const refreshTo = todayBogotaISO();
console.log(`Reconstruyendo employee_daily_status desde ${refreshFrom} hasta ${refreshTo}...`);
const { data: rebuiltRows, error: rebuildError } = await supabase.rpc('refresh_employee_daily_status_range', {
  p_fecha_desde: refreshFrom,
  p_fecha_hasta: refreshTo
});
if (rebuildError) throw rebuildError;
console.log('Filas reconstruidas reportadas por RPC:', rebuiltRows ?? 0);

console.log('Refrescando snapshots operativos por fecha...');
let current = refreshFrom;
while (current <= refreshTo) {
  const { error } = await supabase.rpc('refresh_operational_snapshots_from_employee_daily_status', {
    p_fecha: current
  });
  if (error) throw error;
  console.log('Snapshots refrescados para', current);
  current = addOneDay(current);
}

console.log('Proceso completado.');
