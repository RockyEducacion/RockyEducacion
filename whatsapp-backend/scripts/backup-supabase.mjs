import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import { createClient } from '@supabase/supabase-js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..', '..');
const backendRoot = path.resolve(__dirname, '..');

dotenv.config({ path: path.join(backendRoot, '.env') });

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !serviceRoleKey) {
  throw new Error('Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en whatsapp-backend/.env');
}

const supabase = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false, autoRefreshToken: false }
});

const PAGE_SIZE = 1000;
const tables = [
  { name: 'attendance', order: 'fecha' },
  { name: 'import_replacements', order: 'fecha' },
  { name: 'sede_status', order: 'fecha' },
  { name: 'daily_metrics', order: 'fecha' },
  { name: 'daily_closures', order: 'fecha' },
  { name: 'daily_sede_closures', order: 'fecha' },
  { name: 'import_history', order: 'fecha_operacion' },
  { name: 'employees', order: 'created_at' },
  { name: 'sedes', order: 'created_at' },
  { name: 'cargos', order: 'created_at' },
  { name: 'novedades', order: 'created_at' },
  { name: 'whatsapp_sessions', order: 'updated_at' },
  { name: 'whatsapp_incoming', order: 'received_at' }
];

function stamp() {
  const iso = new Date().toISOString().replace(/[:.]/g, '-');
  return iso;
}

async function selectAllRows(table, order) {
  const rows = [];
  let from = 0;

  while (true) {
    let query = supabase.from(table).select('*').range(from, from + PAGE_SIZE - 1);
    if (order) query = query.order(order, { ascending: true });
    const { data, error } = await query;
    if (error) throw error;
    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  return rows;
}

async function main() {
  const backupId = `supabase-backup-${stamp()}`;
  const backupDir = path.join(projectRoot, 'backups', backupId);
  await fs.mkdir(backupDir, { recursive: true });

  const manifest = {
    backupId,
    createdAt: new Date().toISOString(),
    source: supabaseUrl,
    tables: []
  };

  for (const table of tables) {
    const rows = await selectAllRows(table.name, table.order);
    const filePath = path.join(backupDir, `${table.name}.json`);
    await fs.writeFile(filePath, JSON.stringify(rows, null, 2), 'utf8');
    manifest.tables.push({
      table: table.name,
      rows: rows.length,
      file: `${table.name}.json`
    });
    console.log(`${table.name}: ${rows.length} filas`);
  }

  await fs.writeFile(path.join(backupDir, 'manifest.json'), JSON.stringify(manifest, null, 2), 'utf8');
  console.log(`Backup creado en ${backupDir}`);
}

main().catch((error) => {
  console.error('No se pudo crear el backup:', error);
  process.exitCode = 1;
});

