import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm';
import { SUPABASE_ANON_KEY, SUPABASE_PROFILES_TABLE, SUPABASE_URL } from './config.js';

function assertSupabaseConfig() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error('Configura SUPABASE_URL y SUPABASE_ANON_KEY en src/assets/js/config.js.');
  }
}

assertSupabaseConfig();

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true
  }
});

const tableReloaders = new Map();

function registerTableReloader(table, reloader) {
  if (!tableReloaders.has(table)) tableReloaders.set(table, new Set());
  tableReloaders.get(table).add(reloader);
  return () => tableReloaders.get(table)?.delete(reloader);
}

async function notifyTableReload(table) {
  const loaders = [...(tableReloaders.get(table) || [])];
  await Promise.all(loaders.map(async (fn) => {
    try {
      await fn();
    } catch (error) {
      console.error(`No se pudo refrescar ${table}:`, error);
    }
  }));
}

function normalizeUser(user) {
  if (!user) return null;
  return {
    uid: user.id,
    email: user.email || '',
    displayName: user.user_metadata?.display_name || user.user_metadata?.full_name || null
  };
}

function normalizeProfileRow(uid, data = {}) {
  return {
    id: uid,
    email: String(data.email || '').trim().toLowerCase() || null,
    display_name: data.nombre || data.displayName || null,
    documento: data.documento || null,
    estado: data.estado || 'activo',
    updated_at: new Date().toISOString()
  };
}

async function upsertProfile(uid, data = {}) {
  const payload = normalizeProfileRow(uid, data);
  const { error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .upsert(payload, { onConflict: 'id' });
  if (error) throw error;
}

function normalizeTimestamp(value) {
  if (!value) return null;
  return value;
}

function mapCatalogRow(row = {}) {
  return {
    id: row.id,
    codigo: row.codigo || null,
    nombre: row.nombre || null,
    estado: row.estado || 'activo',
    createdByUid: row.created_by_uid || null,
    createdByEmail: row.created_by_email || null,
    createdAt: normalizeTimestamp(row.created_at),
    updatedAt: normalizeTimestamp(row.updated_at)
  };
}

function mapSedeRow(row = {}) {
  return {
    ...mapCatalogRow(row),
    dependenciaCodigo: row.dependencia_codigo || null,
    dependenciaNombre: row.dependencia_nombre || null,
    zonaCodigo: row.zona_codigo || null,
    zonaNombre: row.zona_nombre || null,
    numeroOperarios: typeof row.numero_operarios === 'number' ? row.numero_operarios : null,
    jornada: row.jornada || 'lun_vie'
  };
}

function mapCargoRow(row = {}) {
  return {
    ...mapCatalogRow(row),
    alineacionCrud: row.alineacion_crud || 'empleado'
  };
}

function mapNovedadRow(row = {}) {
  return {
    ...mapCatalogRow(row),
    codigoNovedad: row.codigo_novedad || null,
    reemplazo: row.reemplazo || null,
    nomina: row.nomina || null
  };
}

function mapEmployeeRow(row = {}) {
  return {
    ...mapCatalogRow(row),
    documento: row.documento || null,
    telefono: row.telefono || null,
    cargoCodigo: row.cargo_codigo || null,
    cargoNombre: row.cargo_nombre || null,
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
    zonaCodigo: row.zona_codigo || null,
    zonaNombre: row.zona_nombre || null,
    fechaIngreso: row.fecha_ingreso || null,
    fechaRetiro: row.fecha_retiro || null,
    lastModifiedByUid: row.last_modified_by_uid || null,
    lastModifiedByEmail: row.last_modified_by_email || null,
    lastModifiedAt: row.last_modified_at || null
  };
}

function mapSupervisorProfileRow(row = {}) {
  return {
    id: row.employee_id || row.id,
    profileId: row.id,
    codigo: row.employee_codigo || null,
    documento: row.documento || null,
    nombre: row.nombre || null,
    cargoCodigo: row.cargo_codigo || null,
    cargoNombre: row.cargo_nombre || null,
    sedeCodigo: row.sede_codigo || null,
    zonaCodigo: row.zona_codigo || null,
    zonaNombre: row.zona_nombre || null,
    fechaIngreso: row.fecha_ingreso || null,
    fechaRetiro: row.fecha_retiro || null,
    estado: row.estado || 'activo',
    createdByUid: row.created_by_uid || null,
    createdByEmail: row.created_by_email || null,
    createdAt: row.created_at || null,
    lastModifiedByUid: row.last_modified_by_uid || null,
    lastModifiedByEmail: row.last_modified_by_email || null,
    lastModifiedAt: row.last_modified_at || null
  };
}

function mapByDocument(rows = []) {
  const out = new Map();
  rows.forEach((row) => {
    const documento = String(row?.documento || '').trim();
    if (!documento) return;
    out.set(documento, row);
  });
  return out;
}

function mapCargoHistoryRow(row = {}) {
  return {
    id: row.id,
    employeeId: row.employee_id || null,
    employeeCodigo: row.employee_codigo || null,
    documento: row.documento || null,
    cargoCodigo: row.cargo_codigo || null,
    cargoNombre: row.cargo_nombre || null,
    fechaIngreso: row.fecha_ingreso || null,
    fechaRetiro: row.fecha_retiro || null,
    source: row.source || null,
    createdAt: row.created_at || null
  };
}

function mapImportHistoryRow(row = {}) {
  return {
    id: row.id,
    fechaOperacion: row.fecha_operacion || null,
    ts: row.ts || null,
    source: row.source || null,
    plannedCount: Number(row.planned_count || 0),
    expectedCount: Number(row.expected_count || 0),
    foundCount: Number(row.found_count || 0),
    missingCount: Number(row.missing_count || 0),
    extraCount: Number(row.extra_count || 0),
    missingSupervisorsCount: Number(row.missing_supervisors_count || 0),
    missingSupernumerariosCount: Number(row.missing_supernumerarios_count || 0),
    missingDocs: Array.isArray(row.missing_docs) ? row.missing_docs : [],
    extraDocs: Array.isArray(row.extra_docs) ? row.extra_docs : [],
    missingSupervisors: Array.isArray(row.missing_supervisors) ? row.missing_supervisors : [],
    missingSupernumerarios: Array.isArray(row.missing_supernumerarios) ? row.missing_supernumerarios : [],
    errores: Array.isArray(row.errores) ? row.errores : [],
    confirmadoPorUid: row.confirmado_por_uid || null,
    confirmadoPorEmail: row.confirmado_por_email || null,
    planeados: Number(row.planned_count || 0),
    contratados: Number(row.expected_count || 0),
    closedByUid: row.confirmado_por_uid || null,
    closedByEmail: row.confirmado_por_email || null
  };
}

function mapAttendanceRow(row = {}) {
  const rawNovedad = row.novedad || null;
  const novedadText = String(rawNovedad || '').trim();
  const novedadCodigo = /^\d+$/.test(novedadText) ? novedadText : null;
  return {
    id: row.id,
    fecha: row.fecha || null,
    empleadoId: row.empleado_id || null,
    documento: row.documento || null,
    nombre: row.nombre || null,
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
    asistio: row.asistio === true,
    novedad: rawNovedad,
    novedadCodigo,
    novedadNombre: novedadCodigo ? null : rawNovedad,
    createdAt: row.created_at || null
  };
}

function mapImportReplacementRow(row = {}) {
  return {
    id: row.id,
    importId: row.import_id || null,
    fechaOperacion: row.fecha_operacion || null,
    fecha: row.fecha || null,
    empleadoId: row.empleado_id || null,
    documento: row.documento || null,
    nombre: row.nombre || null,
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
    novedadCodigo: row.novedad_codigo || null,
    novedadNombre: row.novedad_nombre || null,
    decision: row.decision || 'ausentismo',
    supernumerarioId: row.supernumerario_id || null,
    supernumerarioDocumento: row.supernumerario_documento || null,
    supernumerarioNombre: row.supernumerario_nombre || null,
    ts: row.ts || null,
    actorUid: row.actor_uid || null,
    actorEmail: row.actor_email || null
  };
}

function mapSedeStatusRow(row = {}) {
  return {
    id: row.id,
    fecha: row.fecha || null,
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
    operariosEsperados: Number(row.operarios_esperados || 0),
    operariosPresentes: Number(row.operarios_presentes || 0),
    faltantes: Number(row.faltantes || 0),
    createdAt: row.created_at || null
  };
}

function mapDailyMetricsRow(row = {}) {
  return {
    id: row.id,
    fecha: row.fecha || null,
    planned: Number(row.planned || 0),
    expected: Number(row.expected || 0),
    unique: Number(row.unique_count || 0),
    missing: Number(row.missing || 0),
    attendanceCount: Number(row.attendance_count || 0),
    absenteeism: Number(row.absenteeism || 0),
    paidServices: Number(row.paid_services || 0),
    noContracted: Number(row.no_contracted || 0),
    closed: row.closed === true,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapDailyClosureRow(row = {}) {
  return {
    id: row.id,
    fecha: row.fecha || null,
    status: row.status || 'closed',
    locked: row.locked === true,
    planeados: Number(row.planeados || 0),
    contratados: Number(row.contratados || 0),
    ausentismos: Number(row.ausentismos || 0),
    pagados: Number(row.pagados || 0),
    noContratados: Number(row.no_contratados || 0),
    closedByUid: row.closed_by_uid || null,
    closedByEmail: row.closed_by_email || null,
    closedAt: row.closed_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapIncapacidadRow(row = {}) {
  return {
    id: row.id,
    employeeId: row.employee_id || null,
    documento: row.documento || null,
    nombre: row.nombre || null,
    fechaInicio: row.fecha_inicio || null,
    fechaFin: row.fecha_fin || null,
    estado: row.estado || 'activo',
    source: row.source || null,
    whatsappMessageId: row.whatsapp_message_id || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

async function getCurrentAuditFields() {
  const { data, error } = await supabase.auth.getUser();
  if (error) throw error;
  const user = data.user;
  return {
    created_by_uid: user?.id || null,
    created_by_email: user?.email ? String(user.email).toLowerCase() : null
  };
}

async function resolveZoneBySedeCode(sedeCodigo) {
  const code = String(sedeCodigo || '').trim();
  if (!code) return { zonaCodigo: null, zonaNombre: null };
  const { data, error } = await supabase
    .from('sedes')
    .select('zona_codigo, zona_nombre')
    .eq('codigo', code)
    .maybeSingle();
  if (error) throw error;
  return {
    zonaCodigo: data?.zona_codigo || null,
    zonaNombre: data?.zona_nombre || null
  };
}

async function findCargoByCodeInternal(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('cargos').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data;
}

function normalizeCargoAlignment(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['supernumerario', 'supervisor', 'empleado'].includes(normalized)) return normalized;
  return 'empleado';
}

async function getCargoCrudAlignmentByCode(cargoCodigo, cargoNombre = null) {
  const code = String(cargoCodigo || '').trim();
  const inferByName = (name) => {
    const n = String(name || '').trim().toLowerCase();
    if (!n) return 'empleado';
    if (n.includes('supernumer')) return 'supernumerario';
    if (n.includes('supervisor')) return 'supervisor';
    return 'empleado';
  };
  if (!code) return inferByName(cargoNombre);
  const cargo = await findCargoByCodeInternal(code);
  if (!cargo) return inferByName(cargoNombre);
  return normalizeCargoAlignment(cargo.alineacion_crud || cargoNombre);
}

async function appendEmployeeCargoHistory({
  employeeId,
  employeeCodigo,
  documento,
  cargoCodigo,
  cargoNombre,
  fechaIngreso,
  fechaRetiro = null,
  source = 'manual'
}) {
  if (!employeeId) return;
  const { error } = await supabase.from('employee_cargo_history').insert({
    employee_id: employeeId,
    employee_codigo: employeeCodigo || null,
    documento: documento || null,
    cargo_codigo: cargoCodigo || null,
    cargo_nombre: cargoNombre || null,
    fecha_ingreso: fechaIngreso || null,
    fecha_retiro: fechaRetiro || null,
    source
  });
  if (error) throw error;
  await notifyTableReload('employee_cargo_history');
}

async function upsertSupervisorProfileFromEmployee(employee, override = {}) {
  const audit = await getCurrentAuditFields();
  const payload = {
    employee_id: employee.id,
    employee_codigo: override.codigo ?? employee.codigo ?? null,
    documento: override.documento ?? employee.documento ?? null,
    nombre: override.nombre ?? employee.nombre ?? null,
    cargo_codigo: override.cargoCodigo ?? employee.cargoCodigo ?? null,
    cargo_nombre: override.cargoNombre ?? employee.cargoNombre ?? null,
    sede_codigo: override.sedeCodigo ?? employee.sedeCodigo ?? null,
    zona_codigo: override.zonaCodigo ?? employee.zonaCodigo ?? null,
    zona_nombre: override.zonaNombre ?? employee.zonaNombre ?? null,
    fecha_ingreso: override.fechaIngreso ?? employee.fechaIngreso ?? null,
    fecha_retiro: override.fechaRetiro ?? employee.fechaRetiro ?? null,
    estado: override.estado ?? employee.estado ?? 'activo',
    created_by_uid: audit.created_by_uid,
    created_by_email: audit.created_by_email,
    last_modified_by_uid: audit.created_by_uid,
    last_modified_by_email: audit.created_by_email,
    last_modified_at: new Date().toISOString()
  };
  const { data, error } = await supabase
    .from('supervisor_profile')
    .upsert(payload, { onConflict: 'documento' })
    .select('*')
    .single();
  if (error) throw error;
  await notifyTableReload('supervisor_profile');
  return data;
}

async function recomputeDailyMetrics(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return null;
  const [{ data: attendance }, { data: replacements }, { data: closures }] = await Promise.all([
    supabase.from('attendance').select('*').eq('fecha', day),
    supabase.from('import_replacements').select('*').eq('fecha', day),
    supabase.from('daily_closures').select('*').eq('fecha', day).maybeSingle()
  ]);
  const attRows = (attendance || []).map(mapAttendanceRow);
  const repRows = (replacements || []).map(mapImportReplacementRow);
  const uniqueDocs = new Set(attRows.map((row) => String(row.documento || row.empleadoId || '')).filter(Boolean));
  const absenteeism = repRows.filter((row) => row.decision === 'ausentismo').length;
  const replaced = repRows.filter((row) => row.decision === 'reemplazo').length;
  const attendanceCount = attRows.length;
  const expected = attendanceCount;
  const planned = attendanceCount + absenteeism;
  const paidServices = Math.max(0, expected - absenteeism + replaced);
  const noContracted = Math.max(0, planned - expected);
  const payload = {
    id: day,
    fecha: day,
    planned,
    expected,
    unique_count: uniqueDocs.size,
    missing: Math.max(0, planned - expected),
    attendance_count: attendanceCount,
    absenteeism,
    paid_services: paidServices,
    no_contracted: noContracted,
    closed: closures?.locked === true || String(closures?.status || '').trim() === 'closed'
  };
  const { data, error } = await supabase.from('daily_metrics').upsert(payload, { onConflict: 'id' }).select('*').single();
  if (error) throw error;
  await notifyTableReload('daily_metrics');
  return data;
}

function addDaysToIsoDate(value, days = 1) {
  const iso = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;
  const [year, month, day] = iso.split('-').map((n) => Number(n));
  const utc = new Date(Date.UTC(year, (month || 1) - 1, day || 1));
  utc.setUTCDate(utc.getUTCDate() + Number(days || 0));
  const y = utc.getUTCFullYear();
  const m = String(utc.getUTCMonth() + 1).padStart(2, '0');
  const d = String(utc.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function normalizeDailyDocument(value) {
  return String(value || '').replace(/\D+/g, '').trim();
}

function buildDailyRecordId(fecha, documento = null, empleadoId = null) {
  const day = String(fecha || '').trim();
  const doc = normalizeDailyDocument(documento);
  if (day && doc) return `${day}_${doc}`;
  const employee = String(empleadoId || '').trim();
  if (day && employee) return `${day}_${employee}`;
  return `${day}_${crypto.randomUUID()}`;
}

function incapacitySourceToNoveltyCode(source) {
  const raw = String(source || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();
  if (raw.includes('accidente laboral')) return '2';
  if (raw.includes('enfermedad general')) return '3';
  if (raw.includes('calamidad')) return '4';
  if (raw.includes('licencia no remunerada')) return '5';
  return '3';
}

async function propagateIncapacitiesToNextDay(day) {
  const nextDay = addDaysToIsoDate(day, 1);
  if (!nextDay) return;
  if (await isOperationDayClosed(nextDay)) return;

  const { data: incapRows, error: incapError } = await supabase
    .from('incapacitados')
    .select('*')
    .eq('estado', 'activo')
    .lte('fecha_inicio', nextDay)
    .gte('fecha_fin', nextDay);
  if (incapError) throw incapError;

  for (const incap of incapRows || []) {
    const employeeId = incap.employee_id || null;
    if (!employeeId) continue;

    const { data: employee, error: employeeError } = await supabase
      .from('employees')
      .select('*')
      .eq('id', employeeId)
      .maybeSingle();
    if (employeeError) throw employeeError;
    if (!employee) continue;

    const normalizedDocument = normalizeDailyDocument(employee.documento);
    if (!normalizedDocument) continue;

    const { data: existingAttendance, error: existingAttendanceError } = await supabase
      .from('attendance')
      .select('id')
      .eq('fecha', nextDay)
      .eq('documento', normalizedDocument)
      .limit(1)
      .maybeSingle();
    if (existingAttendanceError) throw existingAttendanceError;
    if (existingAttendance?.id) continue;

    const noveltyCode = incapacitySourceToNoveltyCode(incap.source);
    const attendanceId = buildDailyRecordId(nextDay, normalizedDocument, employee.id);
    const { error: attendanceError } = await supabase.from('attendance').upsert({
      id: attendanceId,
      fecha: nextDay,
      empleado_id: employee.id,
      documento: normalizedDocument,
      nombre: employee.nombre || null,
      sede_codigo: employee.sede_codigo || null,
      sede_nombre: employee.sede_nombre || null,
      asistio: false,
      novedad: noveltyCode
    }, { onConflict: 'id' });
    if (attendanceError) throw attendanceError;

    const { error: absenteeismError } = await supabase.from('absenteeism').upsert({
      id: attendanceId,
      fecha: nextDay,
      empleado_id: employee.id,
      documento: normalizedDocument,
      nombre: employee.nombre || null,
      sede_codigo: employee.sede_codigo || null,
      sede_nombre: employee.sede_nombre || null,
      estado: 'programado_incapacidad'
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
  }

  await recomputeDailyMetrics(nextDay);
  await notifyTableReload('attendance');
  await notifyTableReload('absenteeism');
}

async function getNextPrefixedCode(table, prefix, width = 4) {
  const { data, error } = await supabase
    .from(table)
    .select('codigo');
  if (error) throw error;
  let max = 0;
  (data || []).forEach((row) => {
    const code = String(row?.codigo || '').trim();
    const match = code.match(new RegExp(`^${prefix}-(\\d+)$`));
    if (!match) return;
    const num = Number(match[1] || 0);
    if (num > max) max = num;
  });
  return `${prefix}-${String(max + 1).padStart(width, '0')}`;
}

function streamTable(table, mapper, onData, order = 'created_at') {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from(table)
      .select('*')
      .order(order, { ascending: false });
    if (!active) return;
    if (error) {
      console.error(`No se pudo cargar ${table}:`, error);
      onData([]);
      return;
    }
    onData((data || []).map((row) => mapper(row)));
  };

  emit();
  const unregister = registerTableReloader(table, emit);

  const channel = supabase
    .channel(`${table}-watch`)
    .on('postgres_changes', { event: '*', schema: 'public', table }, emit)
    .subscribe();

  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export const authState = (cb) => {
  supabase.auth.getSession().then(({ data, error }) => {
    if (error) {
      console.error('No se pudo consultar la sesion de Supabase:', error);
      cb(null);
      return;
    }
    cb(normalizeUser(data.session?.user || null));
  });

  const { data } = supabase.auth.onAuthStateChange((_event, session) => {
    cb(normalizeUser(session?.user || null));
  });

  return () => data.subscription.unsubscribe();
};

export async function login(email, pass) {
  const { data, error } = await supabase.auth.signInWithPassword({
    email: String(email || '').trim(),
    password: pass
  });
  if (error) throw error;
  return { user: normalizeUser(data.user) };
}

export async function register(email, pass) {
  const cleanEmail = String(email || '').trim().toLowerCase();
  const { data, error } = await supabase.auth.signUp({
    email: cleanEmail,
    password: pass
  });
  if (error) throw error;
  return { user: normalizeUser(data.user) };
}

export async function logout() {
  const { error } = await supabase.auth.signOut();
  if (error) throw error;
}

export async function createUserProfile(uid, data) {
  await upsertProfile(uid, data);
}

export async function ensureUserProfile(user) {
  if (!user?.uid) return;
  const existing = await loadUserProfile(user.uid);
  if (existing) return;
  await upsertProfile(user.uid, {
    email: user.email,
    displayName: user.displayName,
    estado: 'activo'
  });
}

export async function loadUserProfile(uid) {
  const { data, error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .select('*')
    .eq('id', uid)
    .maybeSingle();
  if (error) throw error;
  if (!data) return null;
  return {
    uid: data.id,
    email: data.email || '',
    displayName: data.display_name || null,
    documento: data.documento || null,
    estado: data.estado || 'activo',
    role: data.role || null,
    zonaCodigo: data.zona_codigo || null,
    zonasPermitidas: Array.isArray(data.zonas_permitidas) ? data.zonas_permitidas : [],
    supervisorEligible: data.supervisor_eligible === true
  };
}

export async function getUserOverrides() {
  const user = (await supabase.auth.getUser()).data.user;
  if (!user?.id) return {};
  const { data, error } = await supabase
    .from('user_overrides')
    .select('permissions')
    .eq('user_id', user.id)
    .maybeSingle();
  if (error) throw error;
  return data?.permissions || {};
}

export async function setUserOverrides() {
  throw new Error('setUserOverrides aun no esta migrado a Supabase.');
}

export async function clearUserOverrides() {
  throw new Error('clearUserOverrides aun no esta migrado a Supabase.');
}

export function streamRoleMatrix(onData) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('roles_matrix')
      .select('role, permissions');
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar roles_matrix:', error);
      onData({});
      return;
    }
    const map = {};
    (data || []).forEach((row) => {
      map[row.role] = row.permissions || {};
    });
    onData(map);
  };

  emit();

  const channel = supabase
    .channel('roles-matrix-watch')
    .on('postgres_changes', { event: '*', schema: 'public', table: 'roles_matrix' }, emit)
    .subscribe();

  return () => {
    active = false;
    supabase.removeChannel(channel);
  };
}

export function streamUserOverrides(uid, onData) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('user_overrides')
      .select('permissions')
      .eq('user_id', uid)
      .maybeSingle();
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar user_overrides:', error);
      onData({});
      return;
    }
    onData(data?.permissions || {});
  };

  emit();

  const channel = supabase
    .channel(`user-overrides-${uid}`)
    .on('postgres_changes', { event: '*', schema: 'public', table: 'user_overrides', filter: `user_id=eq.${uid}` }, emit)
    .subscribe();

  return () => {
    active = false;
    supabase.removeChannel(channel);
  };
}

export function streamZones(onData) {
  return streamTable('zones', mapCatalogRow, onData);
}

export async function getNextZoneCode(prefix = 'ZON', width = 4) {
  return getNextPrefixedCode('zones', prefix, width);
}

export async function createZone({ codigo, nombre }) {
  const audit = await getCurrentAuditFields();
  const { data, error } = await supabase
    .from('zones')
    .insert({
      codigo: codigo || null,
      nombre: nombre || null,
      estado: 'activo',
      ...audit
    })
    .select('id')
    .single();
  if (error) throw error;
  await notifyTableReload('zones');
  return data.id;
}

export async function updateZone(id, { codigo, nombre }) {
  const patch = {};
  if (typeof codigo === 'string') patch.codigo = codigo;
  if (typeof nombre === 'string') patch.nombre = nombre;
  const { error } = await supabase.from('zones').update(patch).eq('id', id);
  if (error) throw error;
  await notifyTableReload('zones');
}

export async function setZoneStatus(id, estado) {
  const { error } = await supabase.from('zones').update({ estado }).eq('id', id);
  if (error) throw error;
  await notifyTableReload('zones');
}

export async function findZoneByCode(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('zones').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data ? mapCatalogRow(data) : null;
}

export function streamDependencies(onData) {
  return streamTable('dependencies', mapCatalogRow, onData);
}

export async function getNextDependencyCode(prefix = 'DEP', width = 4) {
  return getNextPrefixedCode('dependencies', prefix, width);
}

export async function createDependency({ codigo, nombre }) {
  const audit = await getCurrentAuditFields();
  const { data, error } = await supabase
    .from('dependencies')
    .insert({
      codigo: codigo || null,
      nombre: nombre || null,
      estado: 'activo',
      ...audit
    })
    .select('id')
    .single();
  if (error) throw error;
  await notifyTableReload('dependencies');
  return data.id;
}

export async function updateDependency(id, { codigo, nombre }) {
  const patch = {};
  if (typeof codigo === 'string') patch.codigo = codigo;
  if (typeof nombre === 'string') patch.nombre = nombre;
  const { error } = await supabase.from('dependencies').update(patch).eq('id', id);
  if (error) throw error;
  await notifyTableReload('dependencies');
}

export async function setDependencyStatus(id, estado) {
  const { error } = await supabase.from('dependencies').update({ estado }).eq('id', id);
  if (error) throw error;
  await notifyTableReload('dependencies');
}

export async function findDependencyByCode(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('dependencies').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data ? mapCatalogRow(data) : null;
}

export function streamSedes(onData) {
  return streamTable('sedes', mapSedeRow, onData);
}

export async function getNextSedeCode(prefix = 'SED', width = 4) {
  return getNextPrefixedCode('sedes', prefix, width);
}

export async function createSede({ codigo, nombre, dependenciaCodigo, dependenciaNombre, zonaCodigo, zonaNombre, numeroOperarios, jornada }) {
  const audit = await getCurrentAuditFields();
  const { data, error } = await supabase
    .from('sedes')
    .insert({
      codigo: codigo || null,
      nombre: nombre || null,
      dependencia_codigo: dependenciaCodigo || null,
      dependencia_nombre: dependenciaNombre || null,
      zona_codigo: zonaCodigo || null,
      zona_nombre: zonaNombre || null,
      numero_operarios: typeof numeroOperarios === 'number' ? numeroOperarios : null,
      jornada: jornada || 'lun_vie',
      estado: 'activo',
      ...audit
    })
    .select('id')
    .single();
  if (error) throw error;
  await notifyTableReload('sedes');
  return data.id;
}

export async function createSedesBulk(rows = []) {
  const items = Array.isArray(rows) ? rows.filter(Boolean) : [];
  let created = 0;
  for (const row of items) {
    const codigo = row.codigo || await getNextSedeCode('SED', 4);
    await createSede({
      codigo,
      nombre: row.nombre || null,
      dependenciaCodigo: row.dependenciaCodigo || null,
      dependenciaNombre: row.dependenciaNombre || null,
      zonaCodigo: row.zonaCodigo || null,
      zonaNombre: row.zonaNombre || null,
      numeroOperarios: typeof row.numeroOperarios === 'number' ? row.numeroOperarios : Number(row.numeroOperarios || 0),
      jornada: row.jornada || 'lun_vie'
    });
    created += 1;
  }
  return { created };
}

export async function updateSede(id, { codigo, nombre, dependenciaCodigo, dependenciaNombre, zonaCodigo, zonaNombre, numeroOperarios, jornada }) {
  const patch = {};
  if (typeof codigo === 'string') patch.codigo = codigo;
  if (typeof nombre === 'string') patch.nombre = nombre;
  if (typeof dependenciaCodigo === 'string') patch.dependencia_codigo = dependenciaCodigo;
  if (typeof dependenciaNombre === 'string') patch.dependencia_nombre = dependenciaNombre;
  if (typeof zonaCodigo === 'string') patch.zona_codigo = zonaCodigo;
  if (typeof zonaNombre === 'string') patch.zona_nombre = zonaNombre;
  if (typeof numeroOperarios === 'number') patch.numero_operarios = numeroOperarios;
  if (typeof jornada === 'string') patch.jornada = jornada;
  const { error } = await supabase.from('sedes').update(patch).eq('id', id);
  if (error) throw error;
  await notifyTableReload('sedes');
}

export async function setSedeStatus(id, estado) {
  const { error } = await supabase.from('sedes').update({ estado }).eq('id', id);
  if (error) throw error;
  await notifyTableReload('sedes');
}

export async function findSedeByCode(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('sedes').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data ? mapSedeRow(data) : null;
}

export function streamCargos(onData) {
  return streamTable('cargos', mapCargoRow, onData);
}

export async function getNextCargoCode(prefix = 'CAR', width = 4) {
  return getNextPrefixedCode('cargos', prefix, width);
}

export async function createCargo({ codigo, nombre, alineacionCrud }) {
  const audit = await getCurrentAuditFields();
  const { data, error } = await supabase
    .from('cargos')
    .insert({
      codigo: codigo || null,
      nombre: nombre || null,
      alineacion_crud: alineacionCrud || 'empleado',
      estado: 'activo',
      ...audit
    })
    .select('id')
    .single();
  if (error) throw error;
  await notifyTableReload('cargos');
  return data.id;
}

export async function updateCargo(id, { codigo, nombre, alineacionCrud }) {
  const patch = {};
  if (typeof codigo === 'string') patch.codigo = codigo;
  if (typeof nombre === 'string') patch.nombre = nombre;
  if (typeof alineacionCrud === 'string') patch.alineacion_crud = alineacionCrud;
  const { error } = await supabase.from('cargos').update(patch).eq('id', id);
  if (error) throw error;
  await notifyTableReload('cargos');
}

export async function setCargoStatus(id, estado) {
  const { error } = await supabase.from('cargos').update({ estado }).eq('id', id);
  if (error) throw error;
  await notifyTableReload('cargos');
}

export async function findCargoByCode(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('cargos').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data ? mapCargoRow(data) : null;
}

export function streamNovedades(onData) {
  return streamTable('novedades', mapNovedadRow, onData);
}

export async function getNextNovedadCode(prefix = 'NOV', width = 4) {
  return getNextPrefixedCode('novedades', prefix, width);
}

export async function createNovedad({ codigo, codigoNovedad, nombre, reemplazo, nomina }) {
  const audit = await getCurrentAuditFields();
  const { data, error } = await supabase
    .from('novedades')
    .insert({
      codigo: codigo || null,
      codigo_novedad: codigoNovedad || null,
      nombre: nombre || null,
      reemplazo: reemplazo || null,
      nomina: nomina || null,
      estado: 'activo',
      ...audit
    })
    .select('id')
    .single();
  if (error) throw error;
  await notifyTableReload('novedades');
  return data.id;
}

export async function updateNovedad(id, { codigo, codigoNovedad, nombre, reemplazo, nomina }) {
  const patch = {};
  if (typeof codigo === 'string') patch.codigo = codigo;
  if (typeof codigoNovedad === 'string') patch.codigo_novedad = codigoNovedad;
  if (typeof nombre === 'string') patch.nombre = nombre;
  if (typeof reemplazo === 'string') patch.reemplazo = reemplazo;
  if (typeof nomina === 'string') patch.nomina = nomina;
  const { error } = await supabase.from('novedades').update(patch).eq('id', id);
  if (error) throw error;
  await notifyTableReload('novedades');
}

export async function setNovedadStatus(id, estado) {
  const { error } = await supabase.from('novedades').update({ estado }).eq('id', id);
  if (error) throw error;
  await notifyTableReload('novedades');
}

export async function findNovedadByCode(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('novedades').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data ? mapNovedadRow(data) : null;
}

export async function findNovedadByCodigoNovedad(codigoNovedad) {
  if (!codigoNovedad) return null;
  const { data, error } = await supabase.from('novedades').select('*').eq('codigo_novedad', codigoNovedad).maybeSingle();
  if (error) throw error;
  return data ? mapNovedadRow(data) : null;
}

export function streamEmployees(onData) {
  return streamTable('employees', mapEmployeeRow, onData);
}

export async function getNextEmployeeCode(prefix = 'EMP', width = 4) {
  return getNextPrefixedCode('employees', prefix, width);
}

export async function createEmployee({ codigo, documento, nombre, telefono, cargoCodigo, cargoNombre, sedeCodigo, sedeNombre, fechaIngreso }) {
  const audit = await getCurrentAuditFields();
  const zone = await resolveZoneBySedeCode(sedeCodigo);
  const { data, error } = await supabase
    .from('employees')
    .insert({
      codigo: codigo || null,
      documento: String(documento || '').trim() || null,
      nombre: nombre || null,
      telefono: telefono || null,
      cargo_codigo: cargoCodigo || null,
      cargo_nombre: cargoNombre || null,
      sede_codigo: sedeCodigo || null,
      sede_nombre: sedeNombre || null,
      zona_codigo: zone.zonaCodigo || null,
      zona_nombre: zone.zonaNombre || null,
      fecha_ingreso: fechaIngreso || null,
      fecha_retiro: null,
      estado: 'activo',
      created_by_uid: audit.created_by_uid,
      created_by_email: audit.created_by_email,
      last_modified_by_uid: audit.created_by_uid,
      last_modified_by_email: audit.created_by_email,
      last_modified_at: new Date().toISOString()
    })
    .select('*')
    .single();
  if (error) throw error;
  await appendEmployeeCargoHistory({
    employeeId: data.id,
    employeeCodigo: data.codigo,
    documento: data.documento,
    cargoCodigo: data.cargo_codigo,
    cargoNombre: data.cargo_nombre,
    fechaIngreso: data.fecha_ingreso,
    source: 'create_employee'
  });
  await notifyTableReload('employees');
  return data.id;
}

function normalizeBulkPhone(value) {
  const digits = String(value || '').replace(/\D+/g, '').trim();
  if (!digits) return null;
  if (digits.startsWith('57') && digits.length >= 12) return digits.slice(0, 12);
  if (digits.length === 10) return `57${digits}`;
  return digits;
}

function normalizeBulkDate(value) {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value.toISOString();
  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return `${raw}T00:00:00`;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

export async function createEmployeesBulk(rows = []) {
  const items = Array.isArray(rows) ? rows.filter(Boolean) : [];
  let created = 0;
  for (const row of items) {
    const codigo = row.codigo || await getNextEmployeeCode('EMP', 4);
    await createEmployee({
      codigo,
      documento: String(row.documento || '').trim(),
      nombre: row.nombre || null,
      telefono: normalizeBulkPhone(row.telefono),
      cargoCodigo: row.cargoCodigo || null,
      cargoNombre: row.cargoNombre || null,
      sedeCodigo: row.sedeCodigo || null,
      sedeNombre: row.sedeNombre || null,
      fechaIngreso: normalizeBulkDate(row.fechaIngreso)
    });
    created += 1;
  }
  return { created };
}

export async function updateEmployee(id, data = {}) {
  const audit = await getCurrentAuditFields();
  const current = await supabase.from('employees').select('*').eq('id', id).single();
  if (current.error) throw current.error;
  const currentRow = current.data;
  const patch = {
    last_modified_by_uid: audit.created_by_uid,
    last_modified_by_email: audit.created_by_email,
    last_modified_at: new Date().toISOString()
  };
  if (typeof data.codigo === 'string') patch.codigo = data.codigo;
  if (typeof data.documento === 'string') patch.documento = data.documento;
  if (typeof data.nombre === 'string') patch.nombre = data.nombre;
  if (typeof data.telefono === 'string') patch.telefono = data.telefono;
  if (typeof data.cargoCodigo === 'string') patch.cargo_codigo = data.cargoCodigo;
  if (typeof data.cargoNombre === 'string') patch.cargo_nombre = data.cargoNombre;
  if (typeof data.sedeCodigo === 'string') {
    const zone = await resolveZoneBySedeCode(data.sedeCodigo);
    patch.sede_codigo = data.sedeCodigo;
    patch.sede_nombre = typeof data.sedeNombre === 'string' ? data.sedeNombre : null;
    patch.zona_codigo = zone.zonaCodigo || null;
    patch.zona_nombre = zone.zonaNombre || null;
  }
  if (data.fechaIngreso !== undefined) patch.fecha_ingreso = data.fechaIngreso || null;
  if (data.fechaRetiro !== undefined) patch.fecha_retiro = data.fechaRetiro || null;
  const { data: updated, error } = await supabase.from('employees').update(patch).eq('id', id).select('*').single();
  if (error) throw error;
  const cargoChanged =
    patch.cargo_codigo !== undefined &&
    String(patch.cargo_codigo || '') !== String(currentRow.cargo_codigo || '');
  if (cargoChanged) {
    await appendEmployeeCargoHistory({
      employeeId: updated.id,
      employeeCodigo: updated.codigo,
      documento: updated.documento,
      cargoCodigo: updated.cargo_codigo,
      cargoNombre: updated.cargo_nombre,
      fechaIngreso: updated.fecha_ingreso || new Date().toISOString(),
      fechaRetiro: updated.fecha_retiro || null,
      source: 'cargo_change'
    });
  }
  if (await getCargoCrudAlignmentByCode(updated.cargo_codigo, updated.cargo_nombre) === 'supervisor') {
    await upsertSupervisorProfileFromEmployee(mapEmployeeRow(updated));
  }
  await notifyTableReload('employees');
}

export async function setEmployeeStatus(id, estado, fechaRetiro = null) {
  const audit = await getCurrentAuditFields();
  const patch = {
    estado,
    fecha_retiro: estado === 'inactivo' ? (fechaRetiro || new Date().toISOString()) : null,
    last_modified_by_uid: audit.created_by_uid,
    last_modified_by_email: audit.created_by_email,
    last_modified_at: new Date().toISOString()
  };
  const { data, error } = await supabase.from('employees').update(patch).eq('id', id).select('*').single();
  if (error) throw error;
  if (await getCargoCrudAlignmentByCode(data.cargo_codigo, data.cargo_nombre) === 'supervisor') {
    await upsertSupervisorProfileFromEmployee(mapEmployeeRow(data), {
      estado,
      fechaRetiro: patch.fecha_retiro
    });
  }
  await notifyTableReload('employees');
}

export async function findEmployeeByCode(codigo) {
  if (!codigo) return null;
  const { data, error } = await supabase.from('employees').select('*').eq('codigo', codigo).maybeSingle();
  if (error) throw error;
  return data ? mapEmployeeRow(data) : null;
}

export async function findEmployeeByDocument(documento) {
  if (!documento) return null;
  const { data, error } = await supabase.from('employees').select('*').eq('documento', documento).maybeSingle();
  if (error) throw error;
  return data ? mapEmployeeRow(data) : null;
}

export function streamEmployeeCargoHistory(employeeId, onData) {
  const empId = String(employeeId || '').trim();
  if (!empId) {
    onData([]);
    return () => {};
  }
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('employee_cargo_history')
      .select('*')
      .eq('employee_id', empId)
      .order('fecha_ingreso', { ascending: false });
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar historial de cargos:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapCargoHistoryRow));
  };
  emit();
  const unregister = registerTableReloader('employee_cargo_history', emit);
  const channel = supabase
    .channel(`employee-cargo-history-${empId}`)
    .on('postgres_changes', { event: '*', schema: 'public', table: 'employee_cargo_history', filter: `employee_id=eq.${empId}` }, emit)
    .subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamSupernumerarios(onData) {
  let active = true;
  const emit = async () => {
    const [{ data: employees, error: empError }, { data: cargos, error: cargoError }] = await Promise.all([
      supabase.from('employees').select('*').order('created_at', { ascending: false }),
      supabase.from('cargos').select('codigo, nombre, alineacion_crud')
    ]);
    if (!active) return;
    if (empError || cargoError) {
      console.error('No se pudieron cargar supernumerarios:', empError || cargoError);
      onData([]);
      return;
    }
    const cargoMap = new Map((cargos || []).map((row) => [String(row.codigo || ''), row]));
    const rows = (employees || [])
      .filter((emp) => {
        const cargo = cargoMap.get(String(emp.cargo_codigo || '')) || null;
        const alignment = normalizeCargoAlignment(cargo?.alineacion_crud || emp.cargo_nombre);
        return alignment === 'supernumerario';
      })
      .map((row) => mapEmployeeRow(row));
    onData(rows);
  };
  emit();
  const unA = registerTableReloader('employees', emit);
  const unB = registerTableReloader('cargos', emit);
  const channelA = supabase.channel('supernumerarios-employees-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'employees' }, emit).subscribe();
  const channelB = supabase.channel('supernumerarios-cargos-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'cargos' }, emit).subscribe();
  return () => {
    active = false;
    unA();
    unB();
    supabase.removeChannel(channelA);
    supabase.removeChannel(channelB);
  };
}

export async function getNextSupernumerarioCode(prefix = 'SUPN', width = 4) {
  return getNextPrefixedCode('employees', prefix, width);
}

export async function createSupernumerario(payload) {
  return createEmployee(payload);
}

export async function createSupernumerariosBulk(rows = []) {
  const items = Array.isArray(rows) ? rows.filter(Boolean) : [];
  let created = 0;
  for (const row of items) {
    const alignment = await getCargoCrudAlignmentByCode(row.cargoCodigo, row.cargoNombre);
    if (alignment !== 'supernumerario') {
      throw new Error(`El cargo ${row.cargoCodigo || row.cargoNombre || '-'} no está alineado como supernumerario.`);
    }
    const codigo = row.codigo || await getNextSupernumerarioCode('SUPN', 4);
    await createSupernumerario({
      codigo,
      documento: String(row.documento || '').trim(),
      nombre: row.nombre || null,
      telefono: normalizeBulkPhone(row.telefono),
      cargoCodigo: row.cargoCodigo || null,
      cargoNombre: row.cargoNombre || null,
      sedeCodigo: row.sedeCodigo || null,
      sedeNombre: row.sedeNombre || null,
      fechaIngreso: normalizeBulkDate(row.fechaIngreso)
    });
    created += 1;
  }
  return { created };
}

export async function updateSupernumerario(id, data = {}) {
  return updateEmployee(id, data);
}

export async function setSupernumerarioStatus(id, estado, fechaRetiro = null) {
  return setEmployeeStatus(id, estado, fechaRetiro);
}

export async function findSupernumerarioByCode(codigo) {
  const row = await findEmployeeByCode(codigo);
  if (!row) return null;
  const alignment = await getCargoCrudAlignmentByCode(row.cargoCodigo, row.cargoNombre);
  return alignment === 'supernumerario' ? row : null;
}

export async function findSupernumerarioByDocument(documento) {
  const row = await findEmployeeByDocument(documento);
  if (!row) return null;
  const alignment = await getCargoCrudAlignmentByCode(row.cargoCodigo, row.cargoNombre);
  return alignment === 'supernumerario' ? row : null;
}

export function streamSupervisors(onData) {
  let active = true;
  const emit = async () => {
    const [
      { data: employees, error: empError },
      { data: profiles, error: profileError },
      { data: cargos, error: cargoError }
    ] = await Promise.all([
      supabase.from('employees').select('*').order('created_at', { ascending: false }),
      supabase.from('supervisor_profile').select('*').order('created_at', { ascending: false }),
      supabase.from('cargos').select('codigo, nombre, alineacion_crud')
    ]);

    if (!active) return;
    if (empError || profileError || cargoError) {
      console.error('No se pudieron cargar supervisores:', empError || profileError || cargoError);
      onData([]);
      return;
    }

    const cargoMap = new Map((cargos || []).map((row) => [String(row.codigo || ''), row]));
    const profileByDoc = mapByDocument((profiles || []).map(mapSupervisorProfileRow));

    const rows = (employees || [])
      .filter((emp) => {
        const cargo = cargoMap.get(String(emp.cargo_codigo || '')) || null;
        const alignment = normalizeCargoAlignment(cargo?.alineacion_crud || emp.cargo_nombre);
        return alignment === 'supervisor';
      })
      .map((emp) => {
        const base = mapEmployeeRow(emp);
        const documento = String(base.documento || '').trim();
        const profile = profileByDoc.get(documento) || {};
        const cargo = cargoMap.get(String(base.cargoCodigo || '')) || null;
        return {
          id: base.id,
          profileId: profile.profileId || null,
          codigo: base.codigo || null,
          documento: documento || null,
          nombre: base.nombre || null,
          cargoCodigo: base.cargoCodigo || profile.cargoCodigo || null,
          cargoNombre: cargo?.nombre || base.cargoNombre || profile.cargoNombre || null,
          zonaCodigo: profile.zonaCodigo || base.zonaCodigo || null,
          zonaNombre: profile.zonaNombre || base.zonaNombre || null,
          fechaIngreso: base.fechaIngreso || profile.fechaIngreso || null,
          fechaRetiro: base.fechaRetiro || profile.fechaRetiro || null,
          estado: base.estado || profile.estado || 'activo',
          createdAt: profile.createdAt || base.createdAt || null,
          createdByUid: profile.createdByUid || base.createdByUid || null,
          createdByEmail: profile.createdByEmail || base.createdByEmail || null,
          lastModifiedAt: profile.lastModifiedAt || base.lastModifiedAt || null,
          lastModifiedByUid: profile.lastModifiedByUid || base.lastModifiedByUid || null,
          lastModifiedByEmail: profile.lastModifiedByEmail || base.lastModifiedByEmail || null
        };
      });

    onData(rows);
  };

  emit();
  const unA = registerTableReloader('employees', emit);
  const unB = registerTableReloader('supervisor_profile', emit);
  const unC = registerTableReloader('cargos', emit);
  const channelA = supabase.channel('supervisors-employees-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'employees' }, emit).subscribe();
  const channelB = supabase.channel('supervisors-profile-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'supervisor_profile' }, emit).subscribe();
  const channelC = supabase.channel('supervisors-cargos-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'cargos' }, emit).subscribe();

  return () => {
    active = false;
    unA();
    unB();
    unC();
    supabase.removeChannel(channelA);
    supabase.removeChannel(channelB);
    supabase.removeChannel(channelC);
  };
}

export async function getNextSupervisorCode(prefix = 'SUP', width = 4) {
  return getNextPrefixedCode('employees', prefix, width);
}

export async function createSupervisor({ codigo, documento, nombre, zonaCodigo, zonaNombre, fechaIngreso }) {
  const employee = await findEmployeeByDocument(documento);
  if (!employee) throw new Error('No existe empleado con ese documento.');
  const profile = await upsertSupervisorProfileFromEmployee(employee, {
    codigo: codigo || employee.codigo || null,
    documento: documento || employee.documento || null,
    nombre: nombre || employee.nombre || null,
    zonaCodigo: zonaCodigo || employee.zonaCodigo || null,
    zonaNombre: zonaNombre || employee.zonaNombre || null,
    fechaIngreso: fechaIngreso || employee.fechaIngreso || null,
    estado: employee.estado || 'activo'
  });
  return profile.employee_id || employee.id;
}

export async function updateSupervisor(id, data = {}) {
  const employee = await supabase.from('employees').select('*').eq('id', id).single();
  if (employee.error) throw employee.error;
  await upsertSupervisorProfileFromEmployee(mapEmployeeRow(employee.data), {
    zonaCodigo: typeof data.zonaCodigo === 'string' ? data.zonaCodigo : undefined,
    zonaNombre: typeof data.zonaNombre === 'string' ? data.zonaNombre : undefined
  });
}

export async function setSupervisorStatus(id, estado, fechaRetiro = null, opts = {}) {
  if (opts?.syncEmployee === false) {
    const employee = await supabase.from('employees').select('*').eq('id', id).single();
    if (employee.error) throw employee.error;
    await upsertSupervisorProfileFromEmployee(mapEmployeeRow(employee.data), {
      estado,
      fechaRetiro: estado === 'inactivo' ? (fechaRetiro || new Date().toISOString()) : null
    });
    return;
  }
  await setEmployeeStatus(id, estado, fechaRetiro);
}

export async function findSupervisorByCode(codigo) {
  if (!codigo) return null;
  const employee = await findEmployeeByCode(codigo);
  if (!employee) return null;
  const alignment = await getCargoCrudAlignmentByCode(employee.cargoCodigo, employee.cargoNombre);
  if (alignment !== 'supervisor') return null;
  const { data, error } = await supabase.from('supervisor_profile').select('*').eq('documento', employee.documento).maybeSingle();
  if (error) throw error;
  const profile = data ? mapSupervisorProfileRow(data) : {};
  return {
    id: employee.id,
    profileId: profile.profileId || null,
    codigo: employee.codigo || null,
    documento: employee.documento || null,
    nombre: employee.nombre || null,
    zonaCodigo: profile.zonaCodigo || employee.zonaCodigo || null,
    zonaNombre: profile.zonaNombre || employee.zonaNombre || null,
    estado: employee.estado || profile.estado || 'activo',
    fechaIngreso: employee.fechaIngreso || profile.fechaIngreso || null,
    fechaRetiro: employee.fechaRetiro || profile.fechaRetiro || null
  };
}

export async function findSupervisorByDocument(documento) {
  if (!documento) return null;
  const employee = await findEmployeeByDocument(documento);
  if (!employee) return null;
  const alignment = await getCargoCrudAlignmentByCode(employee.cargoCodigo, employee.cargoNombre);
  if (alignment !== 'supervisor') return null;
  const { data, error } = await supabase.from('supervisor_profile').select('*').eq('documento', documento).maybeSingle();
  if (error) throw error;
  const profile = data ? mapSupervisorProfileRow(data) : {};
  return {
    id: employee.id,
    profileId: profile.profileId || null,
    codigo: employee.codigo || null,
    documento: employee.documento || null,
    nombre: employee.nombre || null,
    zonaCodigo: profile.zonaCodigo || employee.zonaCodigo || null,
    zonaNombre: profile.zonaNombre || employee.zonaNombre || null,
    estado: employee.estado || profile.estado || 'activo',
    fechaIngreso: employee.fechaIngreso || profile.fechaIngreso || null,
    fechaRetiro: employee.fechaRetiro || profile.fechaRetiro || null
  };
}

export function streamImportHistory(onData, max = 200) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('import_history')
      .select('*')
      .order('ts', { ascending: false })
      .limit(max);
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar import_history:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapImportHistoryRow));
  };
  emit();
  const unregister = registerTableReloader('import_history', emit);
  const channel = supabase.channel('import-history-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'import_history' }, emit).subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamDailyClosures(onData, max = 200) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('daily_closures')
      .select('*')
      .order('fecha', { ascending: false })
      .limit(max);
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar daily_closures:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapDailyClosureRow));
  };
  emit();
  const unregister = registerTableReloader('daily_closures', emit);
  const channel = supabase.channel('daily-closures-watch').on('postgres_changes', { event: '*', schema: 'public', table: 'daily_closures' }, emit).subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamAttendanceByDate(fecha, onData) {
  const day = String(fecha || '').trim();
  if (!day) {
    onData([]);
    return () => {};
  }
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase.from('attendance').select('*').eq('fecha', day);
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar attendance por fecha:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapAttendanceRow));
  };
  emit();
  const unregister = registerTableReloader('attendance', emit);
  const channel = supabase.channel(`attendance-${day}`).on('postgres_changes', { event: '*', schema: 'public', table: 'attendance', filter: `fecha=eq.${day}` }, emit).subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamAttendanceRecent(onData, max = 300) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase.from('attendance').select('*').order('created_at', { ascending: false }).limit(max);
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar attendance reciente:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapAttendanceRow));
  };
  emit();
  const unregister = registerTableReloader('attendance', emit);
  const channel = supabase.channel('attendance-recent').on('postgres_changes', { event: '*', schema: 'public', table: 'attendance' }, emit).subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamImportReplacementsByDate(fecha, onData) {
  const day = String(fecha || '').trim();
  if (!day) {
    onData([]);
    return () => {};
  }
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase.from('import_replacements').select('*').eq('fecha', day);
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar import_replacements por fecha:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapImportReplacementRow));
  };
  emit();
  const unregister = registerTableReloader('import_replacements', emit);
  const channel = supabase.channel(`import-replacements-${day}`).on('postgres_changes', { event: '*', schema: 'public', table: 'import_replacements', filter: `fecha=eq.${day}` }, emit).subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamDailyMetricsByDate(fecha, onData) {
  const day = String(fecha || '').trim();
  if (!day) {
    onData(null);
    return () => {};
  }
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase.from('daily_metrics').select('*').eq('fecha', day).maybeSingle();
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar daily_metrics por fecha:', error);
      onData(null);
      return;
    }
    onData(data ? mapDailyMetricsRow(data) : null);
  };
  emit();
  const unregister = registerTableReloader('daily_metrics', emit);
  const channel = supabase.channel(`daily-metrics-${day}`).on('postgres_changes', { event: '*', schema: 'public', table: 'daily_metrics', filter: `fecha=eq.${day}` }, emit).subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamIncapacitadosByDate(fecha, onData) {
  const day = String(fecha || '').trim();
  if (!day) {
    onData([]);
    return () => {};
  }
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('incapacitados')
      .select('*')
      .eq('estado', 'activo')
      .lte('fecha_inicio', day)
      .gte('fecha_fin', day)
      .order('fecha_inicio', { ascending: false });
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar incapacitados por fecha:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapIncapacidadRow));
  };
  emit();
  const unregister = registerTableReloader('incapacitados', emit);
  const channel = supabase
    .channel(`incapacitados-${day}`)
    .on('postgres_changes', { event: '*', schema: 'public', table: 'incapacitados' }, emit)
    .subscribe();
  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamDashboardAttendanceByDate(fecha, onData) {
  return streamAttendanceByDate(fecha, onData);
}

export function streamDashboardReplacementsByDate(fecha, onData) {
  return streamImportReplacementsByDate(fecha, onData);
}

export async function listSedeStatusRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('sede_status')
    .select('*')
    .gte('fecha', dateFrom)
    .lte('fecha', dateTo)
    .order('fecha', { ascending: true });
  if (error) throw error;
  return (data || []).map(mapSedeStatusRow);
}

export async function listAttendanceRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('attendance')
    .select('*')
    .gte('fecha', dateFrom)
    .lte('fecha', dateTo)
    .order('fecha', { ascending: true });
  if (error) throw error;
  return (data || []).map(mapAttendanceRow);
}

export async function listImportReplacementsRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('import_replacements')
    .select('*')
    .gte('fecha', dateFrom)
    .lte('fecha', dateTo)
    .order('fecha', { ascending: true });
  if (error) throw error;
  return (data || []).map(mapImportReplacementRow);
}

export async function listDailyMetricsRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('daily_metrics')
    .select('*')
    .gte('fecha', dateFrom)
    .lte('fecha', dateTo)
    .order('fecha', { ascending: true });
  if (error) throw error;
  return (data || []).map(mapDailyMetricsRow);
}

export async function isOperationDayClosed(fecha) {
  const day = String(fecha || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return false;
  const { data, error } = await supabase.from('daily_closures').select('*').eq('fecha', day).maybeSingle();
  if (error) throw error;
  if (!data) return false;
  return data.locked === true || String(data.status || '').trim() === 'closed';
}

export async function listClosedOperationDaysRange(dateFrom, dateTo) {
  const rows = await listDailyClosuresRange(dateFrom, dateTo);
  return rows
    .filter((row) => row.locked === true || String(row.status || '').trim() === 'closed')
    .map((row) => String(row.fecha || row.id || '').trim())
    .filter(Boolean)
    .sort();
}

export async function listDailyClosuresRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('daily_closures')
    .select('*')
    .gte('fecha', dateFrom)
    .lte('fecha', dateTo)
    .order('fecha', { ascending: true });
  if (error) throw error;
  return (data || []).map(mapDailyClosureRow);
}

export async function confirmImportOperation(payload) {
  const data = payload || {};
  const day = String(data.fechaOperacion || '').trim();
  if (day) {
    const { data: existing, error: existingError } = await supabase
      .from('import_history')
      .select('id')
      .eq('fecha_operacion', day)
      .limit(1);
    if (existingError) throw existingError;
    if ((existing || []).length) throw new Error('Ya existe una confirmacion para esa fecha.');
  }

  const audit = await getCurrentAuditFields();
  const { data: importRow, error: importError } = await supabase
    .from('import_history')
    .insert({
      fecha_operacion: day || null,
      source: data.source || null,
      planned_count: data.plannedCount || 0,
      expected_count: data.expectedCount || 0,
      found_count: data.foundCount || 0,
      missing_count: data.missingCount || 0,
      extra_count: data.extraCount || 0,
      missing_supervisors_count: data.missingSupervisorsCount || 0,
      missing_supernumerarios_count: data.missingSupernumerariosCount || 0,
      missing_docs: data.missingDocs || [],
      extra_docs: data.extraDocs || [],
      missing_supervisors: data.missingSupervisors || [],
      missing_supernumerarios: data.missingSupernumerarios || [],
      errores: data.errores || [],
      confirmado_por_uid: audit.created_by_uid,
      confirmado_por_email: audit.created_by_email
    })
    .select('*')
    .single();
  if (importError) throw importError;

  for (const a of data.attendance || []) {
    if (!a || !a.empleadoId || !a.fecha) continue;
    const dailyId = buildDailyRecordId(a.fecha, a.documento, a.empleadoId);
    const { error } = await supabase.from('attendance').upsert({
      id: dailyId,
      fecha: a.fecha,
      empleado_id: a.empleadoId,
      documento: normalizeDailyDocument(a.documento) || null,
      nombre: a.nombre || null,
      sede_codigo: a.sedeCodigo || null,
      sede_nombre: a.sedeNombre || null,
      asistio: Boolean(a.asistio),
      novedad: a.novedad || null
    }, { onConflict: 'id' });
    if (error) throw error;
  }

  for (const ab of data.absences || []) {
    if (!ab || !ab.empleadoId || !ab.fecha) continue;
    const dailyId = buildDailyRecordId(ab.fecha, ab.documento, ab.empleadoId);
    const { error } = await supabase.from('absenteeism').upsert({
      id: dailyId,
      fecha: ab.fecha,
      empleado_id: ab.empleadoId,
      documento: normalizeDailyDocument(ab.documento) || null,
      nombre: ab.nombre || null,
      sede_codigo: ab.sedeCodigo || null,
      sede_nombre: ab.sedeNombre || null,
      estado: ab.estado || 'pendiente',
      reemplazo_id: ab.reemplazoId || null,
      reemplazo_documento: ab.reemplazoDocumento || null,
      created_by_uid: audit.created_by_uid,
      created_by_email: audit.created_by_email
    }, { onConflict: 'id' });
    if (error) throw error;
  }

  for (const ss of data.sedeStatus || []) {
    if (!ss || !ss.fecha || !ss.sedeCodigo) continue;
    const { error } = await supabase.from('sede_status').upsert({
      id: `${ss.fecha}_${ss.sedeCodigo}`,
      fecha: ss.fecha,
      sede_codigo: ss.sedeCodigo,
      sede_nombre: ss.sedeNombre || null,
      operarios_esperados: ss.operariosEsperados || 0,
      operarios_presentes: ss.operariosPresentes || 0,
      faltantes: ss.faltantes || 0
    }, { onConflict: 'id' });
    if (error) throw error;
  }

  if (day) await recomputeDailyMetrics(day);
  await notifyTableReload('import_history');
  await notifyTableReload('attendance');
  await notifyTableReload('absenteeism');
  await notifyTableReload('sede_status');
  return importRow.id;
}

export async function saveImportReplacements({ importId = null, fechaOperacion = null, assignments = [] } = {}) {
  const data = Array.isArray(assignments) ? assignments.filter(Boolean) : [];
  const fechas = [...new Set(data.map((row) => String(row?.fecha || fechaOperacion || '').trim()).filter(Boolean))];
  for (const f of fechas) {
    if (await isOperationDayClosed(f)) throw new Error(`La fecha ${f} ya esta cerrada y no admite cambios.`);
  }
  const used = new Set();
  const audit = await getCurrentAuditFields();
  for (const a of data) {
    if (a.decision === 'reemplazo') {
      const sid = String(a.supernumerarioId || '').trim();
      if (!sid) throw new Error('Falta supernumerario en una fila de reemplazo.');
      if (used.has(sid)) throw new Error('Un supernumerario no puede asignarse dos veces.');
      used.add(sid);
    }
  }
  for (const a of data) {
    const empId = String(a.empleadoId || '').trim();
    const fecha = String(a.fecha || fechaOperacion || '').trim();
    if (!empId || !fecha) continue;
    const replacementId = buildDailyRecordId(fecha, a.documento, empId);
    const { error } = await supabase.from('import_replacements').upsert({
      id: replacementId,
      import_id: importId || null,
      fecha_operacion: fechaOperacion || fecha,
      fecha,
      empleado_id: a.empleadoId || null,
      documento: a.documento || null,
      nombre: a.nombre || null,
      sede_codigo: a.sedeCodigo || null,
      sede_nombre: a.sedeNombre || null,
      novedad_codigo: a.novedadCodigo || null,
      novedad_nombre: a.novedadNombre || null,
      decision: a.decision || 'ausentismo',
      supernumerario_id: a.supernumerarioId || null,
      supernumerario_documento: a.supernumerarioDocumento || null,
      supernumerario_nombre: a.supernumerarioNombre || null,
      actor_uid: audit.created_by_uid,
      actor_email: audit.created_by_email
    }, { onConflict: 'id' });
    if (error) throw error;
  }
  for (const day of fechas) {
    await recomputeDailyMetrics(day);
  }
  await notifyTableReload('import_replacements');
  return { saved: data.length };
}

export async function closeOperationDayManual(fecha) {
  const day = String(fecha || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error('Fecha invalida.');
  if (await isOperationDayClosed(day)) {
    return { ok: true, results: [{ date: day, status: 'already_closed' }] };
  }
  const metrics = mapDailyMetricsRow((await recomputeDailyMetrics(day)) || {});
  const audit = await getCurrentAuditFields();
  const { error } = await supabase.from('daily_closures').upsert({
    id: day,
    fecha: day,
    status: 'closed',
    locked: true,
    planeados: metrics.planned || 0,
    contratados: metrics.expected || 0,
    ausentismos: metrics.absenteeism || 0,
    pagados: metrics.paidServices || 0,
    no_contratados: metrics.noContracted || 0,
    closed_by_uid: audit.created_by_uid,
    closed_by_email: audit.created_by_email
  }, { onConflict: 'id' });
  if (error) throw error;
  await propagateIncapacitiesToNextDay(day);
  await recomputeDailyMetrics(day);
  await notifyTableReload('daily_closures');
  return { ok: true, results: [{ date: day, status: 'closed' }] };
}

export { supabase };
