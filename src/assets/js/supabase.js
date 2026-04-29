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

const POSTGREST_PAGE_SIZE = 1000;
const tableReloaders = new Map();
const REALTIME_SUBSCRIBE_TIMEOUT_MS = 12000;
const INCAPACITY_SUPPORT_BUCKET = 'incapacidades-soportes';
let realtimeChannelSeq = 0;

async function syncRealtimeAuth(session = null) {
  const token = String(session?.access_token || '').trim();
  if (!token) return;
  try {
    await supabase.realtime.setAuth(token);
  } catch (error) {
    console.error('No se pudo sincronizar auth con Realtime:', error);
  }
}

function registerTableReloader(table, reloader) {
  if (!tableReloaders.has(table)) tableReloaders.set(table, new Set());
  tableReloaders.get(table).add(reloader);
  return () => tableReloaders.get(table)?.delete(reloader);
}

function nextRealtimeChannelName(base) {
  realtimeChannelSeq += 1;
  return `${base}-${realtimeChannelSeq}`;
}

function normalizeRealtimeError(label, status, error = null) {
  if (error instanceof Error) return error;
  const suffix = error?.message || error?.details || String(error || '').trim() || status || 'unknown';
  return new Error(`Realtime ${label}: ${suffix}`);
}

function subscribeToRealtime(channel, {
  label = 'channel',
  onStatus = null,
  onError = null,
  timeoutMs = REALTIME_SUBSCRIBE_TIMEOUT_MS
} = {}) {
  let subscribed = false;
  let failureNotified = false;
  const timeoutId = timeoutMs > 0
    ? setTimeout(() => {
      if (subscribed || failureNotified) return;
      failureNotified = true;
      const timeoutError = normalizeRealtimeError(label, 'TIMED_OUT');
      console.error(`Realtime ${label} timed out before subscribe.`);
      onStatus?.('TIMED_OUT', timeoutError);
      onError?.(timeoutError, 'TIMED_OUT');
    }, timeoutMs)
    : null;

  const subscription = channel.subscribe((status, error) => {
    if (status === 'SUBSCRIBED') {
      subscribed = true;
      if (timeoutId) clearTimeout(timeoutId);
    }
    if (status === 'CLOSED' && timeoutId) clearTimeout(timeoutId);
    if ((status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') && timeoutId) clearTimeout(timeoutId);

    onStatus?.(status, error || null);

    if ((status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') && !failureNotified) {
      failureNotified = true;
      const realtimeError = normalizeRealtimeError(label, status, error);
      console.error(`Realtime ${label} failed with status ${status}:`, error || realtimeError);
      onError?.(realtimeError, status);
    }
  });

  return {
    subscription,
    cancel() {
      if (timeoutId) clearTimeout(timeoutId);
    }
  };
}

function shouldRefreshForDay(payload, day, column = 'fecha') {
  const target = String(day || '').trim();
  if (!target) return true;
  const nextVal = String(payload?.new?.[column] || '').trim();
  const prevVal = String(payload?.old?.[column] || '').trim();
  if (nextVal || prevVal) return nextVal === target || prevVal === target;
  return true;
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

async function selectAllRows(table, {
  select = '*',
  order = null,
  ascending = false
} = {}) {
  const rows = [];
  let from = 0;

  while (true) {
    let query = supabase
      .from(table)
      .select(select)
      .range(from, from + POSTGREST_PAGE_SIZE - 1);

    if (order) {
      query = query.order(order, { ascending });
    }

    const { data, error } = await query;
    if (error) throw error;

    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);

    if (batch.length < POSTGREST_PAGE_SIZE) break;
    from += POSTGREST_PAGE_SIZE;
  }

  return rows;
}

function normalizeUser(user) {
  if (!user) return null;
  return {
    uid: user.id,
    email: user.email || '',
    displayName: user.user_metadata?.display_name || user.user_metadata?.full_name || null,
    documento: user.user_metadata?.documento || null
  };
}

function normalizeProfileRow(uid, data = {}) {
  return {
    id: uid,
    email: String(data.email || '').trim().toLowerCase() || null,
    display_name: data.nombre || data.displayName || null,
    documento: data.documento || null,
    role: data.role || 'empleado',
    estado: data.estado || 'activo',
    updated_at: new Date().toISOString()
  };
}

function mapUserProfileRow(row = {}) {
  return {
    uid: row.id,
    email: row.email || '',
    displayName: row.display_name || null,
    documento: row.documento || null,
    role: row.role || 'empleado',
    estado: row.estado || 'activo',
    zonaCodigo: row.zona_codigo || null,
    zonasPermitidas: Array.isArray(row.zonas_permitidas) ? row.zonas_permitidas : [],
    supervisorEligible: row.supervisor_eligible === true,
    createdAt: row.created_at || null,
    lastModifiedAt: row.updated_at || null,
    createdByUid: row.created_by_uid || null,
    createdByEmail: row.created_by_email || null,
    lastModifiedByUid: row.last_modified_by_uid || null,
    lastModifiedByEmail: row.last_modified_by_email || null,
    deletedAt: row.deleted_at || null,
    deletedByUid: row.deleted_by_uid || null,
    deletedByEmail: row.deleted_by_email || null
  };
}

function sanitizePermissionsRecord(data = {}) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return {};
  return Object.fromEntries(
    Object.entries(data).map(([key, value]) => [key, value === true])
  );
}

function mapAuditLogRow(row = {}) {
  return {
    id: row.id,
    ts: row.created_at || null,
    actorUid: row.actor_uid || null,
    actorEmail: row.actor_email || null,
    targetType: row.target_type || null,
    targetId: row.target_id || null,
    action: row.action || null,
    before: row.before_data || null,
    after: row.after_data || null,
    note: row.note || null
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

function formatHour(value) {
  try {
    const d = value?.toDate ? value.toDate() : value ? new Date(value) : null;
    if (!d || Number.isNaN(d.getTime())) return null;
    return d.toLocaleTimeString('es-CO', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  } catch {
    return null;
  }
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
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
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
  const createdAt = row.created_at || null;
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
    createdAt,
    hora: formatHour(createdAt)
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

function mapEmployeeDailyStatusRow(row = {}) {
  const pagaNomina = row.paga_nomina;
  return {
    id: row.id,
    fecha: row.fecha || null,
    employeeId: row.employee_id || null,
    documento: row.documento || null,
    nombre: row.nombre || null,
    tipoPersonal: row.tipo_personal || 'empleado',
    sedeCodigo: row.sede_codigo || null,
    sedeNombreSnapshot: row.sede_nombre_snapshot || null,
    zonaCodigoSnapshot: row.zona_codigo_snapshot || null,
    zonaNombreSnapshot: row.zona_nombre_snapshot || null,
    dependenciaCodigoSnapshot: row.dependencia_codigo_snapshot || null,
    dependenciaNombreSnapshot: row.dependencia_nombre_snapshot || null,
    estadoDia: row.estado_dia || null,
    asistio: row.asistio === true,
    novedadCodigo: row.novedad_codigo || null,
    novedadNombre: row.novedad_nombre || null,
    requiereReemplazo: row.requiere_reemplazo === true,
    decisionCobertura: row.decision_cobertura || 'no_aplica',
    reemplazaAEmployeeId: row.reemplaza_a_employee_id || null,
    reemplazaADocumento: row.reemplaza_a_documento || null,
    reemplazaANombre: row.reemplaza_a_nombre || null,
    reemplazadoPorEmployeeId: row.reemplazado_por_employee_id || null,
    reemplazadoPorDocumento: row.reemplazado_por_documento || null,
    reemplazadoPorNombre: row.reemplazado_por_nombre || null,
    servicioProgramado: row.servicio_programado === true,
    servicioCubierto: row.servicio_cubierto === true,
    cuentaPagoServicio: row.cuenta_pago_servicio === true,
    cuentaNomina: row.cuenta_nomina !== false,
    pagaNomina: pagaNomina == null ? null : pagaNomina === true,
    motivoNomina: row.motivo_nomina || null,
    sourceAttendanceId: row.source_attendance_id || null,
    sourceReplacementId: row.source_replacement_id || null,
    sourceAbsenteeismId: row.source_absenteeism_id || null,
    sourceIncapacityId: row.source_incapacity_id || null,
    origen: row.origen || 'manual',
    closed: row.closed === true,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
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
    asistencias: Number(row.asistencias || 0),
    ausentismos: Number(row.ausentismos || 0),
    faltan: Number(row.faltan || 0),
    sobran: Number(row.sobran || 0),
    noContratados: Number(row.no_contratados || 0),
    closedByUid: row.closed_by_uid || null,
    closedByEmail: row.closed_by_email || null,
    closedAt: row.closed_at || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapDailySedeClosureRow(row = {}) {
  return {
    id: row.id,
    fecha: row.fecha || null,
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
    zonaCodigo: row.zona_codigo || null,
    zonaNombre: row.zona_nombre || null,
    dependenciaCodigo: row.dependencia_codigo || null,
    dependenciaNombre: row.dependencia_nombre || null,
    planeados: Number(row.planeados || 0),
    contratados: Number(row.contratados || 0),
    registrados: Number(row.registrados || 0),
    faltantes: Number(row.faltantes || 0),
    sobrantes: Number(row.sobrantes || 0),
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
    canalRegistro: row.canal_registro || null,
    soporteUrl: row.soporte_url || null,
    soporteNombre: row.soporte_nombre || null,
    soporteTipo: row.soporte_tipo || null,
    soporteStoragePath: row.soporte_storage_path || null,
    whatsappMessageId: row.whatsapp_message_id || null,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function incapacityOverlapsRange(row = {}, dateFrom = '', dateTo = '') {
  const from = String(dateFrom || '').trim();
  const to = String(dateTo || '').trim();
  const start = String(row?.fechaInicio || row?.fecha_inicio || '').trim();
  const end = String(row?.fechaFin || row?.fecha_fin || start).trim();
  if (!start && !end) return true;
  if (from && end && end < from) return false;
  if (to && start && start > to) return false;
  return true;
}

function sanitizeStoragePathPart(value, fallback = 'general') {
  const clean = String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .trim();
  return clean || fallback;
}

function buildIncapacitySupportPath(file, { documento = '', employeeId = '' } = {}) {
  const rawName = String(file?.name || 'soporte').trim();
  const extensionMatch = rawName.match(/(\.[a-zA-Z0-9]+)$/);
  const extension = extensionMatch ? extensionMatch[1].toLowerCase() : '';
  const safeName = sanitizeStoragePathPart(rawName.replace(/(\.[a-zA-Z0-9]+)?$/, ''), 'soporte');
  const owner = sanitizeStoragePathPart(documento || employeeId || 'sin-documento');
  return `${owner}/${Date.now()}_${crypto.randomUUID()}_${safeName}${extension}`;
}

function buildNovedadReplacementRules(rows = []) {
  const byCode = new Map();
  const byName = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const code = String(row?.codigoNovedad || row?.codigo || '').trim();
    const name = normalizeMetricText(String(row?.nombre || '').trim());
    const replacementRaw = normalizeMetricText(String(row?.reemplazo || '').trim());
    const requiresReplacement = ['si', 'yes', 'true', '1', 'reemplazo'].includes(replacementRaw);
    if (code) byCode.set(code, requiresReplacement);
    if (name) byName.set(name, requiresReplacement);
  });
  return { byCode, byName };
}

function normalizeMetricText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function baseMetricNovedadName(raw) {
  return String(raw || '').replace(/\s*\(.*\)\s*$/, '').trim();
}

function metricAttendanceRequiresReplacement(row = {}, rules = {}) {
  const code = String(row?.novedadCodigo || (/^\d+$/.test(String(row?.novedad || '').trim()) ? String(row?.novedad || '').trim() : '')).trim();
  if (['1', '7'].includes(code)) return false;
  if (['2', '3', '4', '5', '8', '9'].includes(code)) return true;
  if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
  const name = normalizeMetricText(baseMetricNovedadName(row?.novedadNombre || row?.novedad || ''));
  if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
  return false;
}

function metricReplacementKey(row = {}) {
  return `${String(row?.fecha || '').trim()}_${String(row?.empleadoId || row?.employeeId || '').trim()}`;
}

function metricAttendanceCountsAsService(row = {}, replacementMap = new Map(), rules = {}) {
  if (!metricAttendanceRequiresReplacement(row, rules)) return true;
  const replacement = replacementMap.get(metricReplacementKey(row)) || null;
  if (!replacement) return false;
  const decision = String(replacement?.decision || '').trim().toLowerCase();
  const hasSupernumerario = Boolean(replacement?.supernumerarioId || replacement?.supernumerarioDocumento || replacement?.supernumerarioNombre);
  return decision === 'reemplazo' && hasSupernumerario;
}

function metricAttendanceCountsAsAbsenteeism(row = {}, replacementMap = new Map(), rules = {}) {
  if (!metricAttendanceRequiresReplacement(row, rules)) return false;
  const replacement = replacementMap.get(metricReplacementKey(row)) || null;
  if (!replacement) return true;
  const decision = String(replacement?.decision || '').trim().toLowerCase();
  return decision !== 'reemplazo';
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

function toISODate(value) {
  if (!value) return null;
  if (typeof value === 'string') {
    const v = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
    const dt = new Date(v);
    if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
    return null;
  }
  if (typeof value?.toDate === 'function') {
    const dt = value.toDate();
    if (!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
    return null;
  }
  if (value instanceof Date) {
    if (!Number.isNaN(value.getTime())) return value.toISOString().slice(0, 10);
    return null;
  }
  return null;
}

function todayBogotaISO() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

const EMPLOYEE_OPERATIONAL_REFRESH_LOOKBACK_DAYS = 31;

function isEmployeeActiveForDate(emp, selectedDate) {
  const day = toISODate(selectedDate);
  if (!day) return false;
  const ingreso = toISODate(emp?.fechaIngreso || emp?.fecha_ingreso);
  if (ingreso && ingreso > day) return false;
  const retiro = toISODate(emp?.fechaRetiro || emp?.fecha_retiro);
  const estado = String(emp?.estado || 'activo').trim().toLowerCase();
  if (estado === 'eliminado') return false;
  if (estado === 'inactivo') return Boolean(retiro && retiro >= day);
  if (retiro && retiro < day) return false;
  return true;
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
  const iso = toISODate(selectedDate);
  if (!iso) return false;
  const year = Number(iso.slice(0, 4));
  return getColombiaHolidaySet(year).has(iso);
}

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = toISODate(selectedDate);
  if (!iso) return false;
  const [year, month, day] = iso.split('-').map((n) => Number(n));
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
  if (isColombiaHolidayDate(iso)) return false;
  if (jornada === 'lun_sab') return weekday >= 1 && weekday <= 6;
  return weekday >= 1 && weekday <= 5;
}

function isEmployeeExpectedForDate(emp, selectedDate, sedeRows = []) {
  if (!selectedDate) return false;
  const ingreso = toISODate(emp?.fechaIngreso || emp?.fecha_ingreso);
  if (!ingreso || ingreso > selectedDate) return false;
  const retiro = toISODate(emp?.fechaRetiro || emp?.fecha_retiro);
  const estado = String(emp?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') return Boolean(retiro && retiro >= selectedDate);
  if (retiro && retiro < selectedDate) return false;
  const sedeCodigo = String(emp?.sedeCodigo || emp?.sede_codigo || '').trim();
  if (!sedeCodigo) return false;
  const sede = (sedeRows || []).find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
  if (!isSedeScheduledForDate(sede, selectedDate)) return false;
  return true;
}

function isEmployeeSupernumerario(emp, cargoMap = new Map()) {
  const cargoCode = String(emp?.cargoCodigo || emp?.cargo_codigo || '').trim();
  const cargo = cargoMap.get(cargoCode) || null;
  const alignment = normalizeCargoAlignment(cargo?.alineacion_crud || emp?.cargoNombre || emp?.cargo_nombre);
  return alignment === 'supernumerario';
}

function isEmployeeAssignedToActiveSedeOnDate(emp, selectedDate, activeSedeCodes = new Set()) {
  if (!selectedDate) return false;
  const ingreso = toISODate(emp?.fechaIngreso || emp?.fecha_ingreso);
  if (!ingreso || ingreso > selectedDate) return false;
  const retiro = toISODate(emp?.fechaRetiro || emp?.fecha_retiro);
  const estado = String(emp?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') return Boolean(retiro && retiro >= selectedDate);
  if (retiro && retiro < selectedDate) return false;
  const sedeCodigo = String(emp?.sedeCodigo || emp?.sede_codigo || '').trim();
  if (!sedeCodigo) return false;
  if (activeSedeCodes.size && !activeSedeCodes.has(sedeCodigo)) return false;
  const sede = Array.isArray(activeSedeCodes?.rows)
    ? activeSedeCodes.rows.find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null
    : null;
  if (sede && !isSedeScheduledForDate(sede, selectedDate)) return false;
  return true;
}

function dedupeAttendanceRows(rows = []) {
  const unique = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row, idx) => {
    const documento = String(row?.documento || '').trim();
    const empleadoId = String(row?.empleadoId || '').trim();
    const fallback = `${String(row?.nombre || '').trim()}|${String(row?.sedeCodigo || '').trim()}|${String(row?.fecha || '').trim()}|${idx}`;
    const key = documento || empleadoId || fallback;
    if (!key) return;
    if (!unique.has(key)) unique.set(key, row);
  });
  return Array.from(unique.values());
}

function resolveAttendanceSedeCode(attendanceRow = {}, context = {}) {
  const rawSedeCode = String(attendanceRow?.sedeCodigo || '').trim();
  if (context?.dayClosed) return rawSedeCode || null;

  const documento = String(attendanceRow?.documento || '').trim();
  if (documento && context?.superDocs?.has(documento)) return null;

  const empleadoId = String(attendanceRow?.empleadoId || '').trim();
  const employee = (empleadoId && context?.employeeById?.get(empleadoId))
    || (documento && context?.employeeByDoc?.get(documento))
    || null;
  if (!employee) return null;
  if (!isEmployeeAssignedToActiveSedeOnDate(employee, context?.selectedDate, context?.activeSedeCodes || new Set())) return null;
  return String(employee?.sedeCodigo || employee?.sede_codigo || '').trim() || null;
}

async function computeDailyClosureSnapshot(fecha) {
  const day = String(fecha || '').trim();
  if (!day) {
    return { planeados: 0, contratados: 0, registrados: 0, faltan: 0, sobran: 0, ausentismos: 0, noContratados: 0 };
  }

  const [
    { data: statusRows, error: statusError },
    sedesRows
  ] = await Promise.all([
    supabase
      .from('employee_daily_status')
      .select('sede_codigo, tipo_personal, servicio_programado, asistio, cuenta_pago_servicio')
      .eq('fecha', day),
    selectAllRows('sedes', { select: '*' })
  ]);
  if (statusError) throw statusError;

  const mappedRows = (statusRows || []).map(mapEmployeeDailyStatusRow);
  const scheduledRows = mappedRows.filter((row) => row.tipoPersonal === 'empleado' && row.servicioProgramado === true);
  const actualRows = mappedRows.filter((row) => row.asistio === true || row.asistio === false);
  const sedes = (sedesRows || [])
    .map(mapSedeRow)
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));

  const bySede = new Map();
  scheduledRows.forEach((row) => {
    const sedeCode = String(row?.sedeCodigo || '').trim();
    if (!sedeCode) return;
    const bucket = bySede.get(sedeCode) || {
      contratados: 0,
      asistencias: 0
    };
    bucket.contratados += 1;
    if (row.cuentaPagoServicio === true) bucket.asistencias += 1;
    bySede.set(sedeCode, bucket);
  });

  const summary = sedes.reduce((acc, sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planned = Number(sede?.numeroOperarios ?? 0) || 0;
    const counts = bySede.get(sedeCode) || { contratados: 0, asistencias: 0 };
    const ausentismos = computeOperationalAbsenteeism(planned, counts.contratados, counts.asistencias);
    acc.planeados += planned;
    acc.contratados += counts.contratados;
    acc.registrados += counts.asistencias;
    acc.faltan += Math.max(0, planned - counts.contratados);
    acc.sobran += Math.max(0, counts.contratados - planned);
    acc.ausentismos += ausentismos;
    return acc;
  }, {
    planeados: 0,
    contratados: 0,
    registrados: 0,
    faltan: 0,
    sobran: 0,
    ausentismos: 0,
    noContratados: 0
  });

  if (summary.planeados === 0 && summary.contratados === 0 && actualRows.length) {
    summary.registrados = actualRows.filter((row) => row.asistio === true).length;
    summary.ausentismos = 0;
    summary.faltan = 0;
    summary.sobran = actualRows.length;
  }

  summary.noContratados = Math.max(0, summary.planeados - summary.contratados);
  return summary;
}

function computeOperationalAbsenteeism(planeados, contratados, cubiertos) {
  const planned = Math.max(0, Number(planeados || 0));
  const contracted = Math.max(0, Number(contratados || 0));
  const covered = Math.max(0, Number(cubiertos || 0));
  if (planned <= 0) return 0;
  return Math.max(0, Math.min(planned, contracted) - covered);
}

async function computeDailySedeClosureSnapshot(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return [];
  const [{ data: attendance }, { data: replacements }, sedesRows, employeesRows, cargosRows, novedadesRows] = await Promise.all([
    supabase.from('attendance').select('*').eq('fecha', day),
    supabase.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes', { select: '*' }),
    selectAllRows('employees', { select: '*' }),
    selectAllRows('cargos', { select: 'codigo, alineacion_crud, nombre' }),
    selectAllRows('novedades', { select: 'codigo, codigo_novedad, nombre, reemplazo' })
  ]);

  const attRows = (attendance || []).map(mapAttendanceRow);
  const repRows = (replacements || []).map(mapImportReplacementRow);
  const sedes = (sedesRows || [])
    .map(mapSedeRow)
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));
  const activeSedeCodes = new Set(sedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules((novedadesRows || []).map(mapNovedadRow));
  const replacementMap = new Map(repRows.map((row) => [metricReplacementKey(row), row]));
  const replacementSuperDocs = new Set(
    repRows
      .filter((row) => String(row?.decision || '').trim().toLowerCase() === 'reemplazo')
      .map((row) => `${String(row?.fecha || '').trim()}|${String(row?.supernumerarioDocumento || '').trim()}`)
      .filter((value) => !value.endsWith('|'))
  );

  const employeeById = new Map();
  const employeeByDoc = new Map();
  const contractedBySede = new Map();
  const supernumerarioDocs = new Set();
  (employeesRows || []).forEach((emp) => {
    const mapped = mapEmployeeRow(emp);
    const empId = String(mapped?.id || '').trim();
    const doc = String(mapped?.documento || '').trim();
    if (empId) employeeById.set(empId, mapped);
    if (doc) employeeByDoc.set(doc, mapped);
    if (doc && isEmployeeSupernumerario(mapped, cargoMap) && isEmployeeAssignedToActiveSedeOnDate(mapped, day, activeSedeCodes)) {
      supernumerarioDocs.add(doc);
    }
    if (!isEmployeeAssignedToActiveSedeOnDate(mapped, day, activeSedeCodes)) return;
    if (isEmployeeSupernumerario(mapped, cargoMap)) return;
    const sedeCode = String(mapped?.sedeCodigo || '').trim();
    if (!sedeCode) return;
    if (!contractedBySede.has(sedeCode)) contractedBySede.set(sedeCode, new Set());
    contractedBySede.get(sedeCode).add(doc || empId);
  });

  const registeredBySede = new Map();
  dedupeAttendanceRows(attRows).forEach((row) => {
    const doc = String(row?.documento || '').trim();
    if (doc && replacementSuperDocs.has(`${String(row?.fecha || '').trim()}|${doc}`)) return;
    if (doc && supernumerarioDocs.has(doc)) return;
    const empId = String(row?.empleadoId || '').trim();
    const employee = (empId && employeeById.get(empId)) || (doc && employeeByDoc.get(doc)) || null;
    if (isEmployeeSupernumerario(employee, cargoMap)) return;
    const sedeCode = String(row?.sedeCodigo || employee?.sedeCodigo || '').trim();
    if (!sedeCode || !activeSedeCodes.has(sedeCode)) return;
    if (!registeredBySede.has(sedeCode)) registeredBySede.set(sedeCode, new Set());
    registeredBySede.get(sedeCode).add(doc || empId || String(row?.id || '').trim());
  });

  return sedes.map((sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planeados = Number(sede?.numeroOperarios ?? 0) || 0;
    const baseContracted = Number(contractedBySede.get(sedeCode)?.size || 0);
    const registrados = Number(registeredBySede.get(sedeCode)?.size || 0);
    const externalRegistered = Math.max(0, registrados - baseContracted);
    const contratados = Math.min(planeados, baseContracted + externalRegistered);
    const faltantes = Math.max(0, planeados - registrados);
    const sobrantes = Math.max(0, registrados - planeados);
    return {
      id: `${day}_${sedeCode}`,
      fecha: day,
      sede_codigo: sedeCode,
      sede_nombre: sede?.nombre || sedeCode || null,
      zona_codigo: sede?.zonaCodigo || null,
      zona_nombre: sede?.zonaNombre || null,
      dependencia_codigo: sede?.dependenciaCodigo || null,
      dependencia_nombre: sede?.dependenciaNombre || null,
      planeados,
      contratados,
      registrados,
      faltantes,
      sobrantes
    };
  });
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
  sedeCodigo = null,
  sedeNombre = null,
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
    sede_codigo: sedeCodigo || null,
    sede_nombre: sedeNombre || null,
    fecha_ingreso: fechaIngreso || null,
    fecha_retiro: fechaRetiro || null,
    source
  });
  if (error) throw error;
  await notifyTableReload('employee_cargo_history');
}

async function appendEmployeeCargoHistoryBulk(rows = [], notifyReload = true) {
  const items = Array.isArray(rows) ? rows.filter((row) => row?.employee_id) : [];
  if (!items.length) return;
  const { error } = await supabase.from('employee_cargo_history').insert(items);
  if (error) throw error;
  if (notifyReload) {
    await notifyTableReload('employee_cargo_history');
  }
}

async function closeActiveEmployeeHistory(employeeId, fechaRetiro, notifyReload = true) {
  const empId = String(employeeId || '').trim();
  if (!empId || !fechaRetiro) return;
  const { error } = await supabase
    .from('employee_cargo_history')
    .update({ fecha_retiro: fechaRetiro })
    .eq('employee_id', empId)
    .is('fecha_retiro', null);
  if (error) throw error;
  if (notifyReload) {
    await notifyTableReload('employee_cargo_history');
  }
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

function isMissingRpcError(error) {
  const code = String(error?.code || '').trim();
  if (code === 'PGRST202' || code === '42883') return true;
  const message = [
    error?.message,
    error?.details,
    error?.hint
  ].filter(Boolean).join(' ');
  return /could not find the function|function .* does not exist|schema cache/i.test(message);
}

function unwrapRpcSingleRow(data) {
  if (Array.isArray(data)) return data[0] || null;
  return data || null;
}

async function getDailyMetricsRowByDate(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return null;
  const { data, error } = await supabase.from('daily_metrics').select('*').eq('fecha', day).maybeSingle();
  if (error) throw error;
  return data || null;
}

async function removeInvalidScheduledEmployeeDailyStatusRows(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return 0;

  const [
    { data: statusRows, error: statusError },
    sedesRows,
    employeesRows,
    cargosRows
  ] = await Promise.all([
    supabase
      .from('employee_daily_status')
      .select('id, employee_id')
      .eq('fecha', day)
      .eq('tipo_personal', 'empleado')
      .eq('servicio_programado', true),
    selectAllRows('sedes', { select: '*' }),
    selectAllRows('employees', { select: '*' }),
    selectAllRows('cargos', { select: 'codigo, alineacion_crud, nombre' })
  ]);
  if (statusError) throw statusError;

  const scheduledSedes = (sedesRows || [])
    .map(mapSedeRow)
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));
  const activeSedeCodes = new Set(scheduledSedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  activeSedeCodes.rows = scheduledSedes;
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row.codigo || '').trim(), row]));
  const employeeById = new Map(
    (employeesRows || [])
      .map(mapEmployeeRow)
      .map((row) => [String(row?.id || '').trim(), row])
      .filter(([id]) => Boolean(id))
  );

  const invalidIds = (statusRows || [])
    .filter((row) => {
      const employee = employeeById.get(String(row?.employee_id || '').trim()) || null;
      if (!employee) return true;
      if (isEmployeeSupernumerario(employee, cargoMap)) return true;
      return !isEmployeeAssignedToActiveSedeOnDate(employee, day, activeSedeCodes);
    })
    .map((row) => String(row?.id || '').trim())
    .filter(Boolean);

  for (let index = 0; index < invalidIds.length; index += 200) {
    const batch = invalidIds.slice(index, index + 200);
    const { error } = await supabase.from('employee_daily_status').delete().in('id', batch);
    if (error) throw error;
  }

  return invalidIds.length;
}

async function refreshEmployeeDailyStatusSnapshot(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return null;
  const { data, error } = await supabase.rpc('refresh_employee_daily_status', { p_fecha: day });
  if (error) {
    if (isMissingRpcError(error)) return null;
    throw error;
  }
  await removeInvalidScheduledEmployeeDailyStatusRows(day);
  await notifyTableReload('employee_daily_status');
  return data ?? 0;
}

async function refreshOperationalSnapshotsFromEmployeeDailyStatus(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return null;

  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed === null) return null;

  const { data, error } = await supabase.rpc('refresh_operational_snapshots_from_employee_daily_status', { p_fecha: day });
  if (error) {
    if (isMissingRpcError(error)) return null;
    throw error;
  }

  await notifyTableReload('sede_status');
  await notifyTableReload('daily_metrics');
  return unwrapRpcSingleRow(data);
}

async function refreshOperationalState(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return null;

  const refreshed = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
  if (refreshed !== null) {
    return getDailyMetricsRowByDate(day);
  }

  await recomputeSedeStatusSnapshot(day);
  return recomputeDailyMetrics(day);
}

async function recomputeDailyMetrics(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return null;

  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed !== null) {
    const { data, error } = await supabase.rpc('recompute_daily_metrics_from_employee_daily_status', { p_fecha: day });
    if (error) {
      if (!isMissingRpcError(error)) throw error;
    } else {
      await notifyTableReload('daily_metrics');
      return unwrapRpcSingleRow(data) || (await getDailyMetricsRowByDate(day));
    }
  }

  const [{ data: attendance }, { data: replacements }, { data: closures }, sedesRows, employeesRows, cargosRows, novedadesRows] = await Promise.all([
    supabase.from('attendance').select('*').eq('fecha', day),
    supabase.from('import_replacements').select('*').eq('fecha', day),
    supabase.from('daily_closures').select('*').eq('fecha', day).maybeSingle(),
    selectAllRows('sedes', { select: '*' }),
    selectAllRows('employees', { select: '*' }),
    selectAllRows('cargos', { select: 'codigo, alineacion_crud, nombre' }),
    selectAllRows('novedades', { select: 'codigo, codigo_novedad, nombre, reemplazo' })
  ]);
  const attRows = (attendance || []).map(mapAttendanceRow);
  const repRows = (replacements || []).map(mapImportReplacementRow);
  const sedes = (sedesRows || []).filter((s) => String(s?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
  const scheduledSedes = sedes.filter((sede) => isSedeScheduledForDate(sede, day));
  const activeSedeCodes = new Set(
    scheduledSedes
      .map((row) => String(row?.codigo || '').trim())
      .filter(Boolean)
  );
  activeSedeCodes.rows = scheduledSedes;
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules((novedadesRows || []).map(mapNovedadRow));
  const replacementMap = new Map(repRows.map((row) => [metricReplacementKey(row), row]));
  const replacementSuperDocs = new Set(
    repRows
      .filter((row) => String(row?.decision || '').trim().toLowerCase() === 'reemplazo')
      .map((row) => `${String(row?.fecha || '').trim()}|${String(row?.supernumerarioDocumento || '').trim()}`)
      .filter((value) => !value.endsWith('|'))
  );
  const employeeById = new Map();
  const employeeByDoc = new Map();
  const supernumerarioDocs = new Set();
  (employeesRows || []).forEach((emp) => {
    const mapped = mapEmployeeRow(emp);
    const empId = String(mapped?.id || '').trim();
    const doc = String(mapped?.documento || '').trim();
    if (empId) employeeById.set(empId, mapped);
    if (doc) employeeByDoc.set(doc, mapped);
    if (doc && isEmployeeSupernumerario(mapped, cargoMap) && isEmployeeAssignedToActiveSedeOnDate(mapped, day, activeSedeCodes)) {
      supernumerarioDocs.add(doc);
    }
  });
  const fallbackExpected = (employeesRows || []).filter((emp) => {
    if (String(emp?.estado || '').trim().toLowerCase() !== 'activo') return false;
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes)) return false;
    return !isEmployeeSupernumerario(emp, cargoMap);
  }).length;
  const planned = scheduledSedes.reduce((acc, sede) => {
    const n = Number(sede?.numero_operarios ?? 0);
    return acc + (Number.isFinite(n) && n > 0 ? n : 0);
  }, 0);
  const expected = fallbackExpected;
  const baseAttendanceRows = dedupeAttendanceRows(attRows).filter((row) => {
    const doc = String(row?.documento || '').trim();
    if (doc && replacementSuperDocs.has(`${String(row?.fecha || '').trim()}|${doc}`)) return false;
    if (doc && supernumerarioDocs.has(doc)) return false;
    const empId = String(row?.empleadoId || '').trim();
    const employee = (empId && employeeById.get(empId)) || (doc && employeeByDoc.get(doc)) || null;
    return !isEmployeeSupernumerario(employee, cargoMap);
  });
  const uniqueDocs = new Set(baseAttendanceRows.map((row) => String(row.documento || row.empleadoId || '').trim()).filter(Boolean));
  const actualAttendanceRows = dedupeAttendanceRows(attRows);
  const actualAttendanceCount = actualAttendanceRows.filter((row) => row?.asistio === true).length;
  const actualAbsenteeism = actualAttendanceRows.filter((row) => row?.asistio === false).length;
  const attendanceCount = planned === 0 && expected === 0
    ? actualAttendanceCount
    : baseAttendanceRows.filter((row) => metricAttendanceCountsAsService(row, replacementMap, replacementRules)).length;
  const absenteeism = planned === 0 && expected === 0
    ? actualAbsenteeism
    : baseAttendanceRows.filter((row) => metricAttendanceCountsAsAbsenteeism(row, replacementMap, replacementRules)).length;
  const paidServices = attendanceCount;
  const noContracted = Math.max(0, planned - expected);
  const payload = {
    id: day,
    fecha: day,
    planned,
    expected,
    unique_count: uniqueDocs.size,
    missing: planned === 0 && expected === 0 ? 0 : Math.max(0, expected - attendanceCount),
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

async function recomputeSedeStatusSnapshot(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return;

  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed !== null) {
    const { data, error } = await supabase.rpc('recompute_sede_status_from_employee_daily_status', { p_fecha: day });
    if (error) {
      if (!isMissingRpcError(error)) throw error;
    } else {
      await notifyTableReload('sede_status');
      return data ?? null;
    }
  }

  const [{ data: attendance }, { data: replacements }, sedesRows, employeesRows, cargosRows, novedadesRows] = await Promise.all([
    supabase.from('attendance').select('*').eq('fecha', day),
    supabase.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes', { select: '*' }),
    selectAllRows('employees', { select: '*' }),
    selectAllRows('cargos', { select: 'codigo, alineacion_crud, nombre' }),
    selectAllRows('novedades', { select: 'codigo, codigo_novedad, nombre, reemplazo' })
  ]);
  const attRows = (attendance || []).map(mapAttendanceRow);
  const repRows = (replacements || []).map(mapImportReplacementRow);
  const sedes = (sedesRows || []).filter((s) => String(s?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
  const activeSedeCodes = new Set(sedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules((novedadesRows || []).map(mapNovedadRow));
  const replacementMap = new Map(repRows.map((row) => [metricReplacementKey(row), row]));
  const replacementSuperDocs = new Set(
    repRows
      .filter((row) => String(row?.decision || '').trim().toLowerCase() === 'reemplazo')
      .map((row) => `${String(row?.fecha || '').trim()}|${String(row?.supernumerarioDocumento || '').trim()}`)
      .filter((value) => !value.endsWith('|'))
  );
  const employeeById = new Map();
  const employeeByDoc = new Map();
  const contractedBySede = new Map();
  const supernumerarioDocs = new Set();

  (employeesRows || []).forEach((emp) => {
    const empId = String(emp?.id || '').trim();
    const doc = String(emp?.documento || '').trim();
    if (empId) employeeById.set(empId, emp);
    if (doc) employeeByDoc.set(doc, emp);
    if (doc && isEmployeeSupernumerario(emp, cargoMap) && isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes)) {
      supernumerarioDocs.add(doc);
    }
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes)) return;
    if (isEmployeeSupernumerario(emp, cargoMap)) return;
    const sedeCode = String(emp?.sedeCodigo || emp?.sede_codigo || '').trim();
    if (!contractedBySede.has(sedeCode)) contractedBySede.set(sedeCode, new Set());
    contractedBySede.get(sedeCode).add(doc || empId);
  });

  const registeredBySede = new Map();
  const novSinReemplazoBySede = new Map();
  dedupeAttendanceRows(attRows).forEach((row) => {
    const doc = String(row?.documento || '').trim();
    if (doc && replacementSuperDocs.has(`${String(row?.fecha || '').trim()}|${doc}`)) return;
    if (doc && supernumerarioDocs.has(doc)) return;
    const empId = String(row?.empleadoId || '').trim();
    const employee = (empId && employeeById.get(empId)) || (doc && employeeByDoc.get(doc)) || null;
    const sedeCode = String(row?.sedeCodigo || employee?.sedeCodigo || employee?.sede_codigo || '').trim();
    if (!sedeCode || !activeSedeCodes.has(sedeCode)) return;
    if (!registeredBySede.has(sedeCode)) registeredBySede.set(sedeCode, new Set());
    registeredBySede.get(sedeCode).add(doc || empId || String(row?.id || '').trim());
    const repl = replacementMap.get(metricReplacementKey(row)) || null;
    const hasReplacement = String(repl?.decision || '').trim().toLowerCase() === 'reemplazo';
    if (row?.asistio === false && metricAttendanceRequiresReplacement(row, replacementRules) && !hasReplacement) {
      novSinReemplazoBySede.set(sedeCode, Number(novSinReemplazoBySede.get(sedeCode) || 0) + 1);
    }
  });

  const payload = sedes.map((sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planned = Number(sede?.numero_operarios ?? 0) || 0;
    const baseContracted = Number(contractedBySede.get(sedeCode)?.size || 0);
    const registered = Number(registeredBySede.get(sedeCode)?.size || 0);
    const externalRegistered = Math.max(0, registered - baseContracted);
    const contracted = Math.min(planned, baseContracted + externalRegistered);
    const noContracted = Math.max(0, planned - contracted);
    const noRegistrado = Math.max(0, contracted - registered);
    const novSinReemplazo = Number(novSinReemplazoBySede.get(sedeCode) || 0);
    const operariosPresentes = Math.max(0, planned - noContracted - noRegistrado - novSinReemplazo);
    return {
      id: `${day}_${sedeCode}`,
      fecha: day,
      sede_codigo: sedeCode,
      sede_nombre: sede?.nombre || sedeCode || null,
      operarios_esperados: contracted,
      operarios_presentes: operariosPresentes,
      faltantes: noRegistrado
    };
  });

  if (!payload.length) return;
  const { error } = await supabase.from('sede_status').upsert(payload, { onConflict: 'id' });
  if (error) throw error;
  await notifyTableReload('sede_status');
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

function collectEmployeeOperationalRefreshDays(before = {}, after = {}) {
  const today = todayBogotaISO();
  if (!today) return [];

  const oldest = addDaysToIsoDate(today, -EMPLOYEE_OPERATIONAL_REFRESH_LOOKBACK_DAYS) || today;
  const hints = [
    today,
    addDaysToIsoDate(today, -1),
    toISODate(before?.fechaIngreso || before?.fecha_ingreso),
    toISODate(after?.fechaIngreso || after?.fecha_ingreso),
    toISODate(before?.fechaRetiro || before?.fecha_retiro),
    toISODate(after?.fechaRetiro || after?.fecha_retiro)
  ]
    .filter((day) => /^\d{4}-\d{2}-\d{2}$/.test(String(day || '')))
    .filter((day) => day <= today)
    .sort();

  const start = hints.length ? (hints[0] < oldest ? oldest : hints[0]) : today;
  const days = [];
  let cursor = start;
  while (cursor && cursor <= today) {
    days.push(cursor);
    if (days.length > EMPLOYEE_OPERATIONAL_REFRESH_LOOKBACK_DAYS + 2) break;
    cursor = addDaysToIsoDate(cursor, 1);
  }
  return days;
}

async function reconcileOperationalSnapshotsForEmployeeChange(before = {}, after = {}) {
  const days = collectEmployeeOperationalRefreshDays(before, after);
  for (const day of days) {
    await refreshOperationalState(day);
  }
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

  await refreshOperationalState(nextDay);
  await notifyTableReload('attendance');
  await notifyTableReload('absenteeism');
}

async function getNextPrefixedCode(table, prefix, width = 4) {
  const data = await selectAllRows(table, { select: 'codigo' });
  let max = 0;
  (data || []).forEach((row) => {
    const num = extractPrefixedCodeNumber(row?.codigo, prefix);
    if (num > max) max = num;
  });
  return `${prefix}-${String(max + 1).padStart(width, '0')}`;
}

function extractPrefixedCodeNumber(code, prefix) {
  const normalized = String(code || '').trim();
  const match = normalized.match(new RegExp(`^${prefix}-(\\d+)$`));
  if (!match) return 0;
  return Number(match[1] || 0);
}

function buildNextPrefixedCode(prefix, value, width = 4) {
  return `${prefix}-${String(value).padStart(width, '0')}`;
}

function chunkArray(items = [], size = 250) {
  const out = [];
  const safeSize = Math.max(1, Number(size) || 1);
  for (let i = 0; i < items.length; i += safeSize) {
    out.push(items.slice(i, i + safeSize));
  }
  return out;
}

async function insertEmployeeRecord({
  codigo,
  documento,
  nombre,
  telefono,
  cargoCodigo,
  cargoNombre,
  sedeCodigo,
  sedeNombre,
  fechaIngreso,
  audit,
  zone,
  notifyEmployeesReload = true,
  historySource = 'create_employee'
}) {
  const normalizedPhone = normalizeStoredPhone(telefono);
  const { data, error } = await supabase
    .from('employees')
    .insert({
      codigo: codigo || null,
      documento: String(documento || '').trim() || null,
      nombre: nombre || null,
      telefono: normalizedPhone,
      cargo_codigo: cargoCodigo || null,
      cargo_nombre: cargoNombre || null,
      sede_codigo: sedeCodigo || null,
      sede_nombre: sedeNombre || null,
      zona_codigo: zone?.zonaCodigo || null,
      zona_nombre: zone?.zonaNombre || null,
      fecha_ingreso: fechaIngreso || null,
      fecha_retiro: null,
      estado: 'activo',
      created_by_uid: audit?.created_by_uid || null,
      created_by_email: audit?.created_by_email || null,
      last_modified_by_uid: audit?.created_by_uid || null,
      last_modified_by_email: audit?.created_by_email || null,
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
    sedeCodigo: data.sede_codigo,
    sedeNombre: data.sede_nombre,
    fechaIngreso: data.fecha_ingreso,
    source: historySource
  });
  if (notifyEmployeesReload) {
    await notifyTableReload('employees');
  }
  return data;
}

function streamTable(table, mapper, onData, {
  order = 'created_at',
  onError = null,
  onStatus = null
} = {}) {
  let active = true;
  const emit = async () => {
    try {
      const data = await selectAllRows(table, { select: '*', order, ascending: false });
      if (!active) return;
      onData((data || []).map((row) => mapper(row)));
    } catch (error) {
      if (!active) return;
      console.error(`No se pudo cargar ${table}:`, error);
      onError?.(error, 'LOAD_ERROR');
      onData([]);
    }
  };

  emit();
  const unregister = registerTableReloader(table, emit);

  const realtime = subscribeToRealtime(
    supabase
      .channel(nextRealtimeChannelName(`${table}-watch`))
      .on('postgres_changes', { event: '*', schema: 'public', table }, emit),
    { label: table, onError, onStatus }
  );
  const channel = realtime.subscription;

  return () => {
    active = false;
    unregister();
    realtime.cancel();
    supabase.removeChannel(channel);
  };
}

export const authState = (cb) => {
  supabase.auth.getSession().then(async ({ data, error }) => {
    if (error) {
      console.error('No se pudo consultar la sesion de Supabase:', error);
      if (/invalid refresh token|refresh token not found/i.test(String(error?.message || ''))) {
        try { await supabase.auth.signOut({ scope: 'local' }); } catch {}
      }
      cb(null);
      return;
    }
    await syncRealtimeAuth(data.session || null);
    cb(normalizeUser(data.session?.user || null));
  });

  const { data } = supabase.auth.onAuthStateChange(async (_event, session) => {
    await syncRealtimeAuth(session || null);
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

export async function register(email, pass, profile = {}) {
  const cleanEmail = String(email || '').trim().toLowerCase();
  const emailRedirectTo = (() => {
    try {
      return window.location.origin;
    } catch {
      return undefined;
    }
  })();
  const { data, error } = await supabase.auth.signUp({
    email: cleanEmail,
    password: pass,
    options: {
      ...(emailRedirectTo ? { emailRedirectTo } : {}),
      data: {
        display_name: profile?.displayName || profile?.nombre || null,
        full_name: profile?.displayName || profile?.nombre || null,
        documento: profile?.documento || null
      }
    }
  });
  if (error) throw error;
  return {
    user: normalizeUser(data.user),
    session: data.session || null
  };
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
    documento: user.documento,
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

export async function getUserOverrides(uid = null) {
  const currentUser = (await supabase.auth.getUser()).data.user;
  const targetUid = String(uid || currentUser?.id || '').trim();
  if (!targetUid) return {};
  const { data, error } = await supabase
    .from('user_overrides')
    .select('permissions')
    .eq('user_id', targetUid)
    .maybeSingle();
  if (error) throw error;
  return data?.permissions || {};
}

export async function setUserOverrides(uid, permissions = {}) {
  const targetUid = String(uid || '').trim();
  if (!targetUid) throw new Error('Falta el usuario para guardar overrides.');
  const payload = {
    user_id: targetUid,
    permissions: sanitizePermissionsRecord(permissions),
    updated_at: new Date().toISOString()
  };
  const { error } = await supabase
    .from('user_overrides')
    .upsert(payload, { onConflict: 'user_id' });
  if (error) throw error;
  await notifyTableReload('user_overrides');
}

export async function clearUserOverrides(uid) {
  const targetUid = String(uid || '').trim();
  if (!targetUid) throw new Error('Falta el usuario para limpiar overrides.');
  const { error } = await supabase
    .from('user_overrides')
    .delete()
    .eq('user_id', targetUid);
  if (error) throw error;
  await notifyTableReload('user_overrides');
}

export async function setRolePermissions(role, permissions = {}) {
  const cleanRole = String(role || '').trim().toLowerCase();
  if (!cleanRole) throw new Error('Falta el rol a actualizar.');
  const { error } = await supabase
    .from('roles_matrix')
    .upsert({
      role: cleanRole,
      permissions: sanitizePermissionsRecord(permissions),
      updated_at: new Date().toISOString()
    }, { onConflict: 'role' });
  if (error) throw error;
  await notifyTableReload('roles_matrix');
}

export function streamRoleMatrix(onData, onError = null, onStatus = null) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('roles_matrix')
      .select('role, permissions');
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar roles_matrix:', error);
      onError?.(error, 'LOAD_ERROR');
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

  const unregister = registerTableReloader('roles_matrix', emit);
  const realtime = subscribeToRealtime(
    supabase
      .channel(nextRealtimeChannelName('roles-matrix-watch'))
      .on('postgres_changes', { event: '*', schema: 'public', table: 'roles_matrix' }, emit),
    { label: 'roles_matrix', onError, onStatus }
  );
  const channel = realtime.subscription;

  return () => {
    active = false;
    unregister();
    realtime.cancel();
    supabase.removeChannel(channel);
  };
}

export function streamUsers(onData) {
  let active = true;
  const emit = async () => {
    try {
      const rows = await selectAllRows(SUPABASE_PROFILES_TABLE, {
        select: '*',
        order: 'email',
        ascending: true
      });
      if (!active) return;
      onData((rows || []).map(mapUserProfileRow));
    } catch (error) {
      console.error('No se pudo cargar profiles:', error);
      if (!active) return;
      onData([]);
    }
  };

  emit();

  const unregister = registerTableReloader(SUPABASE_PROFILES_TABLE, emit);
  const channel = supabase
    .channel('profiles-watch')
    .on('postgres_changes', { event: '*', schema: 'public', table: SUPABASE_PROFILES_TABLE }, emit)
    .subscribe();

  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export async function findUserByEmail(email) {
  const cleanEmail = String(email || '').trim().toLowerCase();
  if (!cleanEmail) return null;
  const { data, error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .select('*')
    .eq('email', cleanEmail)
    .maybeSingle();
  if (error) throw error;
  return data ? mapUserProfileRow(data) : null;
}

export async function setUserRole(uid, role) {
  const targetUid = String(uid || '').trim();
  const nextRole = String(role || '').trim().toLowerCase();
  if (!targetUid) throw new Error('Falta el usuario a actualizar.');
  if (!nextRole) throw new Error('Falta el rol a asignar.');
  const audit = await getCurrentAuditFields();
  const patch = {
    role: nextRole,
    updated_at: new Date().toISOString(),
    last_modified_by_uid: audit.created_by_uid,
    last_modified_by_email: audit.created_by_email
  };
  const { error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .update(patch)
    .eq('id', targetUid);
  if (error) throw error;
  await notifyTableReload(SUPABASE_PROFILES_TABLE);
}

export async function setUserStatus(uid, estado) {
  const targetUid = String(uid || '').trim();
  const nextStatus = String(estado || '').trim().toLowerCase();
  if (!targetUid) throw new Error('Falta el usuario a actualizar.');
  if (!nextStatus) throw new Error('Falta el estado a asignar.');
  const audit = await getCurrentAuditFields();
  const patch = {
    estado: nextStatus,
    updated_at: new Date().toISOString(),
    last_modified_by_uid: audit.created_by_uid,
    last_modified_by_email: audit.created_by_email
  };
  if (nextStatus !== 'eliminado') {
    patch.deleted_at = null;
    patch.deleted_by_uid = null;
    patch.deleted_by_email = null;
  }
  const { error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .update(patch)
    .eq('id', targetUid);
  if (error) throw error;
  await notifyTableReload(SUPABASE_PROFILES_TABLE);
}

export async function softDeleteUser(uid) {
  const targetUid = String(uid || '').trim();
  if (!targetUid) throw new Error('Falta el usuario a eliminar.');
  const audit = await getCurrentAuditFields();
  const timestamp = new Date().toISOString();
  const { error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .update({
      estado: 'eliminado',
      role: 'empleado',
      updated_at: timestamp,
      last_modified_by_uid: audit.created_by_uid,
      last_modified_by_email: audit.created_by_email,
      deleted_at: timestamp,
      deleted_by_uid: audit.created_by_uid,
      deleted_by_email: audit.created_by_email
    })
    .eq('id', targetUid);
  if (error) throw error;
  await clearUserOverrides(targetUid);
  await notifyTableReload(SUPABASE_PROFILES_TABLE);
}

export async function addAuditLog({
  targetType = null,
  targetId = null,
  action = null,
  before = null,
  after = null,
  note = null
} = {}) {
  const audit = await getCurrentAuditFields();
  const { error } = await supabase
    .from('audit_logs')
    .insert({
      actor_uid: audit.created_by_uid,
      actor_email: audit.created_by_email,
      target_type: targetType,
      target_id: targetId == null ? null : String(targetId),
      action,
      before_data: before,
      after_data: after,
      note
    });
  if (error) throw error;
  await notifyTableReload('audit_logs');
}

export function streamAuditLogs(onData, max = 200) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('audit_logs')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(max);
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar audit_logs:', error);
      onData([]);
      return;
    }
    onData((data || []).map(mapAuditLogRow));
  };

  emit();

  const unregister = registerTableReloader('audit_logs', emit);
  const channel = supabase
    .channel('audit-logs-watch')
    .on('postgres_changes', { event: '*', schema: 'public', table: 'audit_logs' }, emit)
    .subscribe();

  return () => {
    active = false;
    unregister();
    supabase.removeChannel(channel);
  };
}

export function streamUserOverrides(uid, onData, onError = null, onStatus = null) {
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
      onError?.(error, 'LOAD_ERROR');
      onData({});
      return;
    }
    onData(data?.permissions || {});
  };

  emit();

  const unregister = registerTableReloader('user_overrides', emit);
  const realtime = subscribeToRealtime(
    supabase
      .channel(nextRealtimeChannelName(`user-overrides-${uid}`))
      .on('postgres_changes', { event: '*', schema: 'public', table: 'user_overrides', filter: `user_id=eq.${uid}` }, emit),
    { label: `user_overrides:${uid}`, onError, onStatus }
  );
  const channel = realtime.subscription;

  return () => {
    active = false;
    unregister();
    realtime.cancel();
    supabase.removeChannel(channel);
  };
}

export function streamZones(onData, onError = null, onStatus = null) {
  return streamTable('zones', mapCatalogRow, onData, { onError, onStatus });
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

export function streamDependencies(onData, onError = null, onStatus = null) {
  return streamTable('dependencies', mapCatalogRow, onData, { onError, onStatus });
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

export function streamSedes(onData, onError = null, onStatus = null) {
  return streamTable('sedes', mapSedeRow, onData, { onError, onStatus });
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

export function streamCargos(onData, onError = null, onStatus = null) {
  return streamTable('cargos', mapCargoRow, onData, { onError, onStatus });
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

export function streamNovedades(onData, onError = null, onStatus = null) {
  return streamTable('novedades', mapNovedadRow, onData, { onError, onStatus });
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

export function streamEmployees(onData, onError = null, onStatus = null) {
  return streamTable('employees', mapEmployeeRow, onData, { onError, onStatus });
}

export function streamActiveBaseEmployees(onData) {
  let active = true;
  const emit = async () => {
    const [employeesResult, { data: cargos, error: cargoError }] = await Promise.all([
      selectAllRows('employees', { select: '*', order: 'created_at', ascending: false }).then((value) => ({ status: 'fulfilled', value })).catch((error) => ({ status: 'rejected', reason: error })),
      supabase.from('cargos').select('codigo, nombre, alineacion_crud')
    ]);
    if (!active) return;
    const empError = employeesResult.status === 'rejected' ? employeesResult.reason : null;
    const employeeRows = employeesResult.status === 'fulfilled' ? employeesResult.value : [];
    if (empError || cargoError) {
      console.error('No se pudieron cargar empleados activos base:', empError || cargoError);
      onData([]);
      return;
    }
    const cargoMap = new Map((cargos || []).map((row) => [String(row.codigo || '').trim(), row]));
    const today = todayBogotaISO();
    const rows = (employeeRows || [])
      .filter((emp) => isEmployeeActiveForDate(emp, today))
      .filter((emp) => !isEmployeeSupernumerario(emp, cargoMap))
      .map((row) => mapEmployeeRow(row));
    onData(rows);
  };
  emit();
  const unA = registerTableReloader('employees', emit);
  const unB = registerTableReloader('cargos', emit);
  const channelA = supabase.channel(nextRealtimeChannelName('employees-active-base-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'employees' }, emit).subscribe();
  const channelB = supabase.channel(nextRealtimeChannelName('employees-active-base-cargos-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'cargos' }, emit).subscribe();
  return () => {
    active = false;
    unA();
    unB();
    supabase.removeChannel(channelA);
    supabase.removeChannel(channelB);
  };
}


export async function getNextEmployeeCode(prefix = 'EMP', width = 4) {
  return getNextPrefixedCode('employees', prefix, width);
}

export async function createEmployee({ codigo, documento, nombre, telefono, cargoCodigo, cargoNombre, sedeCodigo, sedeNombre, fechaIngreso }) {
  const audit = await getCurrentAuditFields();
  const zone = await resolveZoneBySedeCode(sedeCodigo);
  const data = await insertEmployeeRecord({
    codigo,
    documento,
    nombre,
    telefono,
    cargoCodigo,
    cargoNombre,
    sedeCodigo,
    sedeNombre,
    fechaIngreso,
    audit,
    zone,
    notifyEmployeesReload: true,
    historySource: 'create_employee'
  });
  return data.id;
}

function normalizeStoredPhone(value) {
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

export async function createEmployeesBulk(rows = [], options = {}) {
  const items = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if (!items.length) return { created: 0 };
  const onProgress = typeof options?.onProgress === 'function' ? options.onProgress : null;
  const chunkSize = Math.max(1, Number(options?.chunkSize) || 250);
  const reportProgress = (created, total, phase = 'importing') => {
    if (!onProgress) return;
    onProgress({
      created,
      total,
      percent: total > 0 ? Math.min(100, Math.round((created / total) * 100)) : 0,
      phase
    });
  };
  reportProgress(0, items.length, 'preparing');
  const audit = await getCurrentAuditFields();
  const existingCodes = await selectAllRows('employees', { select: 'codigo' });
  let nextCodeNumber = 0;
  (existingCodes || []).forEach((row) => {
    const num = extractPrefixedCodeNumber(row?.codigo, 'EMP');
    if (num > nextCodeNumber) nextCodeNumber = num;
  });
  const sedeCodes = [...new Set(items.map((row) => String(row?.sedeCodigo || '').trim()).filter(Boolean))];
  const zoneBySedeCode = new Map();
  if (sedeCodes.length) {
    const { data: sedesRows, error: sedesError } = await supabase
      .from('sedes')
      .select('codigo, zona_codigo, zona_nombre')
      .in('codigo', sedeCodes);
    if (sedesError) throw sedesError;
    (sedesRows || []).forEach((row) => {
      zoneBySedeCode.set(String(row.codigo || '').trim(), {
        zonaCodigo: row.zona_codigo || null,
        zonaNombre: row.zona_nombre || null
      });
    });
  }
  const batches = chunkArray(items, chunkSize);
  let created = 0;
  for (const batch of batches) {
    const timestamp = new Date().toISOString();
    const payloads = batch.map((row) => {
      const codigo = row.codigo || buildNextPrefixedCode('EMP', ++nextCodeNumber, 4);
      const zone = zoneBySedeCode.get(String(row.sedeCodigo || '').trim()) || { zonaCodigo: null, zonaNombre: null };
      return {
        codigo,
        documento: String(row.documento || '').trim() || null,
        nombre: row.nombre || null,
        telefono: normalizeStoredPhone(row.telefono),
        cargo_codigo: row.cargoCodigo || null,
        cargo_nombre: row.cargoNombre || null,
        sede_codigo: row.sedeCodigo || null,
        sede_nombre: row.sedeNombre || null,
        zona_codigo: zone.zonaCodigo || null,
        zona_nombre: zone.zonaNombre || null,
        fecha_ingreso: normalizeBulkDate(row.fechaIngreso),
        fecha_retiro: null,
        estado: 'activo',
        created_by_uid: audit.created_by_uid,
        created_by_email: audit.created_by_email,
        last_modified_by_uid: audit.created_by_uid,
        last_modified_by_email: audit.created_by_email,
        last_modified_at: timestamp
      };
    });
    const { data: insertedRows, error: insertError } = await supabase
      .from('employees')
      .insert(payloads)
      .select('id, codigo, documento, cargo_codigo, cargo_nombre, sede_codigo, sede_nombre, fecha_ingreso');
    if (insertError) throw insertError;
    await appendEmployeeCargoHistoryBulk((insertedRows || []).map((row) => ({
      employee_id: row.id,
      employee_codigo: row.codigo || null,
      documento: row.documento || null,
      cargo_codigo: row.cargo_codigo || null,
      cargo_nombre: row.cargo_nombre || null,
      sede_codigo: row.sede_codigo || null,
      sede_nombre: row.sede_nombre || null,
      fecha_ingreso: row.fecha_ingreso || null,
      fecha_retiro: null,
      source: 'bulk_create_employee'
    })), false);
    created += (insertedRows || []).length;
    reportProgress(created, items.length, 'importing');
  }
  reportProgress(created, items.length, 'refreshing');
  await Promise.all([
    notifyTableReload('employees'),
    notifyTableReload('employee_cargo_history')
  ]);
  reportProgress(created, items.length, 'completed');
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
  if (typeof data.telefono === 'string') patch.telefono = normalizeStoredPhone(data.telefono);
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
  const currentIngreso = toISODate(currentRow.fecha_ingreso);
  const updatedIngreso = toISODate(updated.fecha_ingreso);
  const currentRetiro = toISODate(currentRow.fecha_retiro);
  const updatedRetiro = toISODate(updated.fecha_retiro);
  const currentSede = String(currentRow.sede_codigo || '').trim();
  const updatedSede = String(updated.sede_codigo || '').trim();
  const currentEstado = String(currentRow.estado || 'activo').trim().toLowerCase();
  const updatedEstado = String(updated.estado || 'activo').trim().toLowerCase();
  const currentDocumento = String(currentRow.documento || '').trim();
  const updatedDocumento = String(updated.documento || '').trim();
  const cargoChanged =
    String(updated.cargo_codigo || '') !== String(currentRow.cargo_codigo || '');
  const sedeChanged = updatedSede !== currentSede;
  const ingresoChanged = updatedIngreso !== currentIngreso;
  const retiroChanged = updatedRetiro !== currentRetiro;
  const estadoChanged = updatedEstado !== currentEstado;
  const documentChanged = updatedDocumento !== currentDocumento;
  const requiresNewHistoryEntry =
    String(updated.estado || 'activo').trim().toLowerCase() === 'activo' &&
    (cargoChanged || sedeChanged || ingresoChanged);
  if (requiresNewHistoryEntry) {
    const historyRetiro =
      data.historialFechaRetiro ||
      data.fechaHistorialRetiro ||
      data.assignmentFechaRetiro ||
      updated.updated_at ||
      new Date().toISOString();
    await closeActiveEmployeeHistory(updated.id, historyRetiro, false);
    await appendEmployeeCargoHistory({
      employeeId: updated.id,
      employeeCodigo: updated.codigo,
      documento: updated.documento,
      cargoCodigo: updated.cargo_codigo,
      cargoNombre: updated.cargo_nombre,
      sedeCodigo: updated.sede_codigo,
      sedeNombre: updated.sede_nombre,
      fechaIngreso: updated.fecha_ingreso || new Date().toISOString(),
      fechaRetiro: null,
      source: sedeChanged ? 'sede_change' : (cargoChanged ? 'cargo_change' : 'employee_update')
    });
  }
  if (await getCargoCrudAlignmentByCode(updated.cargo_codigo, updated.cargo_nombre) === 'supervisor') {
    await upsertSupervisorProfileFromEmployee(mapEmployeeRow(updated));
  }
  if (cargoChanged || sedeChanged || ingresoChanged || retiroChanged || estadoChanged || documentChanged) {
    await reconcileOperationalSnapshotsForEmployeeChange(currentRow, updated);
  }
  await notifyTableReload('employees');
}

export async function setEmployeeStatus(id, estado, options = null) {
  const current = await supabase.from('employees').select('*').eq('id', id).single();
  if (current.error) throw current.error;
  const currentRow = current.data;
  const opts = options && typeof options === 'object' && !(options instanceof Date)
    ? options
    : { fechaRetiro: options || null };
  const fechaRetiro = opts.fechaRetiro || null;
  const fechaIngreso = opts.fechaIngreso || null;
  const audit = await getCurrentAuditFields();
  const patch = {
    estado,
    fecha_retiro: estado === 'inactivo' ? (fechaRetiro || new Date().toISOString()) : null,
    last_modified_by_uid: audit.created_by_uid,
    last_modified_by_email: audit.created_by_email,
    last_modified_at: new Date().toISOString()
  };
  if (estado === 'activo' && String(currentRow.estado || '').trim().toLowerCase() !== 'activo') {
    patch.fecha_ingreso = fechaIngreso || new Date().toISOString();
    patch.fecha_retiro = null;
  }
  const { data, error } = await supabase.from('employees').update(patch).eq('id', id).select('*').single();
  if (error) throw error;
  const previousEstado = String(currentRow.estado || '').trim().toLowerCase();
  const nextEstado = String(estado || '').trim().toLowerCase();
  if (previousEstado !== 'inactivo' && nextEstado === 'inactivo') {
    await closeActiveEmployeeHistory(data.id, patch.fecha_retiro, true);
  }
  if (previousEstado === 'inactivo' && nextEstado === 'activo') {
    await appendEmployeeCargoHistory({
      employeeId: data.id,
      employeeCodigo: data.codigo,
      documento: data.documento,
      cargoCodigo: data.cargo_codigo,
      cargoNombre: data.cargo_nombre,
      sedeCodigo: data.sede_codigo,
      sedeNombre: data.sede_nombre,
      fechaIngreso: data.fecha_ingreso || new Date().toISOString(),
      fechaRetiro: null,
      source: 'reactivate_employee'
    });
  }
  if (await getCargoCrudAlignmentByCode(data.cargo_codigo, data.cargo_nombre) === 'supervisor') {
    await upsertSupervisorProfileFromEmployee(mapEmployeeRow(data), {
      estado,
      fechaIngreso: patch.fecha_ingreso,
      fechaRetiro: patch.fecha_retiro
    });
  }
  if (previousEstado !== nextEstado || toISODate(currentRow.fecha_retiro) !== toISODate(data.fecha_retiro) || toISODate(currentRow.fecha_ingreso) !== toISODate(data.fecha_ingreso)) {
    await reconcileOperationalSnapshotsForEmployeeChange(currentRow, data);
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
    const [employeesResult, { data: cargos, error: cargoError }] = await Promise.all([
      selectAllRows('employees', { select: '*', order: 'created_at', ascending: false }).then((value) => ({ status: 'fulfilled', value })).catch((error) => ({ status: 'rejected', reason: error })),
      supabase.from('cargos').select('codigo, nombre, alineacion_crud')
    ]);
    if (!active) return;
    const empError = employeesResult.status === 'rejected' ? employeesResult.reason : null;
    const employeeRows = employeesResult.status === 'fulfilled' ? employeesResult.value : [];
    if (empError || cargoError) {
      console.error('No se pudieron cargar supernumerarios:', empError || cargoError);
      onData([]);
      return;
    }
    const cargoMap = new Map((cargos || []).map((row) => [String(row.codigo || ''), row]));
    const rows = (employeeRows || [])
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
  const channelA = supabase.channel(nextRealtimeChannelName('supernumerarios-employees-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'employees' }, emit).subscribe();
  const channelB = supabase.channel(nextRealtimeChannelName('supernumerarios-cargos-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'cargos' }, emit).subscribe();
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
      telefono: normalizeStoredPhone(row.telefono),
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
      employeesResult,
      profilesResult,
      { data: cargos, error: cargoError }
    ] = await Promise.all([
      selectAllRows('employees', { select: '*', order: 'created_at', ascending: false }).then((value) => ({ status: 'fulfilled', value })).catch((error) => ({ status: 'rejected', reason: error })),
      selectAllRows('supervisor_profile', { select: '*', order: 'created_at', ascending: false }).then((value) => ({ status: 'fulfilled', value })).catch((error) => ({ status: 'rejected', reason: error })),
      supabase.from('cargos').select('codigo, nombre, alineacion_crud')
    ]);

    const empError = employeesResult.status === 'rejected' ? employeesResult.reason : null;
    const profileError = profilesResult.status === 'rejected' ? profilesResult.reason : null;
    const employees = employeesResult.status === 'fulfilled' ? employeesResult.value : [];
    const profiles = profilesResult.status === 'fulfilled' ? profilesResult.value : [];

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
  const channelA = supabase.channel(nextRealtimeChannelName('supervisors-employees-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'employees' }, emit).subscribe();
  const channelB = supabase.channel(nextRealtimeChannelName('supervisors-profile-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'supervisor_profile' }, emit).subscribe();
  const channelC = supabase.channel(nextRealtimeChannelName('supervisors-cargos-watch')).on('postgres_changes', { event: '*', schema: 'public', table: 'cargos' }, emit).subscribe();

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

export function streamAttendanceByDate(fecha, onData, onError = null, onStatus = null) {
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
      onError?.(error, 'LOAD_ERROR');
      onData([]);
      return;
    }
    onData((data || []).map(mapAttendanceRow));
  };
  const onChange = (payload) => {
    if (!shouldRefreshForDay(payload, day, 'fecha')) return;
    emit();
  };
  emit();
  const unregister = registerTableReloader('attendance', emit);
  const realtime = subscribeToRealtime(
    supabase.channel(nextRealtimeChannelName(`attendance-${day}`)).on('postgres_changes', { event: '*', schema: 'public', table: 'attendance' }, onChange),
    { label: `attendance:${day}`, onError, onStatus }
  );
  const channel = realtime.subscription;
  return () => {
    active = false;
    unregister();
    realtime.cancel();
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

export function streamImportReplacementsByDate(fecha, onData, onError = null, onStatus = null) {
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
      onError?.(error, 'LOAD_ERROR');
      onData([]);
      return;
    }
    onData((data || []).map(mapImportReplacementRow));
  };
  const onChange = (payload) => {
    if (!shouldRefreshForDay(payload, day, 'fecha')) return;
    emit();
  };
  emit();
  const unregister = registerTableReloader('import_replacements', emit);
  const realtime = subscribeToRealtime(
    supabase.channel(nextRealtimeChannelName(`import-replacements-${day}`)).on('postgres_changes', { event: '*', schema: 'public', table: 'import_replacements' }, onChange),
    { label: `import_replacements:${day}`, onError, onStatus }
  );
  const channel = realtime.subscription;
  return () => {
    active = false;
    unregister();
    realtime.cancel();
    supabase.removeChannel(channel);
  };
}

export function streamDailyMetricsByDate(fecha, onData, onError = null, onStatus = null) {
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
      onError?.(error, 'LOAD_ERROR');
      onData(null);
      return;
    }
    onData(data ? mapDailyMetricsRow(data) : null);
  };
  const onChange = (payload) => {
    if (!shouldRefreshForDay(payload, day, 'fecha')) return;
    emit();
  };
  emit();
  const unregister = registerTableReloader('daily_metrics', emit);
  const realtime = subscribeToRealtime(
    supabase.channel(nextRealtimeChannelName(`daily-metrics-${day}`)).on('postgres_changes', { event: '*', schema: 'public', table: 'daily_metrics' }, onChange),
    { label: `daily_metrics:${day}`, onError, onStatus }
  );
  const channel = realtime.subscription;
  return () => {
    active = false;
    unregister();
    realtime.cancel();
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

export function streamIncapacidades(onData, onError = null, onStatus = null) {
  return streamTable('incapacitados', mapIncapacidadRow, onData, { onError, onStatus });
}

export async function uploadIncapacidadSupport(file, context = {}) {
  if (!file) throw new Error('Selecciona un soporte para cargar.');
  const path = buildIncapacitySupportPath(file, context);
  const { error } = await supabase
    .storage
    .from(INCAPACITY_SUPPORT_BUCKET)
    .upload(path, file, {
      upsert: false,
      contentType: file.type || 'application/octet-stream'
    });
  if (error) throw error;
  const { data } = supabase.storage.from(INCAPACITY_SUPPORT_BUCKET).getPublicUrl(path);
  return {
    path,
    url: data?.publicUrl || '',
    name: String(file.name || '').trim() || 'soporte',
    mimeType: String(file.type || '').trim() || 'application/octet-stream'
  };
}

export async function createIncapacidad({
  employeeId = null,
  documento = null,
  nombre = null,
  fechaInicio,
  fechaFin,
  estado = 'activo',
  source = 'Incapacidad',
  canalRegistro = 'portal_web',
  soporteUrl = null,
  soporteNombre = null,
  soporteTipo = null,
  soporteStoragePath = null,
  whatsappMessageId = null
} = {}) {
  const payload = {
    employee_id: employeeId || null,
    documento: documento || null,
    nombre: nombre || null,
    fecha_inicio: fechaInicio || null,
    fecha_fin: fechaFin || null,
    estado: estado || 'activo',
    source: source || 'Incapacidad',
    canal_registro: canalRegistro || 'portal_web',
    soporte_url: soporteUrl || null,
    soporte_nombre: soporteNombre || null,
    soporte_tipo: supportValueOrNull(soporteTipo),
    soporte_storage_path: supportValueOrNull(soporteStoragePath),
    whatsapp_message_id: whatsappMessageId || null
  };
  const { data, error } = await supabase
    .from('incapacitados')
    .insert(payload)
    .select('*')
    .single();
  if (error) throw error;
  await notifyTableReload('incapacitados');
  return mapIncapacidadRow(data);
}

function supportValueOrNull(value) {
  return value ? value : null;
}

export async function updateIncapacidad(id, {
  employeeId,
  documento,
  nombre,
  fechaInicio,
  fechaFin,
  estado,
  source,
  canalRegistro,
  soporteUrl,
  soporteNombre,
  soporteTipo,
  soporteStoragePath
} = {}) {
  const patch = {};
  if (employeeId !== undefined) patch.employee_id = employeeId || null;
  if (documento !== undefined) patch.documento = documento || null;
  if (nombre !== undefined) patch.nombre = nombre || null;
  if (fechaInicio !== undefined) patch.fecha_inicio = fechaInicio || null;
  if (fechaFin !== undefined) patch.fecha_fin = fechaFin || null;
  if (estado !== undefined) patch.estado = estado || 'activo';
  if (source !== undefined) patch.source = source || null;
  if (canalRegistro !== undefined) patch.canal_registro = canalRegistro || null;
  if (soporteUrl !== undefined) patch.soporte_url = soporteUrl || null;
  if (soporteNombre !== undefined) patch.soporte_nombre = soporteNombre || null;
  if (soporteTipo !== undefined) patch.soporte_tipo = soporteTipo || null;
  if (soporteStoragePath !== undefined) patch.soporte_storage_path = soporteStoragePath || null;
  const { data, error } = await supabase
    .from('incapacitados')
    .update(patch)
    .eq('id', id)
    .select('*')
    .single();
  if (error) throw error;
  await notifyTableReload('incapacitados');
  return mapIncapacidadRow(data);
}

export async function setIncapacidadStatus(id, estado) {
  const { data, error } = await supabase
    .from('incapacitados')
    .update({ estado: estado || 'activo' })
    .eq('id', id)
    .select('*')
    .single();
  if (error) throw error;
  await notifyTableReload('incapacitados');
  return mapIncapacidadRow(data);
}

export async function listIncapacidadesRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('incapacitados')
    .select('*')
    .order('created_at', { ascending: false });
  if (error) throw error;
  return (data || [])
    .map(mapIncapacidadRow)
    .filter((row) => incapacityOverlapsRange(row, dateFrom, dateTo));
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

export async function listEmployeeDailyStatusRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const rows = [];
  let from = 0;

  while (true) {
    const { data, error } = await supabase
      .from('employee_daily_status')
      .select('*')
      .gte('fecha', dateFrom)
      .lte('fecha', dateTo)
      .order('fecha', { ascending: true })
      .order('sede_codigo', { ascending: true })
      .order('nombre', { ascending: true })
      .range(from, from + POSTGREST_PAGE_SIZE - 1);
    if (error) throw error;

    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);
    if (batch.length < POSTGREST_PAGE_SIZE) break;
    from += POSTGREST_PAGE_SIZE;
  }

  return rows.map(mapEmployeeDailyStatusRow);
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

export async function listDailySedeClosuresRange(dateFrom, dateTo) {
  if (!dateFrom || !dateTo) return [];
  const { data, error } = await supabase
    .from('daily_sede_closures')
    .select('*')
    .gte('fecha', dateFrom)
    .lte('fecha', dateTo)
    .order('fecha', { ascending: true })
    .order('sede_codigo', { ascending: true });
  if (error) throw error;
  return (data || []).map(mapDailySedeClosureRow);
}

async function persistDailySedeClosureSnapshot(day) {
  const snapshot = await computeDailySedeClosureSnapshot(day);
  if (!snapshot.length) return [];
  const { error } = await supabase.from('daily_sede_closures').upsert(snapshot, { onConflict: 'id' });
  if (error) throw error;
  await notifyTableReload('daily_sede_closures');
  return snapshot;
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
    await refreshOperationalState(day);
  }
  await notifyTableReload('import_replacements');
  return { saved: data.length };
}

export async function closeOperationDayManual(fecha) {
  const day = String(fecha || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new Error('Fecha invalida.');
  if (await isOperationDayClosed(day)) {
    await persistDailySedeClosureSnapshot(day);
    await runPostClosureTasks(day);
    const refreshedAlreadyClosed = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
    if (refreshedAlreadyClosed === null) {
      await recomputeSedeStatusSnapshot(day);
      await recomputeDailyMetrics(day);
    }
    return { ok: true, results: [{ date: day, status: 'already_closed' }] };
  }

  await finalizePendingAbsenteeismForClosure(day);

  const refreshedBeforeClosure = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
  let metricsRow = null;
  if (refreshedBeforeClosure === null) {
    await recomputeSedeStatusSnapshot(day);
    metricsRow = await recomputeDailyMetrics(day);
  } else {
    metricsRow = await getDailyMetricsRowByDate(day);
    if (!metricsRow) metricsRow = await recomputeDailyMetrics(day);
  }

  const metrics = mapDailyMetricsRow(metricsRow || {});
  const closureSnapshot = await computeDailyClosureSnapshot(day);
  const sedeClosureSnapshot = await computeDailySedeClosureSnapshot(day);
  const audit = await getCurrentAuditFields();
  const { error } = await supabase.from('daily_closures').upsert({
    id: day,
    fecha: day,
    status: 'closed',
    locked: true,
    planeados: closureSnapshot?.planeados ?? metrics.planned ?? 0,
    contratados: closureSnapshot?.contratados ?? metrics.expected ?? 0,
    asistencias: closureSnapshot?.registrados ?? 0,
    ausentismos: closureSnapshot?.ausentismos ?? metrics.absenteeism ?? 0,
    faltan: closureSnapshot?.faltan ?? 0,
    sobran: closureSnapshot?.sobran ?? 0,
    no_contratados: closureSnapshot?.noContratados ?? metrics.noContracted ?? 0,
    closed_by_uid: audit.created_by_uid,
    closed_by_email: audit.created_by_email
  }, { onConflict: 'id' });
  if (error) throw error;
  if (sedeClosureSnapshot.length) {
    const { error: sedeClosureError } = await supabase.from('daily_sede_closures').upsert(sedeClosureSnapshot, { onConflict: 'id' });
    if (sedeClosureError) throw sedeClosureError;
    await notifyTableReload('daily_sede_closures');
  }

  await runPostClosureTasks(day);

  const refreshedAfterClosure = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
  if (refreshedAfterClosure === null) {
    await recomputeSedeStatusSnapshot(day);
    await recomputeDailyMetrics(day);
  }

  await notifyTableReload('daily_closures');
  return { ok: true, results: [{ date: day, status: 'closed' }] };
}

async function runPostClosureTasks(day) {

  const { error } = await supabase.from('daily_metrics').update({ closed: true }).eq('fecha', day);
  if (error) throw error;
  await propagateIncapacitiesToNextDay(day);
}

async function finalizePendingAbsenteeismForClosure(day) {
  const audit = await getCurrentAuditFields();
  const [{ data: attendanceRows, error: attendanceError }, { data: replacementRows, error: replacementsError }, novedadesRows] = await Promise.all([
    supabase.from('attendance').select('*').eq('fecha', day),
    supabase.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('novedades', { select: 'codigo, codigo_novedad, nombre, reemplazo' })
  ]);
  if (attendanceError) throw attendanceError;
  if (replacementsError) throw replacementsError;

  const replacementRules = buildNovedadReplacementRules((novedadesRows || []).map(mapNovedadRow));
  const replacementMap = new Map(((replacementRows || []).map((row) => {
    const mapped = mapImportReplacementRow(row);
    return [metricReplacementKey(mapped), mapped];
  })));

  for (const raw of attendanceRows || []) {
    const row = mapAttendanceRow(raw);
    if (!metricAttendanceRequiresReplacement(row, replacementRules)) continue;
    const key = metricReplacementKey(row);
    const existing = replacementMap.get(key);
    if (existing && String(existing?.decision || '').trim().toLowerCase() !== 'reemplazo' && String(existing?.decision || '').trim()) {
      continue;
    }
    if (existing && String(existing?.decision || '').trim().toLowerCase() === 'reemplazo') continue;

    const recordId = buildDailyRecordId(day, row.documento, row.empleadoId);
    const { error: replacementError } = await supabase.from('import_replacements').upsert({
      id: recordId,
      fecha_operacion: day,
      fecha: day,
      empleado_id: row.empleadoId || null,
      documento: row.documento || null,
      nombre: row.nombre || null,
      sede_codigo: row.sedeCodigo || null,
      sede_nombre: row.sedeNombre || null,
      novedad_codigo: row.novedadCodigo || null,
      novedad_nombre: row.novedadNombre || row.novedad || null,
      decision: 'ausentismo',
      actor_uid: audit.created_by_uid,
      actor_email: audit.created_by_email
    }, { onConflict: 'id' });
    if (replacementError) throw replacementError;

    const { error: absenteeismError } = await supabase.from('absenteeism').upsert({
      id: recordId,
      fecha: day,
      empleado_id: row.empleadoId || null,
      documento: row.documento || null,
      nombre: row.nombre || null,
      sede_codigo: row.sedeCodigo || null,
      sede_nombre: row.sedeNombre || null,
      estado: 'confirmado',
      created_by_uid: audit.created_by_uid,
      created_by_email: audit.created_by_email
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
  }

  await cleanupNonProgrammedClosedOperationalAbsenteeism(day);
  await materializeClosedOperationalAbsenteeismForClosure(day, {
    actorUid: audit.created_by_uid,
    actorEmail: audit.created_by_email
  });
}

async function cleanupNonProgrammedClosedOperationalAbsenteeism(day) {
  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed === null) return 0;

  const { data: statusRows, error } = await supabase
    .from('employee_daily_status')
    .select('source_replacement_id, source_absenteeism_id, source_attendance_id, source_incapacity_id, tipo_personal, servicio_programado')
    .eq('fecha', day)
    .eq('tipo_personal', 'empleado')
    .eq('servicio_programado', false);
  if (error) throw error;

  const candidateRows = (statusRows || []).filter((row) => !row?.source_attendance_id && !row?.source_incapacity_id && (row?.source_replacement_id || row?.source_absenteeism_id));
  if (!candidateRows.length) return 0;

  const chunk = (items, size = 200) => {
    const output = [];
    for (let index = 0; index < items.length; index += size) output.push(items.slice(index, index + size));
    return output;
  };

  const replacementIds = [...new Set(candidateRows.map((row) => row?.source_replacement_id).filter(Boolean))];
  const absenteeismIds = [...new Set(candidateRows.map((row) => row?.source_absenteeism_id).filter(Boolean))];

  const cronReplacementIds = [];
  for (const batch of chunk(replacementIds)) {
    const { data, error: replacementError } = await supabase
      .from('import_replacements')
      .select('id, actor_email, decision')
      .in('id', batch);
    if (replacementError) throw replacementError;
    for (const row of data || []) {
      if (String(row?.actor_email || '').trim().toLowerCase() === 'cron@system' && String(row?.decision || '').trim().toLowerCase() === 'ausentismo') {
        cronReplacementIds.push(row.id);
      }
    }
  }

  const cronAbsenteeismIds = [];
  for (const batch of chunk(absenteeismIds)) {
    const { data, error: absenteeismError } = await supabase
      .from('absenteeism')
      .select('id, created_by_email')
      .in('id', batch);
    if (absenteeismError) throw absenteeismError;
    for (const row of data || []) {
      if (String(row?.created_by_email || '').trim().toLowerCase() === 'cron@system') {
        cronAbsenteeismIds.push(row.id);
      }
    }
  }

  for (const batch of chunk(cronReplacementIds)) {
    const { error: deleteError } = await supabase.from('import_replacements').delete().in('id', batch);
    if (deleteError) throw deleteError;
  }
  for (const batch of chunk(cronAbsenteeismIds)) {
    const { error: deleteError } = await supabase.from('absenteeism').delete().in('id', batch);
    if (deleteError) throw deleteError;
  }

  const removed = new Set([...cronReplacementIds, ...cronAbsenteeismIds]).size;
  if (removed > 0) {
    await refreshEmployeeDailyStatusSnapshot(day);
    await notifyTableReload('import_replacements');
    await notifyTableReload('absenteeism');
  }
  return removed;
}

async function materializeClosedOperationalAbsenteeismForClosure(day, { actorUid = null, actorEmail = null } = {}) {
  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed === null) return 0;

  const { data: statusRows, error } = await supabase
    .from('employee_daily_status')
    .select('employee_id, documento, nombre, sede_codigo, sede_nombre_snapshot, novedad_codigo, novedad_nombre, tipo_personal, servicio_programado, cuenta_pago_servicio')
    .eq('fecha', day)
    .eq('tipo_personal', 'empleado')
    .eq('servicio_programado', true)
    .eq('cuenta_pago_servicio', false);
  if (error) throw error;

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
      actor_uid: actorUid,
      actor_email: actorEmail
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
      created_by_uid: actorUid,
      created_by_email: actorEmail
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
    changed += 1;
  }

  if (changed > 0) {
    await notifyTableReload('import_replacements');
    await notifyTableReload('absenteeism');
  }
  return changed;
}

export { supabase };
