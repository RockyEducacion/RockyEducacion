import crypto from 'node:crypto';
import { config } from './config.js';
import { supabaseAdmin } from './supabase.js';

const PRIVILEGED_ROLES = ['superadmin', 'admin', 'editor', 'consultor', 'supervisor'];
const INCAPACITY_SUPPORT_BUCKET = 'incapacidades-soportes';
const MAX_SUPPORT_BYTES = 8 * 1024 * 1024;

function mapErrorMessage(error) {
  const code = String(error?.message || '').trim();

  switch (code) {
    case 'invalid_json':
      return 'La solicitud tiene un formato invalido.';
    case 'invalid_documento':
      return 'Ingresa un documento valido.';
    case 'invalid_last4':
      return 'Ingresa los ultimos 4 digitos del celular.';
    case 'employee_not_found':
      return 'No encontramos un empleado activo con ese documento.';
    case 'employee_phone_missing':
      return 'El empleado no tiene celular valido registrado. Contacta al administrador.';
    case 'employee_credentials_mismatch':
      return 'Los datos ingresados no coinciden con el registro del empleado.';
    case 'use_main_portal':
      return 'Este empleado tiene un perfil con acceso ampliado. Debe ingresar por el portal principal.';
    case 'missing_session':
    case 'session_not_found':
      return 'La sesion del portal de empleados no es valida o ya finalizo.';
    case 'session_expired':
      return 'La sesion expiro. Ingresa nuevamente.';
    case 'employee_inactive':
      return 'El empleado no se encuentra activo.';
    case 'invalid_date':
      return 'Debes indicar fechas validas para la incapacidad.';
    case 'invalid_date_range':
      return 'La fecha de terminacion no puede ser menor a la fecha de inicio.';
    case 'invalid_support':
      return 'Adjunta un soporte valido en PDF o imagen.';
    case 'support_too_large':
      return 'El soporte supera el tamano permitido.';
    case 'incapacity_overlap':
      return 'Ya existe una incapacidad activa que se cruza con ese rango.';
    case 'invalid_incapacity_id':
      return 'La incapacidad indicada no es valida.';
    case 'incapacity_not_found':
      return 'No encontramos la incapacidad seleccionada para este empleado.';
    default:
      return 'Ocurrio un error procesando el portal de empleados.';
  }
}

function mapStatusCode(error) {
  return Number(error?.statusCode || 500);
}

function sanitizeDocument(value) {
  return String(value || '').replace(/\D+/g, '').trim();
}

function sanitizeLast4(value) {
  return String(value || '').replace(/\D+/g, '').slice(-4);
}

function normalizePhoneDigits(value) {
  return String(value || '').replace(/\D+/g, '');
}

function phoneLast4(value) {
  const digits = normalizePhoneDigits(value);
  return digits.length >= 4 ? digits.slice(-4) : '';
}

function hashToken(token) {
  return crypto.createHash('sha256').update(String(token || ''), 'utf8').digest('hex');
}

function createSessionToken() {
  return crypto.randomBytes(32).toString('base64url');
}

function expiresAtIso() {
  const hours = Number(config.employeePortalSessionHours || 12);
  return new Date(Date.now() + hours * 60 * 60 * 1000).toISOString();
}

function getClientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',').map((part) => part.trim()).filter(Boolean)[0];
  return forwarded || String(req.socket?.remoteAddress || '').trim() || null;
}

function getUserAgent(req) {
  return String(req.headers['user-agent'] || '').trim() || null;
}

function getSessionTokenFromRequest(req) {
  const auth = String(req.headers.authorization || '').trim();
  if (auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  return '';
}

function buildEmployeeSessionPayload(sessionRow, employeeRow = null) {
  const employee = employeeRow || {};
  return {
    sessionId: sessionRow.id,
    employeeId: sessionRow.employee_id,
    documento: sessionRow.documento_snapshot || employee.documento || null,
    nombre: sessionRow.nombre_snapshot || employee.nombre || 'Empleado',
    telefonoLast4: sessionRow.telefono_last4_snapshot || phoneLast4(employee.telefono),
    expiresAt: sessionRow.expires_at,
    lastSeenAt: sessionRow.last_seen_at || null
  };
}

function employeePortalCors(req, res) {
  const origin = String(req.headers.origin || '').trim();
  if (!origin) return;
  if (config.employeePortalAllowedOrigins.length && !config.employeePortalAllowedOrigins.includes(origin)) return;

  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
}

async function createEmployeePortalAudit(payload) {
  const { error } = await supabaseAdmin.from('employee_portal_audit').insert({
    employee_id: payload.employee_id || null,
    session_id: payload.session_id || null,
    documento: payload.documento || null,
    action: payload.action,
    detail: payload.detail || {},
    ip: payload.ip || null,
    user_agent: payload.user_agent || null
  });
  if (error) throw error;
}

async function getEmployeeByDocument(documento) {
  const { data, error } = await supabaseAdmin
    .from('employees')
    .select('id,documento,nombre,telefono,estado')
    .eq('documento', documento)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function getEmployeeById(employeeId) {
  const { data, error } = await supabaseAdmin
    .from('employees')
    .select('id,documento,nombre,telefono,estado')
    .eq('id', employeeId)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function getPrivilegedProfileByDocument(documento) {
  const { data, error } = await supabaseAdmin
    .from('profiles')
    .select('id,email,display_name,role,estado')
    .eq('documento', documento)
    .eq('estado', 'activo')
    .in('role', PRIVILEGED_ROLES)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function insertEmployeePortalSession(payload) {
  const { data, error } = await supabaseAdmin
    .from('employee_portal_sessions')
    .insert(payload)
    .select('id,employee_id,documento_snapshot,nombre_snapshot,telefono_last4_snapshot,expires_at,last_seen_at,created_at')
    .single();
  if (error) throw error;
  return data;
}

async function getEmployeePortalSessionByHash(tokenHash) {
  const { data, error } = await supabaseAdmin
    .from('employee_portal_sessions')
    .select('id,employee_id,documento_snapshot,nombre_snapshot,telefono_last4_snapshot,expires_at,revoked_at,last_seen_at,created_at')
    .eq('token_hash', tokenHash)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function touchEmployeePortalSession(sessionId) {
  const { error } = await supabaseAdmin
    .from('employee_portal_sessions')
    .update({ last_seen_at: new Date().toISOString() })
    .eq('id', sessionId);
  if (error) throw error;
}

async function revokeEmployeePortalSession(sessionId) {
  const { error } = await supabaseAdmin
    .from('employee_portal_sessions')
    .update({ revoked_at: new Date().toISOString() })
    .eq('id', sessionId);
  if (error) throw error;
}

function handleEmployeePortalError(res, error) {
  sendPortalJson(res, mapStatusCode(error), {
    ok: false,
    error: mapErrorMessage(error),
    redirectMain: String(error?.message || '').trim() === 'use_main_portal'
  });
}

function sendPortalJson(res, statusCode, payload) {
  res.status(statusCode).json(payload);
}

function sanitizeIsoDate(value) {
  const raw = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : '';
}

function safeStoragePart(value, fallback = 'archivo') {
  const clean = String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .trim();
  return clean || fallback;
}

function extensionFromMimeType(value) {
  const mime = String(value || '').trim().toLowerCase();
  if (mime === 'application/pdf') return '.pdf';
  if (mime === 'image/jpeg') return '.jpg';
  if (mime === 'image/png') return '.png';
  if (mime === 'image/webp') return '.webp';
  return '';
}

function parseSupportDataUrl(dataUrl) {
  const raw = String(dataUrl || '').trim();
  const match = raw.match(/^data:([^;]+);base64,(.+)$/);
  if (!match) {
    const error = new Error('invalid_support');
    error.statusCode = 400;
    throw error;
  }
  const mimeType = String(match[1] || '').trim().toLowerCase();
  if (!['application/pdf', 'image/jpeg', 'image/png', 'image/webp'].includes(mimeType)) {
    const error = new Error('invalid_support');
    error.statusCode = 400;
    throw error;
  }
  const buffer = Buffer.from(match[2], 'base64');
  if (!buffer.length) {
    const error = new Error('invalid_support');
    error.statusCode = 400;
    throw error;
  }
  if (buffer.length > MAX_SUPPORT_BYTES) {
    const error = new Error('support_too_large');
    error.statusCode = 400;
    throw error;
  }
  return { mimeType, buffer };
}

async function uploadIncapacitySupportFile({ documento, fileName, dataUrl }) {
  const { mimeType, buffer } = parseSupportDataUrl(dataUrl);
  const baseName = safeStoragePart(String(fileName || '').replace(/\.[a-zA-Z0-9]+$/, ''), 'soporte');
  const extension = String(fileName || '').match(/\.[a-zA-Z0-9]+$/)?.[0]?.toLowerCase() || extensionFromMimeType(mimeType);
  const owner = safeStoragePart(documento || 'sin-documento');
  const storagePath = `portal-web/${owner}/${Date.now()}_${crypto.randomUUID()}_${baseName}${extension}`;
  const { error } = await supabaseAdmin
    .storage
    .from(INCAPACITY_SUPPORT_BUCKET)
    .upload(storagePath, buffer, {
      upsert: false,
      contentType: mimeType
    });
  if (error) throw error;
  const { data } = supabaseAdmin.storage.from(INCAPACITY_SUPPORT_BUCKET).getPublicUrl(storagePath);
  return {
    url: data?.publicUrl || '',
    name: String(fileName || '').trim() || `soporte${extension}`,
    mimeType,
    storagePath
  };
}

function mapIncapacityRow(row = {}) {
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
  const start = String(row?.fecha_inicio || row?.fechaInicio || '').trim();
  const end = String(row?.fecha_fin || row?.fechaFin || start).trim();
  if (!start && !end) return true;
  if (from && end && end < from) return false;
  if (to && start && start > to) return false;
  return true;
}

async function getActiveEmployeePortalContext(req, { ip, userAgent } = {}) {
  const token = getSessionTokenFromRequest(req);
  if (!token) {
    const error = new Error('missing_session');
    error.statusCode = 401;
    throw error;
  }

  const session = await getEmployeePortalSessionByHash(hashToken(token));
  if (!session || session.revoked_at) {
    const error = new Error('session_not_found');
    error.statusCode = 401;
    throw error;
  }

  if (new Date(session.expires_at).getTime() <= Date.now()) {
    await revokeEmployeePortalSession(session.id);
    await createEmployeePortalAudit({
      employee_id: session.employee_id,
      session_id: session.id,
      documento: session.documento_snapshot,
      action: 'employee_portal_session_expired',
      detail: {},
      ip,
      user_agent: userAgent
    });
    const error = new Error('session_expired');
    error.statusCode = 401;
    throw error;
  }

  const employee = await getEmployeeById(session.employee_id);
  if (!employee || String(employee.estado || '').trim().toLowerCase() !== 'activo') {
    await revokeEmployeePortalSession(session.id);
    await createEmployeePortalAudit({
      employee_id: session.employee_id,
      session_id: session.id,
      documento: session.documento_snapshot,
      action: 'employee_portal_session_revoked_employee_inactive',
      detail: {},
      ip,
      user_agent: userAgent
    });
    const error = new Error('employee_inactive');
    error.statusCode = 403;
    throw error;
  }

  const documento = session.documento_snapshot || employee.documento;
  const privilegedProfile = await getPrivilegedProfileByDocument(documento);
  if (privilegedProfile) {
    await revokeEmployeePortalSession(session.id);
    await createEmployeePortalAudit({
      employee_id: session.employee_id,
      session_id: session.id,
      documento,
      action: 'employee_portal_session_redirect_main',
      detail: {
        role: privilegedProfile.role || null,
        email: privilegedProfile.email || null
      },
      ip,
      user_agent: userAgent
    });
    const error = new Error('use_main_portal');
    error.statusCode = 403;
    throw error;
  }

  await touchEmployeePortalSession(session.id);
  return { session, employee };
}

export function registerEmployeePortalRoutes(app) {
  app.use([
    '/employee-login',
    '/api/employee-login',
    '/employee-me',
    '/api/employee-me',
    '/employee-incapacities',
    '/api/employee-incapacities',
    '/employee-logout',
    '/api/employee-logout'
  ], (req, res, next) => {
    employeePortalCors(req, res);
    if (req.method === 'OPTIONS') return res.status(204).end();
    return next();
  });

  app.post(['/employee-login', '/api/employee-login'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);

    try {
      const documento = sanitizeDocument(req.body?.documento);
      const last4 = sanitizeLast4(req.body?.last4);

      if (!documento) {
        const error = new Error('invalid_documento');
        error.statusCode = 400;
        throw error;
      }

      if (last4.length !== 4) {
        const error = new Error('invalid_last4');
        error.statusCode = 400;
        throw error;
      }

      const employee = await getEmployeeByDocument(documento);
      if (!employee || String(employee.estado || '').trim().toLowerCase() !== 'activo') {
        await createEmployeePortalAudit({
          employee_id: null,
          session_id: null,
          documento,
          action: 'employee_portal_login_denied_employee_not_found',
          detail: { reason: 'employee_not_found' },
          ip,
          user_agent: userAgent
        });
        const error = new Error('employee_not_found');
        error.statusCode = 403;
        throw error;
      }

      const employeePhoneLast4 = phoneLast4(employee.telefono);
      if (!employeePhoneLast4) {
        await createEmployeePortalAudit({
          employee_id: employee.id,
          session_id: null,
          documento,
          action: 'employee_portal_login_denied_phone_missing',
          detail: { reason: 'employee_phone_missing' },
          ip,
          user_agent: userAgent
        });
        const error = new Error('employee_phone_missing');
        error.statusCode = 403;
        throw error;
      }

      if (employeePhoneLast4 !== last4) {
        await createEmployeePortalAudit({
          employee_id: employee.id,
          session_id: null,
          documento,
          action: 'employee_portal_login_denied_bad_match',
          detail: { reason: 'employee_credentials_mismatch' },
          ip,
          user_agent: userAgent
        });
        const error = new Error('employee_credentials_mismatch');
        error.statusCode = 403;
        throw error;
      }

      const privilegedProfile = await getPrivilegedProfileByDocument(documento);
      if (privilegedProfile) {
        await createEmployeePortalAudit({
          employee_id: employee.id,
          session_id: null,
          documento,
          action: 'employee_portal_login_redirect_main',
          detail: {
            reason: 'use_main_portal',
            role: privilegedProfile.role || null,
            email: privilegedProfile.email || null
          },
          ip,
          user_agent: userAgent
        });
        const error = new Error('use_main_portal');
        error.statusCode = 403;
        throw error;
      }

      const token = createSessionToken();
      const session = await insertEmployeePortalSession({
        employee_id: employee.id,
        documento_snapshot: employee.documento,
        nombre_snapshot: employee.nombre || null,
        telefono_last4_snapshot: employeePhoneLast4,
        token_hash: hashToken(token),
        ip,
        user_agent: userAgent,
        last_seen_at: new Date().toISOString(),
        expires_at: expiresAtIso()
      });

      await createEmployeePortalAudit({
        employee_id: employee.id,
        session_id: session.id,
        documento,
        action: 'employee_portal_login_success',
        detail: {},
        ip,
        user_agent: userAgent
      });

      sendPortalJson(res, 200, {
        ok: true,
        token,
        session: buildEmployeeSessionPayload(session, employee)
      });
    } catch (error) {
      console.error('Error procesando employee-login:', error);
      handleEmployeePortalError(res, error);
    }
  });

  app.get(['/employee-me', '/api/employee-me'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);

    try {
      const token = getSessionTokenFromRequest(req);
      if (!token) {
        const error = new Error('missing_session');
        error.statusCode = 401;
        throw error;
      }

      const session = await getEmployeePortalSessionByHash(hashToken(token));
      if (!session || session.revoked_at) {
        const error = new Error('session_not_found');
        error.statusCode = 401;
        throw error;
      }

      if (new Date(session.expires_at).getTime() <= Date.now()) {
        await revokeEmployeePortalSession(session.id);
        await createEmployeePortalAudit({
          employee_id: session.employee_id,
          session_id: session.id,
          documento: session.documento_snapshot,
          action: 'employee_portal_session_expired',
          detail: {},
          ip,
          user_agent: userAgent
        });
        const error = new Error('session_expired');
        error.statusCode = 401;
        throw error;
      }

      const employee = await getEmployeeById(session.employee_id);
      if (!employee || String(employee.estado || '').trim().toLowerCase() !== 'activo') {
        await revokeEmployeePortalSession(session.id);
        await createEmployeePortalAudit({
          employee_id: session.employee_id,
          session_id: session.id,
          documento: session.documento_snapshot,
          action: 'employee_portal_session_revoked_employee_inactive',
          detail: {},
          ip,
          user_agent: userAgent
        });
        const error = new Error('employee_inactive');
        error.statusCode = 403;
        throw error;
      }

      const privilegedProfile = await getPrivilegedProfileByDocument(session.documento_snapshot || employee.documento);
      if (privilegedProfile) {
        await revokeEmployeePortalSession(session.id);
        await createEmployeePortalAudit({
          employee_id: session.employee_id,
          session_id: session.id,
          documento: session.documento_snapshot || employee.documento,
          action: 'employee_portal_session_redirect_main',
          detail: {
            role: privilegedProfile.role || null,
            email: privilegedProfile.email || null
          },
          ip,
          user_agent: userAgent
        });
        const error = new Error('use_main_portal');
        error.statusCode = 403;
        throw error;
      }

      await touchEmployeePortalSession(session.id);
      sendPortalJson(res, 200, {
        ok: true,
        session: buildEmployeeSessionPayload(session, employee)
      });
    } catch (error) {
      handleEmployeePortalError(res, error);
    }
  });

  app.get(['/employee-incapacities', '/api/employee-incapacities'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);

    try {
      const { session, employee } = await getActiveEmployeePortalContext(req, { ip, userAgent });
      const documento = sanitizeDocument(employee?.documento || session?.documento_snapshot || '');
      const dateFrom = sanitizeIsoDate(req.query?.dateFrom);
      const dateTo = sanitizeIsoDate(req.query?.dateTo);
      let query = supabaseAdmin
        .from('incapacitados')
        .select('*')
        .order('created_at', { ascending: false });

      if (employee?.id && documento) {
        query = query.or(`employee_id.eq.${employee.id},documento.eq.${documento}`);
      } else if (employee?.id) {
        query = query.eq('employee_id', employee.id);
      } else {
        query = query.eq('documento', documento);
      }

      const { data, error } = await query;
      if (error) throw error;

      sendPortalJson(res, 200, {
        ok: true,
        rows: (data || [])
          .filter((row) => !dateFrom || !dateTo || incapacityOverlapsRange(row, dateFrom, dateTo))
          .map(mapIncapacityRow)
      });
    } catch (error) {
      console.error('Error consultando incapacidades del portal:', error);
      handleEmployeePortalError(res, error);
    }
  });

  app.post(['/employee-incapacities', '/api/employee-incapacities'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);

    try {
      const { session, employee } = await getActiveEmployeePortalContext(req, { ip, userAgent });
      const fechaInicio = sanitizeIsoDate(req.body?.fechaInicio);
      const fechaFin = sanitizeIsoDate(req.body?.fechaFin);
      const soporte = req.body?.soporte && typeof req.body.soporte === 'object' ? req.body.soporte : null;

      if (!fechaInicio || !fechaFin) {
        const error = new Error('invalid_date');
        error.statusCode = 400;
        throw error;
      }
      if (fechaFin < fechaInicio) {
        const error = new Error('invalid_date_range');
        error.statusCode = 400;
        throw error;
      }
      if (!soporte?.dataUrl) {
        const error = new Error('invalid_support');
        error.statusCode = 400;
        throw error;
      }

      const documento = sanitizeDocument(employee?.documento || session?.documento_snapshot || '');
      const { data: overlapRows, error: overlapError } = await supabaseAdmin
        .from('incapacitados')
        .select('id')
        .eq('estado', 'activo')
        .or(`employee_id.eq.${employee.id},documento.eq.${documento}`)
        .lte('fecha_inicio', fechaFin)
        .gte('fecha_fin', fechaInicio)
        .limit(1);
      if (overlapError) throw overlapError;
      if (Array.isArray(overlapRows) && overlapRows.length) {
        const error = new Error('incapacity_overlap');
        error.statusCode = 409;
        throw error;
      }

      const uploadedSupport = await uploadIncapacitySupportFile({
        documento,
        fileName: soporte?.name || 'soporte.pdf',
        dataUrl: soporte?.dataUrl || ''
      });

      const { data, error } = await supabaseAdmin
        .from('incapacitados')
        .insert({
          employee_id: employee.id,
          documento,
          nombre: employee.nombre || session?.nombre_snapshot || 'Empleado',
          fecha_inicio: fechaInicio,
          fecha_fin: fechaFin,
          estado: 'activo',
          source: 'Incapacidad',
          canal_registro: 'portal_web',
          soporte_url: uploadedSupport.url,
          soporte_nombre: uploadedSupport.name,
          soporte_tipo: uploadedSupport.mimeType,
          soporte_storage_path: uploadedSupport.storagePath
        })
        .select('*')
        .single();
      if (error) throw error;

      await createEmployeePortalAudit({
        employee_id: employee.id,
        session_id: session.id,
        documento,
        action: 'employee_portal_incapacity_created',
        detail: {
          incapacity_id: data.id,
          fecha_inicio: fechaInicio,
          fecha_fin: fechaFin,
          canal_registro: 'portal_web'
        },
        ip,
        user_agent: userAgent
      });

      sendPortalJson(res, 200, {
        ok: true,
        row: mapIncapacityRow(data)
      });
    } catch (error) {
      console.error('Error creando incapacidad desde portal:', error);
      handleEmployeePortalError(res, error);
    }
  });

  app.post(['/employee-incapacities/:id/support', '/api/employee-incapacities/:id/support'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);

    try {
      const { session, employee } = await getActiveEmployeePortalContext(req, { ip, userAgent });
      const incapacityId = String(req.params?.id || '').trim();
      const soporte = req.body?.soporte && typeof req.body.soporte === 'object' ? req.body.soporte : null;

      if (!incapacityId) {
        const error = new Error('invalid_incapacity_id');
        error.statusCode = 400;
        throw error;
      }
      if (!soporte?.dataUrl) {
        const error = new Error('invalid_support');
        error.statusCode = 400;
        throw error;
      }

      const documento = sanitizeDocument(employee?.documento || session?.documento_snapshot || '');
      let query = supabaseAdmin
        .from('incapacitados')
        .select('*')
        .eq('id', incapacityId);

      if (employee?.id && documento) {
        query = query.or(`employee_id.eq.${employee.id},documento.eq.${documento}`);
      } else if (employee?.id) {
        query = query.eq('employee_id', employee.id);
      } else {
        query = query.eq('documento', documento);
      }

      const { data: incapacityRow, error: incapacityError } = await query.maybeSingle();
      if (incapacityError) throw incapacityError;
      if (!incapacityRow) {
        const error = new Error('incapacity_not_found');
        error.statusCode = 404;
        throw error;
      }

      const uploadedSupport = await uploadIncapacitySupportFile({
        documento,
        fileName: soporte?.name || 'soporte.pdf',
        dataUrl: soporte?.dataUrl || ''
      });

      const { data, error } = await supabaseAdmin
        .from('incapacitados')
        .update({
          soporte_url: uploadedSupport.url,
          soporte_nombre: uploadedSupport.name,
          soporte_tipo: uploadedSupport.mimeType,
          soporte_storage_path: uploadedSupport.storagePath
        })
        .eq('id', incapacityId)
        .select('*')
        .single();
      if (error) throw error;

      await createEmployeePortalAudit({
        employee_id: employee.id,
        session_id: session.id,
        documento,
        action: 'employee_portal_incapacity_support_uploaded',
        detail: {
          incapacity_id: data.id,
          soporte_nombre: uploadedSupport.name,
          soporte_tipo: uploadedSupport.mimeType
        },
        ip,
        user_agent: userAgent
      });

      sendPortalJson(res, 200, {
        ok: true,
        row: mapIncapacityRow(data)
      });
    } catch (error) {
      console.error('Error cargando soporte desde portal:', error);
      handleEmployeePortalError(res, error);
    }
  });

  app.post(['/employee-logout', '/api/employee-logout'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);

    try {
      const token = getSessionTokenFromRequest(req);
      if (token) {
        const session = await getEmployeePortalSessionByHash(hashToken(token));
        if (session && !session.revoked_at) {
          await revokeEmployeePortalSession(session.id);
          await createEmployeePortalAudit({
            employee_id: session.employee_id,
            session_id: session.id,
            documento: session.documento_snapshot,
            action: 'employee_portal_logout',
            detail: {},
            ip,
            user_agent: userAgent
          });
        }
      }
    } catch (error) {
      console.error('Error procesando employee-logout:', error);
    }

    sendPortalJson(res, 200, { ok: true });
  });
}
