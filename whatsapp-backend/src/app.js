
import crypto from 'node:crypto';
import express from 'express';
import QRCode from 'qrcode';
import { config } from './config.js';
import { buildEmployeeCertificatePdf, certificateFileName, normalizeCertificateType } from './certificates/certificate-service.js';
import { getActiveEmployeePortalContext, registerEmployeePortalRoutes } from './employee-portal.js';
import { supabaseAdmin } from './supabase.js';

const app = express();

app.use(express.json({
  limit: '12mb',
  verify: (req, _res, buffer) => {
    req.rawBody = buffer;
  }
}));

const SESSION = {
  IDLE: 'idle',
  AWAITING_DOCUMENT: 'awaiting_document',
  AWAITING_ACTION: 'awaiting_action',
  AWAITING_WORKING_SEDE_KEYWORD: 'awaiting_working_sede_keyword',
  AWAITING_WORKING_SEDE_SELECTION: 'awaiting_working_sede_selection',
  AWAITING_QR_ATTENDANCE_ACTION: 'awaiting_qr_attendance_action',
  AWAITING_QR_LOCATION: 'awaiting_qr_location',
  AWAITING_UPDATE_ACTION: 'awaiting_update_action',
  AWAITING_TRANSFER_KEYWORD: 'awaiting_transfer_keyword',
  AWAITING_TRANSFER_SELECTION: 'awaiting_transfer_selection',
  AWAITING_PHONE: 'awaiting_phone',
  AWAITING_NOVELTY: 'awaiting_novelty',
  AWAITING_DATE_START: 'awaiting_date_start',
  AWAITING_DATE_END: 'awaiting_date_end',
  COMPLETED: 'completed'
};

const NOVELTIES = {
  WORKING: { code: '1', label: 'Trabajando', absenteeism: false, requiresDates: false, tracksIncapacity: false, requiresSupport: false, dateContext: 'incapacidad' },
  ACCIDENT: { code: '2', label: 'Accidente Laboral', absenteeism: true, requiresDates: true, tracksIncapacity: true, requiresSupport: true, dateContext: 'incapacidad' },
  SICKNESS: { code: '3', label: 'Enfermedad General', absenteeism: true, requiresDates: true, tracksIncapacity: true, requiresSupport: true, dateContext: 'incapacidad' },
  CALAMITY: { code: '4', label: 'Calamidad', absenteeism: true, requiresDates: true, tracksIncapacity: true, requiresSupport: true, dateContext: 'incapacidad' },
  UNPAID_LEAVE: { code: '5', label: 'Licencia No Remunerada', absenteeism: true, requiresDates: true, tracksIncapacity: true, requiresSupport: false, dateContext: 'licencia' },
  PAID_LEAVE: { code: '6', label: 'Licencia Remunerada', absenteeism: true, requiresDates: true, tracksIncapacity: true, requiresSupport: false, dateContext: 'licencia' },
  COMPENSATORY: { code: '7', label: 'Compensatorio', absenteeism: false, requiresDates: false, tracksIncapacity: false, requiresSupport: false, dateContext: 'incapacidad' },
  VACATIONS: { code: '9', label: 'Vacaciones', absenteeism: true, requiresDates: true, tracksIncapacity: true, requiresSupport: false, dateContext: 'vacaciones' }
};

const MENU_IDS = {
  IDENTITY_YES: 'identity_yes',
  IDENTITY_NO: 'identity_no',
  UPDATE_DATA: 'update_data',
  ACTION_WORKING: 'action_working',
  ACTION_COMPENSATORY: 'action_compensatory',
  ACTION_NOVELTY: 'action_novelty',
  QR_ENTRY: 'qr_entry',
  QR_EXIT: 'qr_exit',
  UPDATE_TRANSFER: 'update_transfer',
  UPDATE_PHONE: 'update_phone',
  NOVELTY_SICKNESS: 'novelty_3',
  NOVELTY_ACCIDENT: 'novelty_2',
  NOVELTY_CALAMITY: 'novelty_4',
  NOVELTY_UNPAID: 'novelty_5',
  NOVELTY_PAID: 'novelty_6',
  NOVELTY_VACATIONS: 'novelty_9'
};

const NO_REGISTERED_MESSAGE = 'No estás registrado en nuestra base de datos, por favor comunícate con tu supervisor.';
const EMPLOYEE_PORTAL_URL = 'https://rockyedu.capcol.com.co/employee.html';

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

registerEmployeePortalRoutes(app);
registerAttendanceQrRoutes(app);
registerCertificateRoutes(app);

app.get(['/cron/close-daily-operation', '/api/cron/close-daily-operation'], async (req, res) => {
  try {
    assertCronAuthorized(req);
    const day = addDaysToIsoDate(currentDate(), -1) || currentDate();
    const result = await closeOperationDay(day);
    res.json({ ok: true, ...result });
  } catch (error) {
    const message = error?.message || 'cron_close_failed';
    const status = message === 'unauthorized_cron' ? 401 : 500;
    console.error('Error en cierre automatico diario:', error);
    res.status(status).json({ ok: false, error: message });
  }
});

app.get('/webhooks/whatsapp', (req, res) => {
  const mode = String(req.query['hub.mode'] || '');
  const token = String(req.query['hub.verify_token'] || '');
  const challenge = String(req.query['hub.challenge'] || '');

  if (mode === 'subscribe' && token === config.whatsappVerifyToken) {
    return res.status(200).send(challenge);
  }
  return res.status(403).send('Forbidden');
});

app.post('/webhooks/whatsapp', async (req, res) => {
  if (!isValidWhatsAppSignature(req)) {
    return res.status(401).json({ ok: false, error: 'invalid_signature' });
  }

  const messages = extractMessages(req.body);
  const statuses = extractStatuses(req.body);

  try {
    for (const status of statuses) {
      await storeIncomingEvent({ eventType: 'status', payload: status });
    }

    for (const message of messages) {
      const incomingId = await storeIncomingEvent({ eventType: 'message', payload: message });
      try {
        await processIncomingMessage(message);
        await markIncomingProcessed(incomingId, 'processed', null);
      } catch (error) {
        console.error('Error procesando mensaje WhatsApp:', error);
        await notifyProcessingError(message, error);
        await markIncomingProcessed(incomingId, 'failed', error.message || 'processing_failed');
      }
    }

    res.json({ ok: true });
  } catch (error) {
    console.error('Error general webhook WhatsApp:', error);
    res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

function registerAttendanceQrRoutes(appInstance) {
  appInstance.use([
    '/attendance-qr/devices',
    '/api/attendance-qr/devices',
    '/attendance-qr/daily',
    '/api/attendance-qr/daily',
    '/attendance-qr/scan',
    '/api/attendance-qr/scan'
  ], (req, res, next) => {
    attendanceQrCors(req, res);
    if (req.method === 'OPTIONS') return res.status(204).end();
    return next();
  });

  appInstance.post(['/attendance-qr/devices', '/api/attendance-qr/devices'], async (req, res) => {
    try {
      const profile = await requireAdminQrUser(req);
      const requestedSedeCodigo = String(req.body?.sedeCodigo || '').trim();
      const selectedSedeCodigos = normalizeSedeCodeList(req.body?.sedeCodigos);
      const sedeCodigos = selectedSedeCodigos.length ? selectedSedeCodigos : normalizeSedeCodeList(null, requestedSedeCodigo);
      const deviceName = String(req.body?.deviceName || '').trim();
      if (!sedeCodigos.length) return sendQrJson(res, 400, { ok: false, error: 'Selecciona al menos una sede.' });
      if (!deviceName) return sendQrJson(res, 400, { ok: false, error: 'Escribe el nombre de la tablet.' });

      const sedes = await getActiveQrSedesByCodes(sedeCodigos);
      if (sedes.length !== sedeCodigos.length) {
        return sendQrJson(res, 404, { ok: false, error: 'Todas las sedes seleccionadas deben estar activas y con QR activo.' });
      }
      const primarySede = sedes.find((row) => String(row.codigo || '').trim() === requestedSedeCodigo) || sedes[0];

      const deviceToken = createQrToken();
      const { data, error } = await supabaseAdmin
        .from('sede_devices')
        .insert({
          sede_id: primarySede.id || null,
          sede_codigo: primarySede.codigo,
          sede_nombre: primarySede.nombre || null,
          device_name: deviceName,
          token_hash: hashToken(deviceToken),
          estado: 'activo',
          created_by_uid: profile.id || null,
          created_by_email: profile.email || null
        })
        .select('id,sede_codigo,sede_nombre,device_name,estado,created_at')
        .single();
      if (error) throw error;
      try {
        await insertQrDeviceSites(data.id, sedes);
      } catch (linkError) {
        try { await supabaseAdmin.from('sede_devices').delete().eq('id', data.id); } catch (_) {}
        throw linkError;
      }

      sendQrJson(res, 200, {
        ok: true,
        device: mapQrDeviceRow(data),
        deviceToken
      });
    } catch (error) {
      console.error('Error creando dispositivo QR:', error);
      sendQrJson(res, qrStatusFromError(error), { ok: false, error: qrMessageFromError(error) });
    }
  });

  appInstance.patch(['/attendance-qr/devices/:deviceId/status', '/api/attendance-qr/devices/:deviceId/status'], async (req, res) => {
    try {
      const profile = await requireAdminQrUser(req);
      const deviceId = String(req.params?.deviceId || '').trim();
      const estado = String(req.body?.estado || '').trim().toLowerCase();
      if (!deviceId) return sendQrJson(res, 400, { ok: false, error: 'Selecciona una tablet.' });
      if (!['activo', 'inactivo'].includes(estado)) return sendQrJson(res, 400, { ok: false, error: 'Estado de tablet invalido.' });

      const now = new Date().toISOString();
      const patch = {
        estado,
        updated_at: now,
        last_modified_at: now,
        last_modified_by_uid: profile.id || null,
        last_modified_by_email: profile.email || null,
        revoked_at: estado === 'inactivo' ? now : null,
        revoked_by_uid: estado === 'inactivo' ? (profile.id || null) : null,
        revoked_by_email: estado === 'inactivo' ? (profile.email || null) : null
      };
      const { data, error } = await supabaseAdmin
        .from('sede_devices')
        .update(patch)
        .eq('id', deviceId)
        .select('id,sede_codigo,sede_nombre,device_name,estado,last_seen_at,revoked_at,created_at,created_by_email,last_modified_by_email,last_modified_at,revoked_by_email')
        .maybeSingle();
      if (error) throw error;
      if (!data?.id) return sendQrJson(res, 404, { ok: false, error: 'Tablet QR no encontrada.' });

      sendQrJson(res, 200, { ok: true, device: mapQrDeviceRow(data) });
    } catch (error) {
      console.error('Error actualizando tablet QR:', error);
      sendQrJson(res, qrStatusFromError(error), { ok: false, error: qrMessageFromError(error) });
    }
  });

  appInstance.get(['/attendance-qr/daily', '/api/attendance-qr/daily'], async (req, res) => {
    try {
      await requireQrRegistryUser(req);
      const date = String(req.query?.date || currentDate()).trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        return sendQrJson(res, 400, { ok: false, error: 'Fecha invalida.' });
      }
      const summary = await listDailyQrRecords(date);
      sendQrJson(res, 200, { ok: true, date, ...summary });
    } catch (error) {
      console.error('Error consultando registro diario QR:', error);
      sendQrJson(res, qrStatusFromError(error), { ok: false, error: qrMessageFromError(error) });
    }
  });

  appInstance.get(['/attendance-qr/image/:token', '/api/attendance-qr/image/:token'], async (req, res) => {
    try {
      const token = String(req.params?.token || '').trim();
      if (!token) return res.status(404).send('QR no encontrado');
      const tokenRow = await getQrTokenByHash(hashToken(token));
      if (!tokenRow) return res.status(404).send('QR no encontrado');

      const qrValue = buildQrScanValue(token);
      const png = await QRCode.toBuffer(qrValue, {
        type: 'png',
        errorCorrectionLevel: 'M',
        margin: 2,
        width: 512
      });
      res.setHeader('Content-Type', 'image/png');
      res.setHeader('Cache-Control', 'no-store');
      res.status(200).send(png);
    } catch (error) {
      console.error('Error generando imagen QR:', error);
      res.status(500).send('No se pudo generar el QR');
    }
  });

  appInstance.post(['/attendance-qr/scan', '/api/attendance-qr/scan'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);
    let auditPayload = { ip, user_agent: userAgent };

    try {
      const deviceToken = getQrDeviceTokenFromRequest(req);
      const qrToken = extractQrToken(req.body?.token || req.body?.qrValue || req.body?.value);
      if (!deviceToken) throw qrError('missing_device_token', 401);
      if (!qrToken) throw qrError('missing_qr_token', 400);

      const device = await getQrDeviceByToken(deviceToken);
      if (!device) throw qrError('device_not_found', 401);
      if (String(device.estado || '').trim().toLowerCase() !== 'activo' || device.revoked_at) throw qrError('device_inactive', 403);

      const tokenRow = await getQrTokenByHash(hashToken(qrToken));
      if (!tokenRow) throw qrError('qr_not_found', 404);

      auditPayload = {
        ...auditPayload,
        qr_token_id: tokenRow.id,
        device_id: device.id,
        action: tokenRow.action || null,
        fecha: tokenRow.fecha || null,
        employee_id: tokenRow.employee_id || null,
        documento: tokenRow.documento || null,
        sede_codigo: tokenRow.sede_codigo || null
      };

      if (!(await qrDeviceAllowsSede(device, tokenRow.sede_codigo))) throw qrError('sede_mismatch', 403);
      if (String(tokenRow.fecha || '').trim() !== currentDate()) throw qrError('wrong_day', 409);
      if (new Date(tokenRow.expires_at).getTime() <= Date.now()) throw qrError('qr_expired', 409);
      if (tokenRow.used_at) throw qrError('qr_used', 409);

      const employee = await findEmployeeByDocument(tokenRow.documento);
      if (!employee || String(employee.estado || '').trim().toLowerCase() !== 'activo') throw qrError('employee_inactive', 403);

      await validateQrActionReady(tokenRow);
      const claimedToken = await claimQrToken(tokenRow.id, device.id);
      if (!claimedToken) throw qrError('qr_used', 409);

      const result = tokenRow.action === 'exit'
        ? await registerQrExit({ tokenRow, device })
        : await registerQrEntry({ tokenRow, device, employee });

      await touchQrDevice(device.id);
      await insertQrScanAudit({ ...auditPayload, ok: true, reason: result.status || 'ok' });

      sendQrJson(res, 200, {
        ok: true,
        action: tokenRow.action,
        status: result.status,
        employee: {
          documento: tokenRow.documento,
          nombre: tokenRow.nombre || employee.nombre || null,
          phoneNumber: tokenRow.phone_number || null
        },
        sede: {
          codigo: tokenRow.sede_codigo,
          nombre: tokenRow.sede_nombre || null
        }
      });
    } catch (error) {
      console.error('Error escaneando QR:', error);
      await insertQrScanAudit({ ...auditPayload, ok: false, reason: String(error?.message || 'scan_failed') }).catch((auditError) => {
        console.error('Error guardando auditoria QR:', auditError);
      });
      sendQrJson(res, qrStatusFromError(error), { ok: false, error: qrMessageFromError(error), code: String(error?.message || 'scan_failed') });
    }
  });
}

function isValidWhatsAppSignature(req) {
  if (!config.whatsappAppSecret) return true;
  const signature = String(req.headers['x-hub-signature-256'] || '');
  if (!signature.startsWith('sha256=')) return false;
  const expected = `sha256=${crypto.createHmac('sha256', config.whatsappAppSecret).update(req.rawBody || '').digest('hex')}`;
  return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
}

function extractMessages(body = {}) {
  const entries = Array.isArray(body?.entry) ? body.entry : [];
  return entries.flatMap((entry) =>
    (Array.isArray(entry?.changes) ? entry.changes : []).flatMap((change) => {
      const value = change?.value || {};
      const metadata = value?.metadata || {};
      return (Array.isArray(value?.messages) ? value.messages : []).map((message) => ({ ...message, metadata }));
    })
  );
}

function extractStatuses(body = {}) {
  const entries = Array.isArray(body?.entry) ? body.entry : [];
  return entries.flatMap((entry) =>
    (Array.isArray(entry?.changes) ? entry.changes : []).flatMap((change) => {
      const value = change?.value || {};
      const metadata = value?.metadata || {};
      return (Array.isArray(value?.statuses) ? value.statuses : []).map((status) => ({ ...status, metadata }));
    })
  );
}

function buildDailyRecordId(date, documento = null, employeeId = null) {
  const day = String(date || '').trim();
  const doc = normalizeDocument(documento);
  if (day && doc) return `${day}_${doc}`;
  const employee = String(employeeId || '').trim();
  if (day && employee) return `${day}_${employee}`;
  return `${day}_${crypto.randomUUID()}`;
}

function hashToken(token) {
  return crypto.createHash('sha256').update(String(token || ''), 'utf8').digest('hex');
}

function createQrToken() {
  return crypto.randomBytes(32).toString('base64url');
}

function qrExpiresAtIso() {
  const minutes = Number(config.qrTokenMinutes || 10);
  return new Date(Date.now() + minutes * 60 * 1000).toISOString();
}

function buildQrScanValue(token) {
  return `${config.publicBackendUrl}/api/attendance-qr/token/${encodeURIComponent(token)}`;
}

function buildQrImageUrl(token) {
  return `${config.publicBackendUrl}/api/attendance-qr/image/${encodeURIComponent(token)}`;
}

function extractQrToken(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const marker = '/attendance-qr/token/';
  const markerIndex = raw.indexOf(marker);
  if (markerIndex >= 0) return decodeURIComponent(raw.slice(markerIndex + marker.length).split(/[?#]/)[0] || '').trim();
  return raw.replace(/^qr:/i, '').trim();
}

function getClientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',').map((part) => part.trim()).filter(Boolean)[0];
  return forwarded || String(req.socket?.remoteAddress || '').trim() || null;
}

function getUserAgent(req) {
  return String(req.headers['user-agent'] || '').trim() || null;
}

function getQrDeviceTokenFromRequest(req) {
  const header = String(req.headers['x-qr-device-token'] || '').trim();
  if (header) return header;
  return String(req.body?.deviceToken || '').trim();
}

function attendanceQrCors(req, res) {
  const origin = String(req.headers.origin || '').trim();
  if (!origin) return;
  if (config.employeePortalAllowedOrigins.length && !config.employeePortalAllowedOrigins.includes(origin)) return;

  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-QR-Device-Token');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
}

function sendQrJson(res, statusCode, payload) {
  res.status(statusCode).json(payload);
}

function registerCertificateRoutes(appInstance) {
  appInstance.use([
    '/employee-certificates',
    '/api/employee-certificates',
    '/certificates/verify/:code',
    '/api/certificates/verify/:code',
    '/certificates/employees/:employeeId',
    '/api/certificates/employees/:employeeId'
  ], (req, res, next) => {
    certificateCors(req, res);
    if (req.method === 'OPTIONS') return res.status(204).end();
    return next();
  });

  appInstance.post(['/employee-certificates', '/api/employee-certificates'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);
    try {
      const { session, employee: portalEmployee } = await getActiveEmployeePortalContext(req, { ip, userAgent });
      const type = normalizeCertificateType(req.body?.type);
      const { employee, cargo } = await loadCertificateContextByEmployeeId(portalEmployee.id);
      const verificationCode = await insertEmployeeCertificateAudit({
        employee,
        type,
        channel: 'employee_portal',
        requestedByEmployeeSessionId: session.id,
        ip,
        userAgent
      });
      const pdf = await buildEmployeeCertificatePdf({
        employee,
        cargo,
        type,
        verificationCode,
        verificationUrl: buildCertificateVerificationUrl(verificationCode)
      });
      sendCertificatePdf(res, pdf, certificateFileName(employee, type));
    } catch (error) {
      console.error('Error generando certificado desde portal:', error);
      sendCertificateError(res, error);
    }
  });

  appInstance.post(['/certificates/employees/:employeeId', '/api/certificates/employees/:employeeId'], async (req, res) => {
    const ip = getClientIp(req);
    const userAgent = getUserAgent(req);
    try {
      const profile = await requireCertificateAdminUser(req);
      const employeeId = String(req.params?.employeeId || '').trim();
      if (!employeeId) throw certificateError('invalid_employee', 400);
      const type = normalizeCertificateType(req.body?.type);
      const { employee, cargo } = await loadCertificateContextByEmployeeId(employeeId);
      const verificationCode = await insertEmployeeCertificateAudit({
        employee,
        type,
        channel: 'admin',
        requestedByProfileId: profile.id || null,
        requestedByEmail: profile.email || null,
        ip,
        userAgent
      });
      const pdf = await buildEmployeeCertificatePdf({
        employee,
        cargo,
        type,
        verificationCode,
        verificationUrl: buildCertificateVerificationUrl(verificationCode)
      });
      sendCertificatePdf(res, pdf, certificateFileName(employee, type));
    } catch (error) {
      console.error('Error generando certificado administrativo:', error);
      sendCertificateError(res, error);
    }
  });

  appInstance.get(['/certificates/verify/:code', '/api/certificates/verify/:code'], async (req, res) => {
    try {
      const code = normalizeCertificateVerificationCode(req.params?.code);
      if (!code) return sendCertificateVerificationHtml(res, 404, null);
      const row = await getCertificateAuditByVerificationCode(code);
      if (!row) return sendCertificateVerificationHtml(res, 404, null);
      return sendCertificateVerificationHtml(res, 200, row);
    } catch (error) {
      console.error('Error verificando certificado:', error);
      return sendCertificateVerificationHtml(res, 500, null, 'No fue posible verificar el certificado.');
    }
  });
}

function certificateCors(req, res) {
  const origin = String(req.headers.origin || '').trim();
  if (!origin) return;
  if (config.employeePortalAllowedOrigins.length && !config.employeePortalAllowedOrigins.includes(origin)) return;
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Disposition');
}

function sendCertificatePdf(res, pdf, filename) {
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.setHeader('Cache-Control', 'no-store');
  res.status(200).send(Buffer.from(pdf));
}

function sendCertificateError(res, error) {
  const status = Number(error?.statusCode || 500);
  res.status(status).json({
    ok: false,
    error: certificateMessageFromError(error),
    code: String(error?.message || 'certificate_failed')
  });
}

function certificateError(message, statusCode = 400) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function certificateMessageFromError(error) {
  switch (String(error?.message || '').trim()) {
    case 'missing_auth':
      return 'Debes iniciar sesion para generar certificados.';
    case 'forbidden':
      return 'No tienes permiso para generar certificados.';
    case 'invalid_employee':
      return 'Selecciona un empleado valido.';
    case 'employee_not_found':
      return 'No encontramos el empleado seleccionado.';
    case 'employee_inactive':
      return 'Solo se pueden generar certificados de empleados activos.';
    case 'missing_salary':
      return 'El cargo del empleado no tiene salario configurado.';
    case 'verification_code_failed':
      return 'No fue posible crear el codigo de verificacion del certificado.';
    default:
      return 'No fue posible generar el certificado laboral.';
  }
}

async function requireCertificateAdminUser(req) {
  return requireQrUser(req, ({ role, profile }) => (
    ['superadmin', 'admin'].includes(role) || (role === 'supervisor' && profile?.supervisor_eligible === true)
  ));
}

async function loadCertificateContextByEmployeeId(employeeId) {
  const id = String(employeeId || '').trim();
  if (!id) throw certificateError('invalid_employee', 400);
  const { data: employee, error } = await supabaseAdmin
    .from('employees')
    .select('id,codigo,documento,nombre,cargo_codigo,cargo_nombre,fecha_ingreso,estado')
    .eq('id', id)
    .maybeSingle();
  if (error) throw error;
  if (!employee?.id) throw certificateError('employee_not_found', 404);
  if (String(employee.estado || '').trim().toLowerCase() !== 'activo') throw certificateError('employee_inactive', 409);
  employee.fecha_ingreso = await resolveCertificateEmploymentStartDate(employee);
  const cargo = await loadCertificateCargo(employee);
  return { employee, cargo };
}

async function resolveCertificateEmploymentStartDate(employee = {}) {
  const historyRows = await loadCertificateEmployeeHistoryRows(employee);
  const candidates = [
    employee?.fecha_ingreso,
    ...historyRows.map((row) => row?.fecha_ingreso)
  ];
  const earliest = earliestDateCandidate(candidates);
  return earliest?.value || employee?.fecha_ingreso || null;
}

async function loadCertificateEmployeeHistoryRows(employee = {}) {
  const employeeId = String(employee?.id || '').trim();
  const documento = String(employee?.documento || '').trim();
  const queries = [];
  if (employeeId) {
    queries.push(
      supabaseAdmin
        .from('employee_cargo_history')
        .select('id,fecha_ingreso')
        .eq('employee_id', employeeId)
    );
  }
  if (documento) {
    queries.push(
      supabaseAdmin
        .from('employee_cargo_history')
        .select('id,fecha_ingreso')
        .eq('documento', documento)
    );
  }
  if (!queries.length) return [];

  const results = await Promise.all(queries);
  const rowsById = new Map();
  for (const { data, error } of results) {
    if (error) throw error;
    for (const row of (Array.isArray(data) ? data : [])) {
      const key = String(row?.id || `${row?.fecha_ingreso || ''}-${rowsById.size}`);
      if (!rowsById.has(key)) rowsById.set(key, row);
    }
  }
  return [...rowsById.values()];
}

function earliestDateCandidate(values = []) {
  return (Array.isArray(values) ? values : [])
    .map((value) => ({ value, iso: toISODate(value) }))
    .filter((candidate) => candidate.iso)
    .sort((left, right) => left.iso.localeCompare(right.iso))[0] || null;
}

async function loadCertificateCargo(employee) {
  const cargoCodigo = String(employee?.cargo_codigo || '').trim();
  const cargoNombre = String(employee?.cargo_nombre || '').trim();
  if (!cargoCodigo && !cargoNombre) return null;
  let query = supabaseAdmin.from('cargos').select('codigo,nombre,salario');
  if (cargoCodigo) query = query.eq('codigo', cargoCodigo);
  else query = query.eq('nombre', cargoNombre);
  const { data, error } = await query.maybeSingle();
  if (error) throw error;
  return data || null;
}

async function insertEmployeeCertificateAudit({ employee, type, channel, requestedByProfileId = null, requestedByEmail = null, requestedByEmployeeSessionId = null, ip = null, userAgent = null }) {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const verificationCode = createCertificateVerificationCode();
    const { data, error } = await supabaseAdmin.from('employee_certificate_audit').insert({
      employee_id: employee?.id || null,
      employee_codigo: employee?.codigo || null,
      documento: employee?.documento || null,
      nombre: employee?.nombre || null,
      verification_code: verificationCode,
      certificate_type: type,
      channel,
      requested_by_profile_id: requestedByProfileId,
      requested_by_email: requestedByEmail,
      requested_by_employee_session_id: requestedByEmployeeSessionId,
      ip,
      user_agent: userAgent
    }).select('verification_code').single();
    if (!error && data?.verification_code) return data.verification_code;
    if (String(error?.code || '') !== '23505') throw error;
  }
  throw certificateError('verification_code_failed', 500);
}

function createCertificateVerificationCode() {
  return crypto.randomBytes(9).toString('base64url').toUpperCase();
}

function buildCertificateVerificationUrl(code) {
  return `${config.publicBackendUrl}/api/certificates/verify/${encodeURIComponent(code)}`;
}

function normalizeCertificateVerificationCode(value) {
  return String(value || '').replace(/[^a-zA-Z0-9_-]+/g, '').trim().toUpperCase();
}

async function getCertificateAuditByVerificationCode(code) {
  const { data, error } = await supabaseAdmin
    .from('employee_certificate_audit')
    .select('verification_code,documento,nombre,certificate_type,channel,created_at')
    .eq('verification_code', code)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

function sendCertificateVerificationHtml(res, statusCode, row, customMessage = '') {
  const ok = statusCode === 200 && row;
  const title = ok ? 'Certificado valido' : 'Certificado no encontrado';
  const message = customMessage || (ok
    ? 'Certificado NO VALIDO'
    : 'No encontramos un certificado emitido con este codigo de verificacion.');
  const typeLabel = row?.certificate_type === 'with_salary' ? 'Laboral con salario' : 'Laboral basico';
  const channelLabel = row?.channel === 'employee_portal' ? 'Portal de empleados' : 'Administrativo';
  const issuedAt = row?.created_at ? new Date(row.created_at).toLocaleString('es-CO', { timeZone: 'America/Bogota' }) : '-';
  const html = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    body{font-family:Arial,sans-serif;background:#f5f7fb;color:#1f2933;margin:0;padding:32px;}
    main{max-width:680px;margin:0 auto;background:#fff;border:1px solid #e4e7ec;border-radius:8px;padding:28px;box-shadow:0 10px 30px rgba(15,23,42,.08);}
    h1{margin:0 0 12px;font-size:24px;}
    p{line-height:1.5;}
    dl{display:grid;grid-template-columns:180px 1fr;gap:10px 16px;margin-top:24px;}
    dt{font-weight:700;color:#475467;}
    dd{margin:0;}
    .ok{color:#047857;}
    .bad{color:#b42318;}
  </style>
</head>
<body>
  <main>
    <h1 class="${ok ? 'ok' : 'bad'}">${escapeHtml(title)}</h1>
    <p>${escapeHtml(message)}</p>
    ${ok ? `<dl>
      <dt>Codigo</dt><dd>${escapeHtml(row.verification_code || '')}</dd>
      <dt>Empleado</dt><dd>${escapeHtml(row.nombre || '-')}</dd>
      <dt>Documento</dt><dd>${escapeHtml(maskDocument(row.documento))}</dd>
      <dt>Tipo</dt><dd>${escapeHtml(typeLabel)}</dd>
      <dt>Canal</dt><dd>${escapeHtml(channelLabel)}</dd>
      <dt>Fecha de emision</dt><dd>${escapeHtml(issuedAt)}</dd>
    </dl>` : ''}
  </main>
</body>
</html>`;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.status(statusCode).send(html);
}

function maskDocument(value) {
  const raw = String(value || '').trim();
  if (raw.length <= 4) return raw || '-';
  return `${'*'.repeat(Math.max(0, raw.length - 4))}${raw.slice(-4)}`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function qrError(message, statusCode = 400) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

function qrStatusFromError(error) {
  return Number(error?.statusCode || 500);
}

function qrMessageFromError(error) {
  switch (String(error?.message || '').trim()) {
    case 'missing_auth':
      return 'Debes iniciar sesion para realizar esta accion.';
    case 'forbidden':
      return 'No tienes permiso para administrar tablets QR.';
    case 'missing_device_token':
      return 'La tablet no esta activada.';
    case 'device_not_found':
      return 'La tablet no esta autorizada.';
    case 'device_inactive':
      return 'La tablet esta inactiva o revocada.';
    case 'missing_qr_token':
      return 'No se detecto un QR valido.';
    case 'qr_not_found':
      return 'QR no encontrado.';
    case 'sede_mismatch':
      return 'El QR pertenece a otra sede.';
    case 'wrong_day':
      return 'El QR no corresponde al dia actual.';
    case 'qr_expired':
      return 'El QR expiro. Solicita uno nuevo por WhatsApp.';
    case 'qr_used':
      return 'El QR ya fue usado.';
    case 'employee_inactive':
      return 'El empleado no esta activo.';
    case 'entry_exists':
      return 'El ingreso de hoy ya esta registrado.';
    case 'exit_requires_entry':
      return 'Primero debe existir un ingreso del dia.';
    case 'exit_exists':
      return 'La salida de hoy ya esta registrada.';
    default:
      return 'No se pudo procesar el QR.';
  }
}

function mapQrDeviceRow(row = {}) {
  return {
    id: row.id,
    sedeCodigo: row.sede_codigo || null,
    sedeNombre: row.sede_nombre || null,
    deviceName: row.device_name || null,
    estado: row.estado || 'activo',
    lastSeenAt: row.last_seen_at || null,
    createdAt: row.created_at || null
  };
}

async function requireAdminQrUser(req) {
  return requireQrUser(req, ({ role, profile }) => (
    ['superadmin', 'admin'].includes(role) || (role === 'supervisor' && profile?.supervisor_eligible === true)
  ));
}

async function requireQrRegistryUser(req) {
  return requireQrUser(req, ({ role, profile }) => (
    ['superadmin', 'admin', 'editor'].includes(role) || (role === 'supervisor' && profile?.supervisor_eligible === true)
  ));
}

async function requireQrUser(req, canAccess) {
  const auth = String(req.headers.authorization || '').trim();
  const token = auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : '';
  if (!token) throw qrError('missing_auth', 401);

  const { data: userData, error: userError } = await supabaseAdmin.auth.getUser(token);
  if (userError || !userData?.user?.id) throw qrError('missing_auth', 401);

  const { data: profile, error: profileError } = await supabaseAdmin
    .from('profiles')
    .select('id,email,role,estado,supervisor_eligible')
    .eq('id', userData.user.id)
    .maybeSingle();
  if (profileError) throw profileError;
  const role = String(profile?.role || '').trim().toLowerCase();
  const status = String(profile?.estado || 'activo').trim().toLowerCase();
  const isPrivileged = typeof canAccess === 'function' ? canAccess({ role, profile }) : false;
  if (status !== 'activo' || !isPrivileged) throw qrError('forbidden', 403);
  return profile;
}

async function getSedeByCode(codigo) {
  const code = String(codigo || '').trim();
  if (!code) return null;
  const { data, error } = await supabaseAdmin.from('sedes').select('*').eq('codigo', code).maybeSingle();
  if (error) throw error;
  return data || null;
}

function normalizeSedeCodeList(value, fallback = '') {
  const list = Array.isArray(value) ? value : [value];
  if (fallback) list.unshift(fallback);
  return [...new Set(list.map((item) => String(item || '').trim()).filter(Boolean))];
}

async function getActiveQrSedesByCodes(codes = []) {
  const normalized = normalizeSedeCodeList(codes);
  if (!normalized.length) return [];
  const { data, error } = await supabaseAdmin
    .from('sedes')
    .select('id,codigo,nombre,qr_enabled,estado')
    .in('codigo', normalized);
  if (error) throw error;
  const byCode = new Map((data || [])
    .filter((row) => row?.qr_enabled === true && String(row?.estado || '').trim().toLowerCase() === 'activo')
    .map((row) => [String(row.codigo || '').trim(), row]));
  return normalized.map((code) => byCode.get(code)).filter(Boolean);
}

async function insertQrDeviceSites(deviceId, sedes = []) {
  if (!deviceId || !sedes.length) return;
  const rows = sedes.map((sede) => ({
    device_id: deviceId,
    sede_id: sede.id || null,
    sede_codigo: sede.codigo,
    sede_nombre: sede.nombre || null
  }));
  const { error } = await supabaseAdmin
    .from('sede_device_sites')
    .upsert(rows, { onConflict: 'device_id,sede_codigo' });
  if (error) throw error;
}

async function qrDeviceAllowsSede(device = {}, sedeCodigo = '') {
  const code = String(sedeCodigo || '').trim();
  if (!device?.id || !code) return false;
  const fallbackAllowed = String(device.sede_codigo || '').trim() === code;
  const { data, error } = await supabaseAdmin
    .from('sede_device_sites')
    .select('sede_codigo')
    .eq('device_id', device.id);
  if (error) {
    console.error('Error consultando sedes autorizadas de tablet QR:', error);
    return fallbackAllowed;
  }
  if (!Array.isArray(data) || !data.length) return fallbackAllowed;
  return data.some((row) => String(row?.sede_codigo || '').trim() === code);
}

async function getQrTokenByHash(tokenHash) {
  const { data, error } = await supabaseAdmin
    .from('attendance_qr_tokens')
    .select('*')
    .eq('token_hash', tokenHash)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function getQrDeviceByToken(deviceToken) {
  const { data, error } = await supabaseAdmin
    .from('sede_devices')
    .select('*')
    .eq('token_hash', hashToken(deviceToken))
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function touchQrDevice(deviceId) {
  if (!deviceId) return;
  const { error } = await supabaseAdmin
    .from('sede_devices')
    .update({ last_seen_at: new Date().toISOString() })
    .eq('id', deviceId);
  if (error) throw error;
}

async function claimQrToken(tokenId, deviceId) {
  const { data, error } = await supabaseAdmin
    .from('attendance_qr_tokens')
    .update({
      used_at: new Date().toISOString(),
      used_by_device_id: deviceId
    })
    .eq('id', tokenId)
    .is('used_at', null)
    .select('id')
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function validateQrActionReady(tokenRow) {
  return validateQrActionAvailability({
    action: tokenRow.action,
    fecha: tokenRow.fecha,
    documento: tokenRow.documento
  });
}

async function validateQrActionAvailability({ action, fecha, documento }) {
  if (action === 'exit') {
    const { data: attendanceRow, error: attendanceError } = await supabaseAdmin
      .from('attendance')
      .select('id,created_at')
      .eq('fecha', fecha)
      .eq('documento', documento)
      .limit(1)
      .maybeSingle();
    if (attendanceError) throw attendanceError;
    if (!attendanceRow?.id) throw qrError('exit_requires_entry', 409);

    const { data: exitRow, error: exitError } = await supabaseAdmin
      .from('employee_daily_exits')
      .select('id')
      .eq('fecha', fecha)
      .eq('documento', documento)
      .limit(1)
      .maybeSingle();
    if (exitError) throw exitError;
    if (exitRow?.id) throw qrError('exit_exists', 409);
    return attendanceRow;
  }

  const { data: attendanceRow, error } = await supabaseAdmin
    .from('attendance')
    .select('id')
    .eq('fecha', fecha)
    .eq('documento', documento)
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  if (attendanceRow?.id) throw qrError('entry_exists', 409);
  return null;
}

async function registerQrEntry({ tokenRow, employee }) {
  const attendanceId = buildDailyRecordId(tokenRow.fecha, tokenRow.documento, tokenRow.employee_id);
  const { error } = await supabaseAdmin.from('attendance').upsert({
    id: attendanceId,
    fecha: tokenRow.fecha,
    empleado_id: tokenRow.employee_id,
    documento: tokenRow.documento,
    nombre: tokenRow.nombre || employee?.nombre || null,
    sede_codigo: tokenRow.sede_codigo,
    sede_nombre: tokenRow.sede_nombre || null,
    asistio: true,
    novedad: NOVELTIES.WORKING.code
  }, { onConflict: 'id' });
  if (error) throw error;
  await clearDailyOperationalAbsenceArtifacts(attendanceId);
  await refreshOperationalState(tokenRow.fecha);
  return { status: 'entry_registered', attendanceId };
}

async function registerQrExit({ tokenRow, device }) {
  const attendanceRow = await validateQrActionReady(tokenRow);
  const exitId = buildDailyRecordId(tokenRow.fecha, tokenRow.documento, tokenRow.employee_id);
  const { error } = await supabaseAdmin.from('employee_daily_exits').insert({
    id: exitId,
    fecha: tokenRow.fecha,
    employee_id: tokenRow.employee_id,
    documento: tokenRow.documento,
    nombre: tokenRow.nombre || null,
    sede_codigo: tokenRow.sede_codigo,
    sede_nombre: tokenRow.sede_nombre || null,
    qr_token_id: tokenRow.id,
    device_id: device.id,
    entry_attendance_id: attendanceRow?.id || null
  });
  if (error) {
    if (error.code === '23505') throw qrError('exit_exists', 409);
    throw error;
  }
  return { status: 'exit_registered', exitId };
}

async function insertQrScanAudit(payload = {}) {
  const { error } = await supabaseAdmin.from('attendance_qr_scans').insert({
    qr_token_id: payload.qr_token_id || null,
    device_id: payload.device_id || null,
    action: payload.action || null,
    fecha: payload.fecha || null,
    employee_id: payload.employee_id || null,
    documento: payload.documento || null,
    sede_codigo: payload.sede_codigo || null,
    ok: payload.ok === true,
    reason: payload.reason || null,
    ip: payload.ip || null,
    user_agent: payload.user_agent || null
  });
  if (error) throw error;
}

async function listDailyQrRecords(date) {
  const day = String(date || '').trim();
  const [
    { data: tokenRows, error: tokenError },
    { data: exitRows, error: exitError },
    { data: sedeRows, error: sedeError },
    { data: incapacityRows, error: incapacityError },
    { data: attendanceRows, error: attendanceError }
  ] = await Promise.all([
    supabaseAdmin
      .from('attendance_qr_tokens')
      .select('*')
      .eq('fecha', day)
      .not('used_at', 'is', null)
      .order('used_at', { ascending: true }),
    supabaseAdmin
      .from('employee_daily_exits')
      .select('*')
      .eq('fecha', day)
      .order('exit_at', { ascending: true }),
    supabaseAdmin
      .from('sedes')
      .select('codigo,nombre,dependencia_codigo,dependencia_nombre,zona_codigo,zona_nombre,qr_enabled,estado')
      .eq('qr_enabled', true),
    supabaseAdmin
      .from('incapacitados')
      .select('employee_id,documento,nombre,source,fecha_inicio,fecha_fin')
      .eq('estado', 'activo')
      .lte('fecha_inicio', day)
      .gte('fecha_fin', day),
    supabaseAdmin
      .from('attendance')
      .select('id,empleado_id,documento,nombre,sede_codigo,sede_nombre,asistio,novedad,created_at')
      .eq('fecha', day)
  ]);
  if (tokenError) throw tokenError;
  if (exitError) throw exitError;
  if (sedeError) throw sedeError;
  if (incapacityError) throw incapacityError;
  if (attendanceError) throw attendanceError;

  const tokens = Array.isArray(tokenRows) ? tokenRows : [];
  const exits = Array.isArray(exitRows) ? exitRows : [];
  const incapacities = Array.isArray(incapacityRows) ? incapacityRows : [];
  let attendance = Array.isArray(attendanceRows) ? attendanceRows : [];
  const qrSedes = (Array.isArray(sedeRows) ? sedeRows : [])
    .filter((row) => String(row?.estado || 'activo').trim().toLowerCase() === 'activo');
  const qrSedesByCode = new Map(qrSedes.map((row) => [String(row.codigo || '').trim(), row]));
  const qrSedeCodes = [...qrSedesByCode.keys()].filter(Boolean);
  attendance = attendance.filter((row) => (
    qrSedesByCode.has(String(row?.sede_codigo || '').trim())
    && (row?.asistio === true || String(row?.novedad || '').trim())
  ));
  let statusRows = [];
  let pendingStatusRows = [];
  let registeredStatusRows = [];
  if (qrSedeCodes.length) {
    const { data: dailyStatusRows, error: statusError } = await supabaseAdmin
      .from('employee_daily_status')
      .select('employee_id,documento,nombre,sede_codigo,sede_nombre_snapshot,zona_codigo_snapshot,zona_nombre_snapshot,dependencia_codigo_snapshot,dependencia_nombre_snapshot,tipo_personal,servicio_programado,estado_dia,novedad_nombre,source_incapacity_id')
      .eq('fecha', day)
      .eq('tipo_personal', 'empleado')
      .in('sede_codigo', qrSedeCodes)
      .order('sede_nombre_snapshot', { ascending: true })
      .order('nombre', { ascending: true });
    if (statusError) throw statusError;
    statusRows = Array.isArray(dailyStatusRows) ? dailyStatusRows : [];
    pendingStatusRows = statusRows.filter((row) => row?.servicio_programado === true);
    registeredStatusRows = statusRows.filter((row) => (
      row?.source_incapacity_id
      || ['incapacidad', 'vacaciones', 'compensatorio'].includes(String(row?.estado_dia || '').trim().toLowerCase())
    ));
  }
  const employeeIds = [...new Set([
    ...tokens.map((row) => row?.employee_id).filter(Boolean),
    ...exits.map((row) => row?.employee_id).filter(Boolean),
    ...attendance.map((row) => row?.empleado_id).filter(Boolean),
    ...statusRows.map((row) => row?.employee_id).filter(Boolean)
  ])];

  const employeesById = new Map();
  if (employeeIds.length) {
    const { data: employees, error: employeesError } = await supabaseAdmin
      .from('employees')
      .select('id,documento,nombre,telefono,sede_codigo,sede_nombre')
      .in('id', employeeIds);
    if (employeesError) throw employeesError;
    (employees || []).forEach((employee) => employeesById.set(String(employee.id), employee));
  }

  const tokenById = new Map(tokens.map((row) => [String(row.id), row]));
  const rowMap = new Map();
  const keyFor = (row = {}) => String(row?.employee_id || row?.documento || '').trim();
  const ensureRow = (source = {}) => {
    const key = keyFor(source);
    if (!key) return null;
    if (!rowMap.has(key)) {
      const employee = employeesById.get(String(source.employee_id || '')) || {};
      rowMap.set(key, {
        employeeId: source.employee_id || null,
        documento: source.documento || employee.documento || null,
        nombre: source.nombre || employee.nombre || null,
        sedeCodigo: source.sede_codigo || employee.sede_codigo || null,
        sedeNombre: source.sede_nombre || employee.sede_nombre || null,
        employeePhone: employee.telefono || null,
        entryAt: null,
        entryPhone: null,
        entryDistanceMeters: null,
        entrySource: null,
        entryLabel: null,
        exitAt: null,
        exitPhone: null,
        exitDistanceMeters: null
      });
    }
    return rowMap.get(key);
  };

  tokens.filter((row) => row?.action === 'entry').forEach((tokenRow) => {
    const row = ensureRow(tokenRow);
    if (!row) return;
    row.entryAt = tokenRow.used_at || null;
    row.entryPhone = tokenRow.phone_number || null;
    row.entryDistanceMeters = tokenRow.request_distance_meters == null ? null : Number(tokenRow.request_distance_meters);
  });
  const entryKeys = new Set(
    tokens
      .filter((row) => row?.action === 'entry')
      .flatMap((row) => [
        row?.employee_id ? `id:${String(row.employee_id).trim()}` : '',
        row?.documento ? `doc:${String(row.documento).trim()}` : ''
      ])
      .filter(Boolean)
  );
  attendance.forEach((attendanceRow) => {
    const row = ensureRow({
      employee_id: attendanceRow.empleado_id || null,
      documento: attendanceRow.documento || null,
      nombre: attendanceRow.nombre || null,
      sede_codigo: attendanceRow.sede_codigo || null,
      sede_nombre: attendanceRow.sede_nombre || null
    });
    if (!row) return;
    if (!row.entryAt) row.entryAt = attendanceRow.created_at || null;
    if (!row.entrySource) row.entrySource = 'attendance';
    const noveltyCode = String(attendanceRow.novedad || '').trim();
    const noveltyLabel = noveltyLabelByCode(noveltyCode);
    if (noveltyLabel && noveltyCode !== NOVELTIES.WORKING.code) row.entryLabel = noveltyLabel;
    else if (noveltyCode && noveltyCode !== NOVELTIES.WORKING.code) row.entryLabel = `Novedad ${noveltyCode}`;
  });
  const attendanceKeys = new Set(
    attendance
      .flatMap((row) => [
        row?.empleado_id ? `id:${String(row.empleado_id).trim()}` : '',
        row?.documento ? `doc:${String(row.documento).trim()}` : ''
      ])
      .filter(Boolean)
  );
  const incapacityByKey = new Map();
  const setIncapacityKey = (key, row) => {
    if (key && !incapacityByKey.has(key)) incapacityByKey.set(key, row);
  };
  incapacities.forEach((row) => {
    setIncapacityKey(row?.employee_id ? `id:${String(row.employee_id).trim()}` : '', row);
    setIncapacityKey(row?.documento ? `doc:${String(row.documento).trim()}` : '', row);
  });
  const incapacityKeys = new Set(
    incapacities
      .flatMap((row) => [
        row?.employee_id ? `id:${String(row.employee_id).trim()}` : '',
        row?.documento ? `doc:${String(row.documento).trim()}` : ''
      ])
      .filter(Boolean)
  );
  registeredStatusRows.forEach((statusRow) => {
    const employeeId = String(statusRow?.employee_id || '').trim();
    const documento = String(statusRow?.documento || '').trim();
    const incapacity = (employeeId && incapacityByKey.get(`id:${employeeId}`)) || (documento && incapacityByKey.get(`doc:${documento}`)) || null;
    const statusLabel = String(statusRow?.novedad_nombre || '').trim() || dailyStatusLabel(statusRow?.estado_dia);
    if (!incapacity && !statusLabel) return;
    const row = ensureRow({
      employee_id: statusRow.employee_id || incapacity?.employee_id || null,
      documento: statusRow.documento || incapacity?.documento || null,
      nombre: statusRow.nombre || incapacity?.nombre || null,
      sede_codigo: statusRow.sede_codigo || null,
      sede_nombre: statusRow.sede_nombre_snapshot || null
    });
    if (!row) return;
    if (!row.entrySource) row.entrySource = incapacity ? 'incapacity' : 'daily_status';
    if (!row.entryLabel) row.entryLabel = String(incapacity?.source || '').trim() || statusLabel;
  });

  exits.forEach((exitRow) => {
    const row = ensureRow(exitRow);
    if (!row) return;
    const exitToken = tokenById.get(String(exitRow.qr_token_id || '')) || null;
    row.exitAt = exitRow.exit_at || null;
    row.exitPhone = exitToken?.phone_number || null;
    row.exitDistanceMeters = exitToken?.request_distance_meters == null ? null : Number(exitToken.request_distance_meters);
  });

  tokens.filter((row) => row?.action === 'exit' && !rowMap.has(keyFor(row))).forEach((tokenRow) => {
    const row = ensureRow(tokenRow);
    if (!row) return;
    row.exitAt = tokenRow.used_at || null;
    row.exitPhone = tokenRow.phone_number || null;
    row.exitDistanceMeters = tokenRow.request_distance_meters == null ? null : Number(tokenRow.request_distance_meters);
  });

  const rows = [...rowMap.values()]
    .map((row) => {
      const entryPhoneDifferent = isDifferentQrPhone(row.entryPhone, row.employeePhone);
      const exitPhoneDifferent = isDifferentQrPhone(row.exitPhone, row.employeePhone);
      return {
        ...row,
        entryPhoneDifferent,
        exitPhoneDifferent,
        phoneDifferent: entryPhoneDifferent || exitPhoneDifferent,
        alert: entryPhoneDifferent || exitPhoneDifferent ? 'Celular diferente' : null
      };
    })
    .sort((a, b) => String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || '')) || String(a.nombre || '').localeCompare(String(b.nombre || '')));
  const pendingRows = pendingStatusRows
    .filter((row) => {
      const employeeId = String(row?.employee_id || '').trim();
      const documento = String(row?.documento || '').trim();
      const hasEntry = (employeeId && entryKeys.has(`id:${employeeId}`)) || (documento && entryKeys.has(`doc:${documento}`));
      const hasAttendance = (employeeId && attendanceKeys.has(`id:${employeeId}`)) || (documento && attendanceKeys.has(`doc:${documento}`));
      const hasIncapacity = (employeeId && incapacityKeys.has(`id:${employeeId}`)) || (documento && incapacityKeys.has(`doc:${documento}`));
      return !hasEntry && !hasAttendance && !hasIncapacity;
    })
    .map((row) => {
      const sede = qrSedesByCode.get(String(row?.sede_codigo || '').trim()) || {};
      const employee = employeesById.get(String(row?.employee_id || '')) || {};
      return {
        employeeId: row.employee_id || null,
        documento: row.documento || employee.documento || null,
        nombre: row.nombre || employee.nombre || null,
        telefono: employee.telefono || null,
        sedeCodigo: row.sede_codigo || sede.codigo || null,
        sedeNombre: row.sede_nombre_snapshot || sede.nombre || null,
        dependenciaCodigo: row.dependencia_codigo_snapshot || sede.dependencia_codigo || null,
        dependenciaNombre: row.dependencia_nombre_snapshot || sede.dependencia_nombre || null,
        zonaCodigo: row.zona_codigo_snapshot || sede.zona_codigo || null,
        zonaNombre: row.zona_nombre_snapshot || sede.zona_nombre || null
      };
    })
    .sort((a, b) => String(a.sedeNombre || '').localeCompare(String(b.sedeNombre || '')) || String(a.nombre || '').localeCompare(String(b.nombre || '')));

  return { rows, pendingRows };
}

function isDifferentQrPhone(qrPhone, employeePhone) {
  const qr = normalizePhone(qrPhone);
  const expected = normalizePhone(employeePhone);
  if (!qr || !expected) return false;
  return qr !== expected;
}

function noveltyLabelByCode(code) {
  const target = String(code || '').trim();
  if (!target) return '';
  const novelty = Object.values(NOVELTIES).find((item) => String(item?.code || '').trim() === target);
  return String(novelty?.label || '').trim();
}

function dailyStatusLabel(status) {
  switch (String(status || '').trim().toLowerCase()) {
    case 'incapacidad':
      return 'Incapacidad';
    case 'vacaciones':
      return 'Vacaciones';
    case 'compensatorio':
      return 'Compensatorio';
    default:
      return '';
  }
}

async function storeIncomingEvent({ eventType, payload }) {
  const row = {
    id: payload?.id || payload?.message_id || crypto.randomUUID(),
    source: 'whatsapp_cloud_api',
    event_type: eventType,
    message_id: payload?.id || payload?.message_id || null,
    wa_from: payload?.from || payload?.recipient_id || null,
    wa_timestamp: payload?.timestamp || null,
    wa_type: payload?.type || payload?.status || null,
    text_body: extractMessageText(payload),
    phone_number_id: payload?.metadata?.phone_number_id || null,
    display_phone_number: payload?.metadata?.display_phone_number || null,
    raw_payload: payload,
    process_status: eventType === 'message' ? 'pending' : 'processed',
    process_reason: eventType === 'message' ? null : 'status_event',
    processed_at: eventType === 'message' ? null : new Date().toISOString()
  };

  const { error } = await supabaseAdmin.from('whatsapp_incoming').upsert(row, { onConflict: 'id' });
  if (error) throw error;
  return row.id;
}

async function markIncomingProcessed(id, status, reason) {
  if (!id) return;
  await supabaseAdmin.from('whatsapp_incoming').update({
    process_status: status,
    process_reason: reason,
    processed_at: new Date().toISOString()
  }).eq('id', id);
}

async function notifyProcessingError(message, error) {
  if (error?.userNotified === true) return;
  const phone = normalizePhone(message?.from);
  if (!phone) return;
  try {
    await sendText(phone, userMessageForProcessingError(error));
  } catch (notifyError) {
    console.error('No se pudo notificar error al usuario WhatsApp:', notifyError);
  }
}

function userMessageForProcessingError(error) {
  const raw = `${error?.message || ''} ${error?.details || ''} ${error?.hint || ''}`.toLowerCase();
  if (raw.includes('request_latitude') || raw.includes('qr_latitude') || raw.includes('location_verified_at')) {
    return 'No pudimos generar el QR porque falta actualizar la base de datos con la migracion de ubicacion QR. Comunicate con el supervisor.';
  }
  if (raw.includes('attendance_qr_tokens')) {
    return 'No pudimos generar el QR por una novedad en la configuracion QR. Comunicate con el supervisor.';
  }
  if (raw.includes('send_failed')) {
    return 'Validamos tu solicitud, pero no pudimos enviar el QR por WhatsApp. Intenta nuevamente en unos minutos.';
  }
  if (raw.includes('attendance_missing_sede')) {
    return 'No pudimos generar el QR porque tu registro no tiene sede asignada. Comunicate con el supervisor.';
  }
  if (raw.includes('employee_registered_before_transfer')) {
    return 'El empleado ya se registro hoy en la sede anterior. No se puede iniciar el cambio de sede hoy.';
  }
  return 'No pudimos procesar tu solicitud en este momento. Intenta nuevamente o comunicate con el supervisor.';
}

async function processIncomingMessage(message) {
  const phone = normalizePhone(message?.from);
  if (!phone) throw new Error('missing_phone_number');

  const session = await getSession(phone);
  const parsed = parseInboundAction(message);

  if (!parsed.value && !parsed.id && !parsed.location) {
    await sendText(phone, 'No entendí tu respuesta. Por favor selecciona una opción del menú.');
    return;
  }


  if (normalizeKey(parsed.value) === 'hola') {
    await resetSession(phone, session, {});
    await startIdentificationFlow(phone);
    return;
  }
  if (session.session_state === SESSION.IDLE || session.session_state === SESSION.COMPLETED) {
    await startIdentificationFlow(phone);
    return;
  }

  switch (session.session_state) {
    case SESSION.AWAITING_DOCUMENT:
      await handleDocumentInput(phone, session, parsed.value);
      return;
    case SESSION.AWAITING_ACTION:
      await handleActionSelection(phone, session, parsed);
      return;
    case SESSION.AWAITING_UPDATE_ACTION:
      await handleUpdateSelection(phone, session, parsed);
      return;
    case SESSION.AWAITING_PHONE:
      await handlePhoneUpdate(phone, session, parsed.value);
      return;
    case SESSION.AWAITING_TRANSFER_KEYWORD:
      await handleTransferKeyword(phone, session, parsed.value, false);
      return;
    case SESSION.AWAITING_TRANSFER_SELECTION:
      await handleTransferSelection(phone, session, parsed);
      return;
    case SESSION.AWAITING_WORKING_SEDE_KEYWORD:
      await handleTransferKeyword(phone, session, parsed.value, true);
      return;
    case SESSION.AWAITING_WORKING_SEDE_SELECTION:
      await handleWorkingSedeSelection(phone, session, parsed);
      return;
    case SESSION.AWAITING_QR_ATTENDANCE_ACTION:
      await handleQrAttendanceAction(phone, session, parsed);
      return;
    case SESSION.AWAITING_QR_LOCATION:
      await handleQrLocationInput(phone, session, parsed);
      return;
    case SESSION.AWAITING_NOVELTY:
      await handleNoveltySelection(phone, session, parsed);
      return;
    case SESSION.AWAITING_DATE_START:
      await handleDateStart(phone, session, parsed.value);
      return;
    case SESSION.AWAITING_DATE_END:
      await handleDateEnd(phone, session, parsed.value);
      return;
    default:
      await resetSession(phone, session, {});
      await startIdentificationFlow(phone);
  }
}
async function startIdentificationFlow(phone) {
  const employeeByPhone = await findEmployeeByPhone(phone);
  if (employeeByPhone) {
    await storeSession(phone, {
      employee_id: employeeByPhone.id,
      documento: employeeByPhone.documento,
      session_state: SESSION.AWAITING_ACTION,
      session_data: { employee: sessionEmployee(employeeByPhone), identifiedBy: 'phone' }
    });
    await sendIdentityOrMenu(phone, employeeByPhone);
    return;
  }

  await storeSession(phone, {
    session_state: SESSION.AWAITING_DOCUMENT,
    session_data: { identifiedBy: 'unknown_phone' }
  });
  await sendText(phone, 'Hola, no encontramos tu número registrado en la base de datos, por favor escribe tu cédula sin puntos.');
}

async function handleDocumentInput(phone, session, value) {
  const document = normalizeDocument(value);
  if (!document) {
    await sendText(phone, 'Por favor escribe tu número de cédula sin puntos.');
    return;
  }

  const employee = await findEmployeeByDocument(document);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await storeSession(phone, { session_state: SESSION.COMPLETED, session_data: session.session_data || {} });
    return;
  }

  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.AWAITING_ACTION,
    session_data: { ...(session.session_data || {}), employee: sessionEmployee(employee), identifiedBy: 'document' }
  });
  await sendIdentityOrMenu(phone, employee);
}

async function sendIdentityOrMenu(phone, employee) {
  if (employee.isSupernumerario) {
    await sendButtons(phone,
      `Hola, soy Rocky\n\nEres: ${employee.nombre}\nCédula: ${employee.documento}\nEstas como SUPERNUMERARIO\n\nElige una opción:`,
      [
        { id: MENU_IDS.ACTION_WORKING, title: 'Trabajando' },
        { id: MENU_IDS.ACTION_NOVELTY, title: 'Novedad' },
        { id: MENU_IDS.UPDATE_DATA, title: 'Actualizar Datos' }
      ]
    );
    return;
  }

  await sendButtons(phone,
    `Hola, soy Rocky\n\nEres: ${employee.nombre}\nCédula: ${employee.documento}\nEstás en: ${employee.sede_nombre || 'Sin sede'}\n\nElige una opción:`,
    [
      { id: MENU_IDS.IDENTITY_YES, title: 'Soy Yo' },
      { id: MENU_IDS.IDENTITY_NO, title: 'No Soy Yo' },
      { id: MENU_IDS.UPDATE_DATA, title: 'Actualizar Datos' }
    ]
  );
}

async function handleActionSelection(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const hasMainMenu = Boolean(session?.session_data?.menuReady);
  const choice = mapActionChoice(parsed, employee.isSupernumerario, hasMainMenu);
  if (!choice) {
    await sendText(phone, 'Selecciona una opción válida del menú.');
    return;
  }

  if (!employee.isSupernumerario && choice === 'identity_yes') {
    const activeIncapacity = await findActiveIncapacity(employee.documento, currentDate());
    if (activeIncapacity) {
      await sendText(phone, 'Te encuentras incapacitado, Muchas Gracia por el registro.');
      await storeSession(phone, {
        employee_id: employee.id,
        documento: employee.documento,
        session_state: SESSION.COMPLETED,
        session_data: { employee: sessionEmployee(employee) }
      });
      return;
    }

    await storeSession(phone, {
      employee_id: employee.id,
      documento: employee.documento,
      session_state: SESSION.AWAITING_ACTION,
      session_data: { employee: sessionEmployee(employee), menuReady: true }
    });
    await sendButtons(phone, 'Elige una opción:', [
      { id: MENU_IDS.ACTION_WORKING, title: 'Trabajando' },
      { id: MENU_IDS.ACTION_COMPENSATORY, title: 'Compensatorio' },
      { id: MENU_IDS.ACTION_NOVELTY, title: 'Novedad' }
    ]);
    return;
  }

  if (!employee.isSupernumerario && choice === 'identity_no') {
    await storeSession(phone, {
      session_state: SESSION.AWAITING_DOCUMENT,
      session_data: { identifiedBy: 'identity_override' }
    });
    await sendText(phone, 'Por favor escribe tu número de cédula sin puntos:');
    return;
  }

  if (choice === 'update_data') {
    await storeSession(phone, {
      employee_id: employee.id,
      documento: employee.documento,
      session_state: SESSION.AWAITING_UPDATE_ACTION,
      session_data: { employee: sessionEmployee(employee), menuReady: hasMainMenu }
    });
    await sendButtons(phone, 'Selecciona una opción:', [
      { id: MENU_IDS.UPDATE_TRANSFER, title: 'Traslado de Sede' },
      { id: MENU_IDS.UPDATE_PHONE, title: 'Cambio de Teléfono' }
    ]);
    return;
  }

  if (choice === 'working') {
    if (employee.isSupernumerario) {
      await storeSession(phone, {
        employee_id: employee.id,
        documento: employee.documento,
        session_state: SESSION.AWAITING_WORKING_SEDE_KEYWORD,
        session_data: { employee: sessionEmployee(employee), pendingNovelty: NOVELTIES.WORKING }
      });
      await sendText(phone, 'Escribe una palabra clave del nombre de la sede en la que te encuentras:');
      return;
    }
    if (await isQrEnabledForSede(employee.sede_codigo)) {
      await promptQrAttendanceAction(phone, employee, null);
      return;
    }
    await registerNovelty(phone, employee, NOVELTIES.WORKING, null);
    return;
  }

  if (choice === 'compensatory') {
    await registerNovelty(phone, employee, NOVELTIES.COMPENSATORY, null);
    return;
  }

  if (choice === 'novelty') {
    await storeSession(phone, {
      employee_id: employee.id,
      documento: employee.documento,
      session_state: SESSION.AWAITING_NOVELTY,
      session_data: { employee: sessionEmployee(employee) }
    });
    await sendList(phone, 'Selecciona la novedad que presentas:', 'Seleccionar novedad', [{
      title: 'Novedades',
      rows: buildNoveltyRows(employee.isSupernumerario)
    }]);
    return;
  }

  await sendText(phone, 'Selecciona una opción válida del menú.');
}

async function handleUpdateSelection(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const normalized = normalizeKey(parsed.id || parsed.value);
  if (normalized === normalizeKey(MENU_IDS.UPDATE_TRANSFER) || normalized === 'trasladodesede') {
    await storeSession(phone, {
      employee_id: employee.id,
      documento: employee.documento,
      session_state: SESSION.AWAITING_TRANSFER_KEYWORD,
      session_data: { employee: sessionEmployee(employee) }
    });
    await sendText(phone, 'Escribe una palabra clave del nombre de la sede a la que te trasladaron:');
    return;
  }

  if (normalized === normalizeKey(MENU_IDS.UPDATE_PHONE) || normalized === 'cambiodetelefono') {
    await storeSession(phone, {
      employee_id: employee.id,
      documento: employee.documento,
      session_state: SESSION.AWAITING_PHONE,
      session_data: { employee: sessionEmployee(employee) }
    });
    await sendText(phone, 'Diligencia el número de celular nuevo:');
    return;
  }

  await sendText(phone, 'Selecciona una opción válida del menú.');
}

async function handlePhoneUpdate(phone, session, value) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const normalizedPhone = normalizePhone(value);
  if (!normalizedPhone) {
    await sendText(phone, 'Diligencia el número de celular nuevo:');
    return;
  }

  const { error } = await supabaseAdmin.from('employees').update({ telefono: normalizedPhone, last_modified_at: new Date().toISOString() }).eq('id', employee.id);
  if (error) throw error;

  const refreshed = { ...employee, telefono: normalizedPhone };
  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.COMPLETED,
    session_data: { employee: sessionEmployee(refreshed) }
  });
  await sendText(phone, 'Información actualizada correctamente, si no haz realizado el registro por favor escribe nuevamente "Hola".');
}
async function handleTransferKeyword(phone, session, value, forWorkingSelection) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const keyword = String(value || '').trim();
  if (!keyword) {
    await sendText(phone, forWorkingSelection ? 'Escribe una palabra clave del nombre de la sede en la que te encuentras:' : 'Escribe una palabra clave del nombre de la sede a la que te trasladaron:');
    return;
  }

  const matches = await searchSedes(keyword);
  if (!matches.length) {
    await sendText(phone, 'No encontramos sedes con esa palabra. Intenta con otra palabra clave.');
    return;
  }

  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: forWorkingSelection ? SESSION.AWAITING_WORKING_SEDE_SELECTION : SESSION.AWAITING_TRANSFER_SELECTION,
    session_data: { ...(session.session_data || {}), employee: sessionEmployee(employee), sedeOptions: matches }
  });

  await sendList(phone, 'Selecciona la sede:', 'Ver sedes', [{
    title: 'Sedes disponibles',
    rows: matches.map((sede) => ({
      id: `${forWorkingSelection ? 'work' : 'transfer'}_sede_${sede.id}`,
      title: truncate(sede.nombre || sede.codigo, 24),
      description: truncate(`${sede.codigo || 'Sin código'} · ${sede.zona_nombre || 'Sin zona'}`, 72)
    }))
  }]);
}

async function handleTransferSelection(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  const selected = resolveSedeSelection(session, parsed, 'transfer_sede_');
  if (!employee || !selected) {
    await sendText(phone, 'Selecciona una sede válida del listado.');
    return;
  }

  const previousEmployeeSedeSnapshot = {
    sede_codigo: employee.sede_codigo || null,
    sede_nombre: employee.sede_nombre || null,
    zona_codigo: employee.zona_codigo || null,
    zona_nombre: employee.zona_nombre || null
  };

  const transferDate = currentDate();
  await assertNoEmployeeAttendanceTodayBeforeSedeTransfer(employee, transferDate);
  const employeeUpdatePayload = {
    sede_codigo: selected.codigo || null,
    sede_nombre: selected.nombre || null,
    zona_codigo: selected.zona_codigo || null,
    zona_nombre: selected.zona_nombre || null,
    last_modified_at: new Date().toISOString()
  };

  const { error } = await supabaseAdmin.from('employees').update(employeeUpdatePayload).eq('id', employee.id);
  if (error) throw error;

  try {
    await syncEmployeeSedeHistoryAfterTransfer(employee, selected, transferDate);
    await refreshOperationalState(transferDate);
  } catch (historyError) {
    await supabaseAdmin.from('employees').update({
      ...previousEmployeeSedeSnapshot,
      last_modified_at: new Date().toISOString()
    }).eq('id', employee.id);
    throw historyError;
  }

  const refreshed = { ...employee, sede_codigo: selected.codigo, sede_nombre: selected.nombre, zona_codigo: selected.zona_codigo, zona_nombre: selected.zona_nombre };
  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.COMPLETED,
    session_data: { employee: sessionEmployee(refreshed) }
  });
  await sendText(phone, 'Información actualizada correctamente, si no haz realizado el registro por favor escribe nuevamente "Hola".');
}

async function syncEmployeeSedeHistoryAfterTransfer(employee, selectedSede, transferDate) {
  const employeeId = String(employee?.id || '').trim();
  const selectedCode = String(selectedSede?.codigo || '').trim();
  if (!employeeId || !selectedCode || !transferDate) return;

  const selectedName = selectedSede?.nombre || null;
  const previousDay = addDaysToIsoDate(transferDate, -1) || transferDate;

  const { data: openRows, error: openRowsError } = await supabaseAdmin
    .from('employee_cargo_history')
    .select('id, employee_id, sede_codigo, fecha_ingreso, fecha_retiro, created_at')
    .eq('employee_id', employeeId)
    .is('fecha_retiro', null)
    .order('created_at', { ascending: false });
  if (openRowsError) throw openRowsError;

  const normalizedOpenRows = openRows || [];
  const openOnSelectedSede = normalizedOpenRows.find((row) => String(row?.sede_codigo || '').trim() === selectedCode) || null;

  for (const row of normalizedOpenRows) {
    if (openOnSelectedSede && row.id === openOnSelectedSede.id) continue;
    const ingresoDate = isoDatePart(row?.fecha_ingreso);
    let retiroDate = previousDay;
    if (ingresoDate && retiroDate < ingresoDate) retiroDate = ingresoDate;
    const patchedRetiro = withIsoDatePreservingTime(row?.fecha_retiro, retiroDate);
    const { error: closeError } = await supabaseAdmin
      .from('employee_cargo_history')
      .update({ fecha_retiro: patchedRetiro })
      .eq('id', row.id);
    if (closeError) throw closeError;
  }

  if (openOnSelectedSede) return;

  const transferIngreso = `${transferDate}T05:00:00+00:00`;
  const { error: insertError } = await supabaseAdmin.from('employee_cargo_history').insert({
    employee_id: employeeId,
    employee_codigo: employee?.codigo || null,
    documento: employee?.documento || null,
    cargo_codigo: employee?.cargo_codigo || null,
    cargo_nombre: employee?.cargo_nombre || null,
    fecha_ingreso: transferIngreso,
    fecha_retiro: null,
    source: 'sede_change',
    sede_codigo: selectedCode,
    sede_nombre: selectedName
  });
  if (insertError) throw insertError;
}

async function handleWorkingSedeSelection(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  const selected = resolveSedeSelection(session, parsed, 'work_sede_');
  const novelty = session?.session_data?.pendingNovelty || NOVELTIES.WORKING;
  if (!employee || !selected) {
    await sendText(phone, 'Selecciona una sede válida del listado.');
    return;
  }

  if (novelty?.code === NOVELTIES.WORKING.code && await isQrEnabledForSede(selected.codigo)) {
    await promptQrAttendanceAction(phone, employee, selected);
    return;
  }

  await registerNovelty(phone, employee, novelty, selected);
}

async function isQrEnabledForSede(sedeCodigo) {
  const sede = await getSedeByCode(sedeCodigo);
  return sede?.qr_enabled === true;
}

async function promptQrAttendanceAction(phone, employee, selectedSede = null) {
  const freshEmployee = await reloadEmployeeForAttendance(employee);
  const sedeCodigo = selectedSede?.codigo || freshEmployee?.sede_codigo || null;
  const sedeNombre = selectedSede?.nombre || freshEmployee?.sede_nombre || null;
  if (!sedeCodigo) {
    throw new Error(`attendance_missing_sede:${freshEmployee?.id || 'no_id'}:${freshEmployee?.documento || 'no_doc'}`);
  }

  await storeSession(phone, {
    employee_id: freshEmployee.id,
    documento: freshEmployee.documento,
    session_state: SESSION.AWAITING_QR_ATTENDANCE_ACTION,
    session_data: {
      employee: sessionEmployee(freshEmployee),
      selectedSede: selectedSede ? {
        id: selectedSede.id || null,
        codigo: sedeCodigo,
        nombre: sedeNombre,
        zona_codigo: selectedSede.zona_codigo || freshEmployee.zona_codigo || null,
        zona_nombre: selectedSede.zona_nombre || freshEmployee.zona_nombre || null
      } : null
    }
  });

  await sendButtons(phone, `La sede ${sedeNombre || sedeCodigo} usa registro por QR.\n\nQue deseas registrar?`, [
    { id: MENU_IDS.QR_ENTRY, title: 'Ingreso' },
    { id: MENU_IDS.QR_EXIT, title: 'Salida' }
  ]);
}

async function handleQrAttendanceAction(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const normalizedId = normalizeKey(parsed.id);
  const normalizedValue = normalizeKey(parsed.value);
  let action = null;
  if (normalizedId === normalizeKey(MENU_IDS.QR_ENTRY) || normalizedValue === 'ingreso') action = 'entry';
  if (normalizedId === normalizeKey(MENU_IDS.QR_EXIT) || normalizedValue === 'salida') action = 'exit';
  if (!action) {
    await sendText(phone, 'Selecciona una opcion valida: Ingreso o Salida.');
    return;
  }

  const selectedSede = session?.session_data?.selectedSede || null;
  const freshEmployee = await reloadEmployeeForAttendance(employee);
  const documento = normalizeDocument(freshEmployee?.documento);
  try {
    await validateQrActionAvailability({
      action,
      fecha: currentDate(),
      documento
    });
  } catch (error) {
    if (String(error?.message || '') === 'entry_exists') {
      await storeSession(phone, {
        employee_id: freshEmployee.id,
        documento: freshEmployee.documento,
        session_state: SESSION.COMPLETED,
        session_data: { employee: sessionEmployee(freshEmployee) }
      });
      await sendText(phone, 'Ya tienes un ingreso registrado para hoy. Si necesitas marcar salida, escribe "Hola" y selecciona Salida.');
      return;
    }
    if (String(error?.message || '') === 'exit_requires_entry') {
      await sendText(phone, 'No encontramos un ingreso registrado para hoy. Primero debes registrar Ingreso.');
      return;
    }
    if (String(error?.message || '') === 'exit_exists') {
      await storeSession(phone, {
        employee_id: freshEmployee.id,
        documento: freshEmployee.documento,
        session_state: SESSION.COMPLETED,
        session_data: { employee: sessionEmployee(freshEmployee) }
      });
      await sendText(phone, 'Ya tienes una salida registrada para hoy. No es necesario generar otro QR.');
      return;
    }
    throw error;
  }

  await storeSession(phone, {
    employee_id: freshEmployee.id,
    documento: freshEmployee.documento,
    session_state: SESSION.AWAITING_QR_LOCATION,
    session_data: {
      ...(session.session_data || {}),
      employee: sessionEmployee(freshEmployee),
      selectedSede,
      pendingQrAction: action
    }
  });

  await sendText(phone, 'Para generar el QR comparte tu ubicacion actual desde WhatsApp. Debes estar a maximo 500 metros de la sede.');
}

async function handleQrLocationInput(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const location = parsed.location || null;
  if (!location) {
    await sendText(phone, 'Por favor comparte tu ubicacion actual usando la opcion Ubicacion de WhatsApp para generar el QR.');
    return;
  }
  if (isNamedLocation(location)) {
    await sendText(phone, 'Recibimos una ubicacion con nombre o direccion, que puede corresponder a una busqueda. Para generar el QR comparte tu ubicacion actual desde WhatsApp, sin seleccionar una direccion del mapa.');
    return;
  }

  const action = String(session?.session_data?.pendingQrAction || '').trim();
  if (!['entry', 'exit'].includes(action)) {
    await resetSession(phone, session, {});
    await sendText(phone, 'No encontramos la accion QR pendiente. Escribe "Hola" para iniciar de nuevo.');
    return;
  }

  const selectedSede = session?.session_data?.selectedSede || null;
  const sedeCodigo = selectedSede?.codigo || employee?.sede_codigo || null;
  const sede = await getSedeByCode(sedeCodigo);
  const validation = validateQrLocationForSede(location, sede);
  if (!validation.ok) {
    await sendText(phone, validation.message);
    return;
  }

  try {
    await sendAttendanceQr(phone, employee, action, selectedSede, {
      latitude: location.latitude,
      longitude: location.longitude,
      distanceMeters: validation.distanceMeters
    });
  } catch (error) {
    console.error('Error generando QR despues de validar ubicacion:', error);
    await sendText(phone, userMessageForProcessingError(error));
    error.userNotified = true;
    throw error;
  }
}

function validateQrLocationForSede(location, sede) {
  const sedeLat = Number(sede?.qr_latitude);
  const sedeLng = Number(sede?.qr_longitude);
  const userLat = Number(location?.latitude);
  const userLng = Number(location?.longitude);
  const radius = Number(sede?.qr_radius_meters || 500);

  if (!Number.isFinite(sedeLat) || !Number.isFinite(sedeLng)) {
    return {
      ok: false,
      message: 'Esta sede tiene QR activo pero no tiene latitud/longitud configurada. Comunicate con el supervisor.'
    };
  }
  if (!Number.isFinite(userLat) || !Number.isFinite(userLng)) {
    return {
      ok: false,
      message: 'No pudimos leer tu ubicacion. Por favor comparte tu ubicacion actual desde WhatsApp.'
    };
  }

  const distanceMeters = Math.round(distanceBetweenMeters(userLat, userLng, sedeLat, sedeLng));
  if (distanceMeters > radius) {
    return {
      ok: false,
      distanceMeters,
      message: `Tu ubicacion esta a ${distanceMeters} metros de la sede. El maximo permitido es ${radius} metros. Comparte tu ubicacion actual cuando estes en la sede.`
    };
  }

  return { ok: true, distanceMeters };
}

function isNamedLocation(location = {}) {
  return Boolean(String(location?.name || '').trim() || String(location?.address || '').trim());
}

function distanceBetweenMeters(latA, lngA, latB, lngB) {
  const earthRadiusMeters = 6371000;
  const toRadians = (value) => Number(value) * Math.PI / 180;
  const deltaLat = toRadians(latB - latA);
  const deltaLng = toRadians(lngB - lngA);
  const startLat = toRadians(latA);
  const endLat = toRadians(latB);
  const a = Math.sin(deltaLat / 2) ** 2
    + Math.cos(startLat) * Math.cos(endLat) * Math.sin(deltaLng / 2) ** 2;
  return earthRadiusMeters * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

async function sendAttendanceQr(phone, employee, action, selectedSede = null, locationProof = null) {
  const freshEmployee = await reloadEmployeeForAttendance(employee);
  const documento = normalizeDocument(freshEmployee?.documento);
  const sedeCodigo = selectedSede?.codigo || freshEmployee?.sede_codigo || null;
  const sedeNombre = selectedSede?.nombre || freshEmployee?.sede_nombre || null;
  if (!documento || !freshEmployee?.id) throw new Error('missing_employee_identity');
  if (!sedeCodigo) throw new Error(`attendance_missing_sede:${freshEmployee?.id || 'no_id'}:${documento || 'no_doc'}`);

  const token = createQrToken();
  const date = currentDate();
  const { data, error } = await supabaseAdmin.from('attendance_qr_tokens').insert({
    token_hash: hashToken(token),
    action,
    fecha: date,
    employee_id: freshEmployee.id,
    documento,
    nombre: freshEmployee.nombre || null,
    sede_codigo: sedeCodigo,
    sede_nombre: sedeNombre || null,
    phone_number: phone,
    request_latitude: typeof locationProof?.latitude === 'number' ? locationProof.latitude : null,
    request_longitude: typeof locationProof?.longitude === 'number' ? locationProof.longitude : null,
    request_distance_meters: Number.isFinite(Number(locationProof?.distanceMeters)) ? Number(locationProof.distanceMeters) : null,
    location_verified_at: new Date().toISOString(),
    expires_at: qrExpiresAtIso()
  }).select('id,expires_at').single();
  if (error) throw error;

  await storeSession(phone, {
    employee_id: freshEmployee.id,
    documento: freshEmployee.documento,
    session_state: SESSION.COMPLETED,
    session_data: {
      employee: sessionEmployee(freshEmployee),
      lastQrTokenId: data.id,
      lastQrAction: action
    }
  });

  const actionLabel = action === 'exit' ? 'salida' : 'ingreso';
  const distanceText = Number.isFinite(Number(locationProof?.distanceMeters)) ? `\nUbicacion validada: ${Number(locationProof.distanceMeters)} m de la sede.` : '';
  const caption = `QR temporal para registrar ${actionLabel}.\nEmpleado: ${freshEmployee.nombre || documento}\nSede: ${sedeNombre || sedeCodigo}${distanceText}\nVence en ${Number(config.qrTokenMinutes || 10)} minutos.`;
  await sendQrImage(phone, token, caption);
}

async function handleNoveltySelection(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  if (!employee) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  const novelty = mapNovelty(parsed);
  if (!novelty) {
    await sendText(phone, 'Selecciona una novedad válida del listado.');
    return;
  }

  if (!novelty.requiresDates) {
    await registerNovelty(phone, employee, novelty, null);
    return;
  }

  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.AWAITING_DATE_START,
    session_data: { ...(session.session_data || {}), employee: sessionEmployee(employee), pendingNovelty: novelty }
  });
  const prompts = getNoveltyDatePrompts(novelty);
  await sendText(phone, prompts.startIntro);
}

async function handleDateStart(phone, session, value) {
  const employee = await loadEmployeeFromSession(session);
  const novelty = session?.session_data?.pendingNovelty;
  const parsedDate = parseInputDate(value);
  if (!employee || !novelty) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  if (!parsedDate) {
    await sendText(phone, getNoveltyDatePrompts(novelty).startOnly);
    return;
  }

  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.AWAITING_DATE_END,
    session_data: { ...(session.session_data || {}), employee: sessionEmployee(employee), pendingNovelty: novelty, incapacityStart: parsedDate }
  });
  await sendText(phone, getNoveltyDatePrompts(novelty).endOnly);
}

async function handleDateEnd(phone, session, value) {
  const employee = await loadEmployeeFromSession(session);
  const novelty = session?.session_data?.pendingNovelty;
  const startDate = session?.session_data?.incapacityStart || null;
  const endDate = parseInputDate(value);
  if (!employee || !novelty || !startDate) {
    await sendText(phone, NO_REGISTERED_MESSAGE);
    await resetSession(phone, session, {});
    return;
  }

  if (!endDate) {
    await sendText(phone, getNoveltyDatePrompts(novelty).endOnly);
    return;
  }

  if (endDate < startDate) {
    await sendText(phone, `La fecha de terminación no puede ser menor a la fecha de inicio (${formatDateForHumans(startDate)}).\n\n${getNoveltyDatePrompts(novelty).endOnly}`);
    return;
  }

  await registerNovelty(phone, employee, novelty, null, { startDate, endDate });
}

async function registerNovelty(phone, employee, novelty, selectedSede = null, incapacity = null) {
  const date = currentDate();
  const time = currentTime();
  const freshEmployee = await reloadEmployeeForAttendance(employee);
  const documento = normalizeDocument(freshEmployee.documento);
  const attendanceId = buildDailyRecordId(date, documento, freshEmployee.id);
  const sedeCodigo = selectedSede?.codigo || freshEmployee.sede_codigo || null;
  const sedeNombre = selectedSede?.nombre || freshEmployee.sede_nombre || null;
  if (!sedeCodigo) {
    throw new Error(`attendance_missing_sede:${freshEmployee.id || 'no_id'}:${documento || 'no_doc'}`);
  }

  if (novelty.tracksIncapacity && incapacity?.startDate && incapacity?.endDate) {
    const overlapping = await findOverlappingIncapacity(documento, incapacity.startDate, incapacity.endDate);
    if (overlapping) {
      await storeSession(phone, {
        employee_id: freshEmployee.id,
        documento: freshEmployee.documento,
        session_state: SESSION.COMPLETED,
        session_data: { employee: sessionEmployee(freshEmployee) }
      });
      await sendText(phone, buildOverlapMessage(novelty));
      return;
    }
  }

  const { error: attendanceError } = await supabaseAdmin.from('attendance').upsert({
    id: attendanceId,
    fecha: date,
    empleado_id: freshEmployee.id,
    documento,
    nombre: freshEmployee.nombre,
    sede_codigo: sedeCodigo,
    sede_nombre: sedeNombre,
    asistio: [NOVELTIES.WORKING.code, NOVELTIES.COMPENSATORY.code].includes(novelty.code),
    novedad: novelty.code
  }, { onConflict: 'id' });
  if (attendanceError) throw attendanceError;

  if (novelty.absenteeism) {
    const { error: absenteeismError } = await supabaseAdmin.from('absenteeism').upsert({
      id: attendanceId,
      fecha: date,
      empleado_id: freshEmployee.id,
      documento,
      nombre: freshEmployee.nombre,
      sede_codigo: sedeCodigo,
      sede_nombre: sedeNombre,
      estado: 'reportado_whatsapp'
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
  } else {
    await clearDailyOperationalAbsenceArtifacts(attendanceId);
  }

  if (novelty.tracksIncapacity && incapacity?.startDate && incapacity?.endDate) {
    const { error: incapacityError } = await supabaseAdmin.from('incapacitados').insert({
      employee_id: freshEmployee.id,
      documento,
      nombre: freshEmployee.nombre,
      fecha_inicio: incapacity.startDate,
      fecha_fin: incapacity.endDate,
      estado: 'activo',
      source: novelty.label,
      canal_registro: 'whatsapp',
      whatsapp_message_id: `${attendanceId}_${novelty.code}`
    });
    if (incapacityError) throw incapacityError;
  }

  await refreshOperationalState(date);
  await storeSession(phone, {
    employee_id: freshEmployee.id,
    documento: freshEmployee.documento,
    session_state: SESSION.COMPLETED,
    session_data: { employee: sessionEmployee(freshEmployee) }
  });

  const supportMessage = buildSupportMessage(novelty, incapacity);
  if (supportMessage) {
    await sendText(phone, supportMessage);
    return;
  }

  await sendText(phone, `Registro confirmado. Fecha: ${formatDateForHumans(date)}, Hora: ${time}, Novedad: ${novelty.label}, Muchas Gracias.`);
}

async function clearDailyOperationalAbsenceArtifacts(recordId) {
  const dailyId = String(recordId || '').trim();
  if (!dailyId) return;

  const { error: absenteeismError } = await supabaseAdmin
    .from('absenteeism')
    .delete()
    .eq('id', dailyId);
  if (absenteeismError) throw absenteeismError;

  const { error: replacementError } = await supabaseAdmin
    .from('import_replacements')
    .delete()
    .eq('id', dailyId);
  if (replacementError) throw replacementError;
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

async function fetchDailyMetricsRow(date) {
  const day = String(date || '').trim();
  if (!day) return null;
  const { data, error } = await supabaseAdmin
    .from('daily_metrics')
    .select('*')
    .eq('fecha', day)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function removeInvalidScheduledEmployeeDailyStatusRows(date) {
  const day = String(date || '').trim();
  if (!day) return 0;

  const [
    { data: statusRows, error: statusError },
    sedesRows,
    employeesRows,
    cargosRows,
    employeeHistoryRows
  ] = await Promise.all([
    supabaseAdmin
      .from('employee_daily_status')
      .select('id, employee_id')
      .eq('fecha', day)
      .eq('tipo_personal', 'empleado')
      .eq('servicio_programado', true),
    selectAllRows('sedes'),
    selectAllRows('employees'),
    selectAllRows('cargos', 'codigo, alineacion_crud, nombre'),
    selectAllRows('employee_cargo_history', 'id, employee_id, cargo_codigo, cargo_nombre, sede_codigo, fecha_ingreso, fecha_retiro, created_at')
  ]);
  if (statusError) throw statusError;

  const scheduledSedes = (sedesRows || [])
    .filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((row) => isSedeScheduledForDate(row, day));
  const activeSedeCodes = new Set(scheduledSedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row?.codigo || '').trim(), row]));
  const employeeById = new Map(
    (employeesRows || [])
      .map((row) => [String(row?.id || '').trim(), row])
      .filter(([id]) => Boolean(id))
  );
  const historyByEmployeeId = new Map();
  for (const row of employeeHistoryRows || []) {
    const employeeId = String(row?.employee_id || '').trim();
    if (!employeeId) continue;
    if (!historyByEmployeeId.has(employeeId)) historyByEmployeeId.set(employeeId, []);
    historyByEmployeeId.get(employeeId).push(row);
  }

  const invalidIds = (statusRows || [])
    .filter((row) => {
      const employeeId = String(row?.employee_id || '').trim();
      const employee = employeeById.get(employeeId) || null;
      if (!employee) return true;
      const historyRows = historyByEmployeeId.get(employeeId) || [];
      const assignment = resolveEmployeeAssignmentHistoryOnDate(employee, day, historyRows);
      const source = assignment || employee;
      const cargoCode = String(source?.cargo_codigo || source?.cargoCodigo || '').trim();
      const cargo = cargoMap.get(cargoCode) || null;
      const alignment = normalizeCargoAlignment(cargo?.alineacion_crud || source?.cargo_nombre || source?.cargoNombre || '');
      if (alignment === 'supernumerario') return true;
      return !isEmployeeAssignedToActiveSedeOnDate(employee, day, activeSedeCodes, historyRows);
    })
    .map((row) => String(row?.id || '').trim())
    .filter(Boolean);

  for (let index = 0; index < invalidIds.length; index += 200) {
    const batch = invalidIds.slice(index, index + 200);
    const { error } = await supabaseAdmin.from('employee_daily_status').delete().in('id', batch);
    if (error) throw error;
  }

  return invalidIds.length;
}

async function refreshEmployeeDailyStatusSnapshot(date) {
  const day = String(date || '').trim();
  if (!day) return null;
  const { data, error } = await supabaseAdmin.rpc('refresh_employee_daily_status', { p_fecha: day });
  if (error) {
    if (isMissingRpcError(error)) return null;
    throw error;
  }
  await removeInvalidScheduledEmployeeDailyStatusRows(day);
  return data ?? 0;
}

async function refreshOperationalSnapshotsFromEmployeeDailyStatus(date) {
  const day = String(date || '').trim();
  if (!day) return null;
  if (await isOperationDayClosed(day)) {
    const metrics = await fetchDailyMetricsRow(day);
    return metrics ? { fecha: day, skipped: 'closed' } : null;
  }

  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed === null) return null;

  const { data, error } = await supabaseAdmin.rpc('refresh_operational_snapshots_from_employee_daily_status', { p_fecha: day });
  if (error) {
    if (isMissingRpcError(error)) return null;
    throw error;
  }

  return unwrapRpcSingleRow(data);
}

async function refreshOperationalState(date) {
  const day = String(date || '').trim();
  if (!day) return null;
  if (await isOperationDayClosed(day)) return fetchDailyMetricsRow(day);

  const refreshed = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
  if (refreshed !== null) {
    return fetchDailyMetricsRow(day);
  }

  await recomputeSedeStatusSnapshot(day);
  return recomputeAndFetchDailyMetrics(day);
}

async function recomputeDailyMetrics(date) {
  const day = String(date || '').trim();
  if (!day) return null;
  if (await isOperationDayClosed(day)) return fetchDailyMetricsRow(day);

  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed !== null) {
    const { data, error } = await supabaseAdmin.rpc('recompute_daily_metrics_from_employee_daily_status', { p_fecha: day });
    if (error) {
      if (!isMissingRpcError(error)) throw error;
    } else {
      return unwrapRpcSingleRow(data) || (await fetchDailyMetricsRow(day));
    }
  }

  const [
    { data: attendance, error: attendanceError },
    { data: replacements, error: replacementsError },
    sedesRows,
    employeesRows,
    employeeHistoryRows,
    cargosRows,
    novedadesRows
  ] = await Promise.all([
    supabaseAdmin.from('attendance').select('*').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes'),
    selectAllRows('employees'),
    selectAllRows('employee_cargo_history', 'id, employee_id, cargo_codigo, cargo_nombre, sede_codigo, sede_nombre, fecha_ingreso, fecha_retiro, created_at'),
    selectAllRows('cargos', 'codigo, alineacion_crud, nombre'),
    selectAllRows('novedades', 'codigo, codigo_novedad, nombre, reemplazo')
  ]);
  if (attendanceError) throw attendanceError;
  if (replacementsError) throw replacementsError;

  const attRows = Array.isArray(attendance) ? attendance : [];
  const repRows = Array.isArray(replacements) ? replacements : [];
  const sedes = (sedesRows || []).filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
  const scheduledSedes = sedes.filter((row) => isSedeScheduledForDate(row, day));
  const activeSedeCodes = new Set(
    scheduledSedes
      .map((row) => String(row?.codigo || '').trim())
      .filter(Boolean)
  );
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row?.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules(novedadesRows || []);
  const replacementMap = new Map(repRows.map((row) => [metricReplacementKey({
    fecha: row?.fecha,
    employeeId: row?.empleado_id || row?.employee_id
  }), row]));
  const historyByEmployeeId = buildEmployeeHistoryByEmployeeId(employeeHistoryRows);
  const fallbackExpected = (employeesRows || []).filter((emp) => {
    if (String(emp?.estado || '').trim().toLowerCase() !== 'activo') return false;
    const employeeId = String(emp?.id || '').trim();
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes, historyByEmployeeId.get(employeeId) || [])) return false;
    return !isEmployeeSupernumerarioByCargoMap(emp, cargoMap);
  }).length;
  const planned = scheduledSedes.reduce((sum, sede) => {
    const count = Number(sede?.numero_operarios ?? 0);
    return sum + (Number.isFinite(count) && count > 0 ? count : 0);
  }, 0);
  const expected = fallbackExpected;
  const uniqueDocs = new Set(attRows.map((row) => String(row?.documento || row?.empleado_id || '').trim()).filter(Boolean));
  const dedupedAttendanceRows = dedupeAttendanceRows(attRows);
  const actualAttendanceCount = dedupedAttendanceRows.filter((row) => row?.asistio === true).length;
  const actualAbsenteeism = dedupedAttendanceRows.filter((row) => row?.asistio === false).length;
  const attendanceCount = planned === 0 && expected === 0
    ? actualAttendanceCount
    : attRows.filter((row) => metricAttendanceCountsAsService(row, replacementMap, replacementRules)).length;
  const absenteeism = planned === 0 && expected === 0
    ? actualAbsenteeism
    : attRows.filter((row) => metricAttendanceCountsAsAbsenteeism(row, replacementMap, replacementRules)).length;
  const paidServices = attendanceCount;
  const noContracted = Math.max(0, planned - expected);
  const { error } = await supabaseAdmin.from('daily_metrics').upsert({
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
    closed: false
  }, { onConflict: 'id' });
  if (error) throw error;
}

async function recomputeSedeStatusSnapshot(date) {
  const day = String(date || '').trim();
  if (!day) return;
  if (await isOperationDayClosed(day)) return null;

  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed !== null) {
    const { data, error } = await supabaseAdmin.rpc('recompute_sede_status_from_employee_daily_status', { p_fecha: day });
    if (error) {
      if (!isMissingRpcError(error)) throw error;
    } else {
      return data ?? null;
    }
  }

  const [
    { data: attendance, error: attendanceError },
    { data: replacements, error: replacementsError },
    sedesRows,
    employeesRows,
    employeeHistoryRows,
    cargosRows,
    novedadesRows
  ] = await Promise.all([
    supabaseAdmin.from('attendance').select('*').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes'),
    selectAllRows('employees'),
    selectAllRows('employee_cargo_history', 'id, employee_id, cargo_codigo, cargo_nombre, sede_codigo, sede_nombre, fecha_ingreso, fecha_retiro, created_at'),
    selectAllRows('cargos', 'codigo, alineacion_crud, nombre'),
    selectAllRows('novedades', 'codigo, codigo_novedad, nombre, reemplazo')
  ]);
  if (attendanceError) throw attendanceError;
  if (replacementsError) throw replacementsError;

  const attRows = Array.isArray(attendance) ? attendance : [];
  const repRows = Array.isArray(replacements) ? replacements : [];
  const sedes = (sedesRows || []).filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
  const activeSedeCodes = new Set(sedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row?.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules(novedadesRows || []);
  const replacementMap = new Map(repRows.map((row) => [metricReplacementKey({
    fecha: row?.fecha,
    employeeId: row?.empleado_id || row?.employee_id
  }), row]));
  const replacementSuperDocs = new Set(
    repRows
      .filter((row) => String(row?.decision || '').trim().toLowerCase() === 'reemplazo')
      .map((row) => `${String(row?.fecha || '').trim()}|${String(row?.supernumerario_documento || row?.supernumerarioDocumento || '').trim()}`)
      .filter((value) => !value.endsWith('|'))
  );
  const employeeById = new Map();
  const employeeByDoc = new Map();
  const contractedBySede = new Map();
  const historyByEmployeeId = buildEmployeeHistoryByEmployeeId(employeeHistoryRows);

  (employeesRows || []).forEach((emp) => {
    const empId = String(emp?.id || '').trim();
    const doc = String(emp?.documento || '').trim();
    if (empId) employeeById.set(empId, emp);
    if (doc) employeeByDoc.set(doc, emp);
    const historyRows = historyByEmployeeId.get(empId) || [];
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes, historyRows)) return;
    if (isEmployeeSupernumerarioByCargoMap(emp, cargoMap)) return;
    const assignment = resolveEmployeeAssignmentHistoryOnDate(emp, day, historyRows);
    const source = assignment || emp;
    const sedeCode = String(source?.sede_codigo || source?.sedeCodigo || '').trim();
    if (!contractedBySede.has(sedeCode)) contractedBySede.set(sedeCode, new Set());
    contractedBySede.get(sedeCode).add(doc || empId);
  });

  const registeredBySede = new Map();
  const novSinReemplazoBySede = new Map();
  attRows.forEach((row) => {
    const doc = String(row?.documento || '').trim();
    if (doc && replacementSuperDocs.has(`${String(row?.fecha || '').trim()}|${doc}`)) return;
    const empId = String(row?.empleado_id || row?.empleadoId || '').trim();
    const employee = (empId && employeeById.get(empId)) || (doc && employeeByDoc.get(doc)) || null;
    const historyRows = employee ? (historyByEmployeeId.get(String(employee?.id || '').trim()) || []) : [];
    const assignment = employee ? resolveEmployeeAssignmentHistoryOnDate(employee, day, historyRows) : null;
    const source = assignment || employee;
    const sedeCode = String(row?.sede_codigo || source?.sede_codigo || source?.sedeCodigo || '').trim();
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
  const { error } = await supabaseAdmin.from('sede_status').upsert(payload, { onConflict: 'id' });
  if (error) throw error;
}

async function selectAllRows(table, select = '*') {
  const rows = [];
  let from = 0;
  const pageSize = 1000;
  while (true) {
    const { data, error } = await supabaseAdmin
      .from(table)
      .select(select)
      .range(from, from + pageSize - 1);
    if (error) throw error;
    const batch = Array.isArray(data) ? data : [];
    rows.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }
  return rows;
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
  const iso = String(selectedDate || '').trim();
  if (!iso) return false;
  const year = Number(iso.slice(0, 4));
  return getColombiaHolidaySet(year).has(iso);
}

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = String(selectedDate || '').trim();
  if (!iso || !sede) return false;
  const [year, month, day] = iso.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
  if (isColombiaHolidayDate(iso)) return false;
  if (jornada === 'lun_sab') return weekday >= 1 && weekday <= 6;
  return weekday >= 1 && weekday <= 5;
}

function buildNovedadReplacementRules(rows = []) {
  const byCode = new Map();
  const byName = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const code = String(row?.codigo_novedad || row?.codigo || '').trim();
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

function metricAttendanceNovedadCode(row = {}) {
  const explicit = String(row?.novedad_codigo || row?.novedadCodigo || '').trim();
  if (explicit) return explicit;
  const raw = String(row?.novedad || '').trim();
  return /^\d+$/.test(raw) ? raw : '';
}

function metricAttendanceRequiresReplacement(row = {}, rules = {}) {
  const code = metricAttendanceNovedadCode(row);
  if (['1', '7'].includes(code)) return false;
  if (['2', '3', '4', '5', '8', '9'].includes(code)) return true;
  if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
  const name = normalizeMetricText(baseMetricNovedadName(row?.novedad_nombre || row?.novedadNombre || row?.novedad || ''));
  if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
  return false;
}

function metricReplacementKey(row = {}) {
  return `${String(row?.fecha || '').trim()}_${String(row?.empleado_id || row?.empleadoId || row?.employee_id || row?.employeeId || '').trim()}`;
}

function dedupeAttendanceRows(rows = []) {
  const unique = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const logicalKey = [
      String(row?.fecha || '').trim(),
      String(row?.empleado_id || row?.empleadoId || row?.employee_id || row?.employeeId || '').trim()
        || String(row?.documento || '').trim()
        || String(row?.id || '').trim()
    ].join('|');
    if (!logicalKey || logicalKey === '|') continue;
    const existing = unique.get(logicalKey);
    if (!existing) {
      unique.set(logicalKey, row);
      continue;
    }
    const existingTs = String(existing?.created_at || '').trim();
    const rowTs = String(row?.created_at || '').trim();
    if (rowTs > existingTs) unique.set(logicalKey, row);
  }
  return Array.from(unique.values());
}

function metricAttendanceCountsAsService(row = {}, replacementMap = new Map(), rules = {}) {
  if (!metricAttendanceRequiresReplacement(row, rules)) return true;
  const replacement = replacementMap.get(metricReplacementKey(row)) || null;
  if (!replacement) return false;
  const decision = String(replacement?.decision || '').trim().toLowerCase();
  const hasSupernumerario = Boolean(replacement?.supernumerario_id || replacement?.supernumerarioId || replacement?.supernumerario_documento || replacement?.supernumerarioDocumento);
  return decision === 'reemplazo' && hasSupernumerario;
}

function metricAttendanceCountsAsAbsenteeism(row = {}, replacementMap = new Map(), rules = {}) {
  if (!metricAttendanceRequiresReplacement(row, rules)) return false;
  const replacement = replacementMap.get(metricReplacementKey(row)) || null;
  if (!replacement) return true;
  const decision = String(replacement?.decision || '').trim().toLowerCase();
  return decision !== 'reemplazo';
}

function isEmployeeSupernumerarioByCargoMap(emp, cargoMap = new Map()) {
  const cargoCode = String(emp?.cargo_codigo || '').trim();
  const cargo = cargoMap.get(cargoCode) || null;
  const alignment = normalizeCargoAlignment(cargo?.alineacion_crud || emp?.cargo_nombre || '');
  return alignment === 'supernumerario';
}

function resolveEmployeeAssignmentHistoryOnDate(emp, selectedDate, historyRows = []) {
  const day = String(selectedDate || '').trim();
  if (!day) return null;
  const matching = (Array.isArray(historyRows) ? historyRows : []).filter((row) => {
    const ingreso = toISODate(row?.fecha_ingreso || row?.fechaIngreso);
    if (!ingreso || ingreso > day) return false;
    const retiro = toISODate(row?.fecha_retiro || row?.fechaRetiro);
    return !retiro || retiro >= day;
  });
  if (!matching.length) return null;
  matching.sort((left, right) => {
    const leftIngreso = toISODate(left?.fecha_ingreso || left?.fechaIngreso) || '';
    const rightIngreso = toISODate(right?.fecha_ingreso || right?.fechaIngreso) || '';
    if (leftIngreso !== rightIngreso) return rightIngreso.localeCompare(leftIngreso);
    const leftCreated = String(left?.created_at || left?.createdAt || '').trim();
    const rightCreated = String(right?.created_at || right?.createdAt || '').trim();
    if (leftCreated !== rightCreated) return rightCreated.localeCompare(leftCreated);
    return String(right?.id || '').localeCompare(String(left?.id || ''));
  });
  return matching[0] || null;
}

function buildEmployeeHistoryByEmployeeId(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce((acc, row) => {
    const employeeId = String(row?.employee_id || row?.employeeId || '').trim();
    if (!employeeId) return acc;
    if (!acc.has(employeeId)) acc.set(employeeId, []);
    acc.get(employeeId).push(row);
    return acc;
  }, new Map());
}

function isEmployeeAssignedToActiveSedeOnDate(emp, selectedDate, activeSedeCodes = new Set(), historyRows = []) {
  if (!selectedDate) return false;
  const assignment = resolveEmployeeAssignmentHistoryOnDate(emp, selectedDate, historyRows);
  const source = assignment || emp;
  const ingreso = toISODate(source?.fecha_ingreso || source?.fechaIngreso);
  if (!ingreso || ingreso > selectedDate) return false;
  const retiro = toISODate(source?.fecha_retiro || source?.fechaRetiro);
  const estado = String(emp?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') return Boolean(retiro && retiro >= selectedDate);
  if (retiro && retiro < selectedDate) return false;
  const sedeCodigo = String(source?.sede_codigo || source?.sedeCodigo || '').trim();
  if (!sedeCodigo) return false;
  if (activeSedeCodes.size && !activeSedeCodes.has(sedeCodigo)) return false;
  return true;
}

function normalizeCargoAlignment(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['supernumerario', 'supervisor', 'empleado'].includes(normalized)) return normalized;
  if (normalized.includes('supernumer')) return 'supernumerario';
  if (normalized.includes('supervisor')) return 'supervisor';
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
async function computeDailyClosureSummary(date) {
  const day = String(date || '').trim();
  if (!day) {
    return { planeados: 0, contratados: 0, asistencias: 0, faltan: 0, sobran: 0, ausentismos: 0, noContratados: 0 };
  }

  const [
    { data: statusRows, error: statusError },
    sedesRows
  ] = await Promise.all([
    supabaseAdmin
      .from('employee_daily_status')
      .select('sede_codigo, tipo_personal, servicio_programado, asistio, cuenta_pago_servicio')
      .eq('fecha', day),
    selectAllRows('sedes')
  ]);
  if (statusError) throw statusError;

  const scheduledRows = (statusRows || []).filter((row) => String(row?.tipo_personal || '').trim() === 'empleado' && row?.servicio_programado === true);
  const actualRows = (statusRows || []).filter((row) => row?.asistio === true || row?.asistio === false);
  const sedes = (sedesRows || [])
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));

  const bySede = new Map();
  for (const row of scheduledRows) {
    const sedeCode = String(row?.sede_codigo || '').trim();
    if (!sedeCode) continue;
    const bucket = bySede.get(sedeCode) || {
      contratados: 0,
      asistencias: 0
    };
    bucket.contratados += 1;
    if (row?.cuenta_pago_servicio === true) bucket.asistencias += 1;
    bySede.set(sedeCode, bucket);
  }

  const summary = sedes.reduce((acc, sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planned = Number(sede?.numero_operarios ?? 0) || 0;
    const counts = bySede.get(sedeCode) || { contratados: 0, asistencias: 0 };
    const ausentismos = computeOperationalAbsenteeism(planned, counts.contratados, counts.asistencias);
    acc.planeados += planned;
    acc.contratados += counts.contratados;
    acc.asistencias += counts.asistencias;
    acc.faltan += Math.max(0, planned - counts.contratados);
    acc.sobran += Math.max(0, counts.contratados - planned);
    acc.ausentismos += ausentismos;
    return acc;
  }, {
    planeados: 0,
    contratados: 0,
    asistencias: 0,
    faltan: 0,
    sobran: 0,
    ausentismos: 0,
    noContratados: 0
  });

  if (summary.planeados === 0 && summary.contratados === 0 && actualRows.length) {
    summary.asistencias = actualRows.filter((row) => row?.asistio === true).length;
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

async function computeDailySedeClosureSnapshot(date) {
  const day = String(date || '').trim();
  if (!day) return [];

  const [
    { data: attendance, error: attendanceError },
    { data: replacements, error: replacementsError },
    sedesRows,
    employeesRows,
    employeeHistoryRows,
    cargosRows,
    novedadesRows
  ] = await Promise.all([
    supabaseAdmin.from('attendance').select('*').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes'),
    selectAllRows('employees'),
    selectAllRows('employee_cargo_history', 'id, employee_id, cargo_codigo, cargo_nombre, sede_codigo, sede_nombre, fecha_ingreso, fecha_retiro, created_at'),
    selectAllRows('cargos', 'codigo, alineacion_crud, nombre'),
    selectAllRows('novedades', 'codigo, codigo_novedad, nombre, reemplazo')
  ]);
  if (attendanceError) throw attendanceError;
  if (replacementsError) throw replacementsError;

  const sedes = (sedesRows || [])
    .filter((sede) => String(sede?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .filter((sede) => isSedeScheduledForDate(sede, day));
  const activeSedeCodes = new Set(sedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row?.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules(novedadesRows || []);
  const replacementMap = new Map((replacements || []).map((row) => [metricReplacementKey(row), row]));
  const replacementSuperDocs = new Set(
    (replacements || [])
      .filter((row) => String(row?.decision || '').trim().toLowerCase() === 'reemplazo')
      .map((row) => day + '|' + String(row?.supernumerario_documento || row?.supernumerarioDocumento || '').trim())
      .filter((value) => !value.endsWith('|'))
  );

  const employeeById = new Map();
  const employeeByDoc = new Map();
  const contractedBySede = new Map();
  const supernumerarioDocs = new Set();
  const historyByEmployeeId = buildEmployeeHistoryByEmployeeId(employeeHistoryRows);

  for (const emp of employeesRows || []) {
    const empId = String(emp?.id || '').trim();
    const doc = String(emp?.documento || '').trim();
    if (empId) employeeById.set(empId, emp);
    if (doc) employeeByDoc.set(doc, emp);
    const historyRows = historyByEmployeeId.get(empId) || [];
    if (doc && isEmployeeSupernumerarioByCargoMap(emp, cargoMap) && isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes, historyRows)) {
      supernumerarioDocs.add(doc);
    }
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes, historyRows)) continue;
    if (isEmployeeSupernumerarioByCargoMap(emp, cargoMap)) continue;
    const assignment = resolveEmployeeAssignmentHistoryOnDate(emp, day, historyRows);
    const source = assignment || emp;
    const sedeCode = String(source?.sede_codigo || source?.sedeCodigo || '').trim();
    if (!sedeCode) continue;
    if (!contractedBySede.has(sedeCode)) contractedBySede.set(sedeCode, new Set());
    contractedBySede.get(sedeCode).add(doc || empId);
  }

  const registeredBySede = new Map();
  for (const row of dedupeAttendanceRows(attendance || [])) {
    const doc = String(row?.documento || '').trim();
    if (doc && replacementSuperDocs.has(day + '|' + doc)) continue;
    if (doc && supernumerarioDocs.has(doc)) continue;
    const empId = String(row?.empleado_id || row?.empleadoId || '').trim();
    const employee = (empId && employeeById.get(empId)) || (doc && employeeByDoc.get(doc)) || null;
    if (isEmployeeSupernumerarioByCargoMap(employee, cargoMap)) continue;
    const historyRows = employee ? (historyByEmployeeId.get(String(employee?.id || '').trim()) || []) : [];
    const assignment = employee ? resolveEmployeeAssignmentHistoryOnDate(employee, day, historyRows) : null;
    const source = assignment || employee;
    const sedeCode = String(row?.sede_codigo || row?.sedeCodigo || source?.sede_codigo || source?.sedeCodigo || '').trim();
    if (!sedeCode || !activeSedeCodes.has(sedeCode)) continue;
    if (!registeredBySede.has(sedeCode)) registeredBySede.set(sedeCode, new Set());
    registeredBySede.get(sedeCode).add(doc || empId || String(row?.id || '').trim());
  }

  return sedes.map((sede) => {
    const sedeCode = String(sede?.codigo || '').trim();
    const planeados = Number(sede?.numero_operarios ?? 0) || 0;
    const baseContracted = Number(contractedBySede.get(sedeCode)?.size || 0);
    const registrados = Number(registeredBySede.get(sedeCode)?.size || 0);
    const externalRegistered = Math.max(0, registrados - baseContracted);
    const contratados = Math.min(planeados, baseContracted + externalRegistered);
    const faltantes = Math.max(0, planeados - registrados);
    const sobrantes = Math.max(0, registrados - planeados);
    return {
      id: day + '_' + sedeCode,
      fecha: day,
      sede_codigo: sedeCode,
      sede_nombre: sede?.nombre || sedeCode || null,
      zona_codigo: sede?.zona_codigo || null,
      zona_nombre: sede?.zona_nombre || null,
      dependencia_codigo: sede?.dependencia_codigo || null,
      dependencia_nombre: sede?.dependencia_nombre || null,
      planeados,
      contratados,
      registrados,
      faltantes,
      sobrantes
    };
  });
}

async function persistDailySedeClosureSnapshot(day) {
  const snapshot = await computeDailySedeClosureSnapshot(day);
  if (!snapshot.length) return [];
  const { error } = await supabaseAdmin.from('daily_sede_closures').upsert(snapshot, { onConflict: 'id' });
  if (error) throw error;
  return snapshot;
}

async function closeOperationDay(date) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(date || '').trim())) {
    throw new Error('invalid_date');
  }

  const day = String(date).trim();
  await insertSystemAuditLog({
    actorEmail: 'cron@system',
    targetType: 'daily_closure',
    targetId: day,
    action: 'cron_close_started',
    note: 'Inicio de cierre automatico para ' + day + '.'
  });

  const { data: existingClosure, error: existingClosureError } = await supabaseAdmin
    .from('daily_closures')
    .select('*')
    .eq('fecha', day)
    .maybeSingle();
  if (existingClosureError) throw existingClosureError;

  if (existingClosure?.locked === true || String(existingClosure?.status || '').trim().toLowerCase() === 'closed') {
    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_skipped_already_closed',
      before: existingClosure,
      note: 'El cierre automatico de ' + day + ' ya estaba cerrado; no se recalcularon snapshots ni metricas.'
    });
    return { date: day, status: 'already_closed' };
  }

  try {
    await finalizePendingAbsenteeismForClosure(day);
    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_stage_finalize_absenteeism',
      note: 'Se consolidaron ausentismos pendientes para ' + day + '.'
    });

    const refreshedBeforeClosure = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
    if (refreshedBeforeClosure === null) {
      await recomputeSedeStatusSnapshot(day);
      await insertSystemAuditLog({
        actorEmail: 'cron@system',
        targetType: 'daily_closure',
        targetId: day,
        action: 'cron_close_stage_sede_status',
        note: 'Se recalculo sede_status para ' + day + '.'
      });
    } else {
      await insertSystemAuditLog({
        actorEmail: 'cron@system',
        targetType: 'daily_closure',
        targetId: day,
        action: 'cron_close_stage_operational_snapshots',
        note: 'Se consolidaron employee_daily_status, sede_status y daily_metrics para ' + day + '.'
      });
    }

    const metrics = refreshedBeforeClosure === null
      ? await recomputeAndFetchDailyMetrics(day)
      : ((await fetchDailyMetricsRow(day)) || (await recomputeAndFetchDailyMetrics(day)));
    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_stage_metrics',
      after: metrics,
      note: 'Se recalcularon daily_metrics para ' + day + '.'
    });

    const closureSummary = await computeDailyClosureSummary(day);
    const sedeClosureSnapshot = await computeDailySedeClosureSnapshot(day);

    const { error: closureError } = await supabaseAdmin
      .from('daily_closures')
      .upsert({
        id: day,
        fecha: day,
        status: 'closed',
        locked: true,
        planeados: Number(closureSummary?.planeados || metrics?.planned || 0),
        contratados: Number(closureSummary?.contratados || metrics?.expected || 0),
        asistencias: Number(closureSummary?.asistencias || 0),
        ausentismos: Number(closureSummary?.ausentismos || metrics?.absenteeism || 0),
        faltan: Number(closureSummary?.faltan || 0),
        sobran: Number(closureSummary?.sobran || 0),
        no_contratados: Number(closureSummary?.noContratados || metrics?.no_contracted || metrics?.noContracted || 0),
        closed_by_uid: null,
        closed_by_email: 'cron@system'
      }, { onConflict: 'id' });
    if (closureError) throw closureError;

    if (sedeClosureSnapshot.length) {
      const { error: sedeClosureError } = await supabaseAdmin.from('daily_sede_closures').upsert(sedeClosureSnapshot, { onConflict: 'id' });
      if (sedeClosureError) throw sedeClosureError;
    }

    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_stage_closure_saved',
      after: metrics,
      note: 'Se guardo daily_closures para ' + day + '.'
    });

    await runPostClosureTasks(day);
    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_stage_post_closure',
      note: 'Se ejecutaron tareas de post-cierre para ' + day + '.'
    });

    const refreshedAfterClosure = await refreshOperationalSnapshotsFromEmployeeDailyStatus(day);
    if (refreshedAfterClosure === null) {
      await recomputeSedeStatusSnapshot(day);
      await recomputeDailyMetrics(day);
    }

    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_stage_metrics_closed',
      note: 'Se reconciliaron snapshots cerrados para ' + day + '.'
    });
    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_completed',
      note: 'El cierre automatico de ' + day + ' finalizo correctamente.'
    });
  } catch (error) {
    await insertSystemAuditLog({
      actorEmail: 'cron@system',
      targetType: 'daily_closure',
      targetId: day,
      action: 'cron_close_failed',
      before: existingClosure,
      after: serializeErrorForAudit(error),
      note: 'Fallo el cierre automatico de ' + day + '.'
    });
    throw error;
  }

  return { date: day, status: 'closed' };
}

async function runPostClosureTasks(day) {

  const { error: metricCloseError } = await supabaseAdmin
    .from('daily_metrics')
    .update({ closed: true })
    .eq('fecha', day);
  if (metricCloseError) throw metricCloseError;
  await propagateIncapacitiesToNextDay(day);
}

async function recomputeAndFetchDailyMetrics(date) {
  await recomputeDailyMetrics(date);
  return fetchDailyMetricsRow(date);
}

async function insertSystemAuditLog({
  actorEmail = 'cron@system',
  targetType = null,
  targetId = null,
  action = null,
  before = null,
  after = null,
  note = null
} = {}) {
  if (!action) return;
  try {
    const { error } = await supabaseAdmin
      .from('audit_logs')
      .insert({
        actor_uid: null,
        actor_email: actorEmail,
        target_type: targetType,
        target_id: targetId == null ? null : String(targetId),
        action,
        before_data: before,
        after_data: after,
        note
      });
    if (error) {
      console.error('No se pudo guardar audit_logs del sistema:', error);
    }
  } catch (error) {
    console.error('Fallo insertando audit_logs del sistema:', error);
  }
}

function serializeErrorForAudit(error) {
  if (!error) return null;
  return {
    message: String(error?.message || error),
    code: error?.code || null,
    details: error?.details || null,
    hint: error?.hint || null,
    name: error?.name || null
  };
}

function assertCronAuthorized(req) {
  if (!config.cronSecret) return;
  const header = String(req.headers.authorization || '').trim();
  const expected = `Bearer ${config.cronSecret}`;
  if (header !== expected) {
    throw new Error('unauthorized_cron');
  }
}

async function finalizePendingAbsenteeismForClosure(day) {
  const [
    { data: attendanceRows, error: attendanceError },
    { data: replacementRows, error: replacementsError },
    novedadesRows
  ] = await Promise.all([
    supabaseAdmin.from('attendance').select('*').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('novedades', 'codigo, codigo_novedad, nombre, reemplazo')
  ]);
  if (attendanceError) throw attendanceError;
  if (replacementsError) throw replacementsError;

  const replacementRules = buildNovedadReplacementRules(novedadesRows || []);
  const replacementMap = new Map((replacementRows || []).map((row) => [metricReplacementKey(row), row]));

  for (const row of attendanceRows || []) {
    if (!metricAttendanceRequiresReplacement(row, replacementRules)) continue;
    const key = metricReplacementKey(row);
    const existing = replacementMap.get(key);
    const existingDecision = String(existing?.decision || '').trim().toLowerCase();
    if (existingDecision === 'reemplazo') continue;
    if (existingDecision === 'ausentismo') continue;

    const recordId = buildDailyRecordId(day, row?.documento, row?.empleado_id);
    const { error: replacementError } = await supabaseAdmin.from('import_replacements').upsert({
      id: recordId,
      fecha_operacion: day,
      fecha: day,
      empleado_id: row?.empleado_id || null,
      documento: row?.documento || null,
      nombre: row?.nombre || null,
      sede_codigo: row?.sede_codigo || null,
      sede_nombre: row?.sede_nombre || null,
      novedad_codigo: metricAttendanceNovedadCode(row) || null,
      novedad_nombre: row?.novedad || null,
      decision: 'ausentismo',
      actor_uid: null,
      actor_email: 'cron@system'
    }, { onConflict: 'id' });
    if (replacementError) throw replacementError;

    const { error: absenteeismError } = await supabaseAdmin.from('absenteeism').upsert({
      id: recordId,
      fecha: day,
      empleado_id: row?.empleado_id || null,
      documento: row?.documento || null,
      nombre: row?.nombre || null,
      sede_codigo: row?.sede_codigo || null,
      sede_nombre: row?.sede_nombre || null,
      estado: 'confirmado',
      created_by_uid: null,
      created_by_email: 'cron@system'
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
  }

  await cleanupNonProgrammedClosedOperationalAbsenteeism(day);
  await materializeClosedOperationalAbsenteeismForClosure(day);
}

async function cleanupNonProgrammedClosedOperationalAbsenteeism(day) {
  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed === null) return 0;

  const { data: statusRows, error } = await supabaseAdmin
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
    const { data, error: replacementError } = await supabaseAdmin
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
    const { data, error: absenteeismError } = await supabaseAdmin
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
    const { error: deleteError } = await supabaseAdmin.from('import_replacements').delete().in('id', batch);
    if (deleteError) throw deleteError;
  }
  for (const batch of chunk(cronAbsenteeismIds)) {
    const { error: deleteError } = await supabaseAdmin.from('absenteeism').delete().in('id', batch);
    if (deleteError) throw deleteError;
  }

  const removed = new Set([...cronReplacementIds, ...cronAbsenteeismIds]).size;
  if (removed > 0) {
    await refreshEmployeeDailyStatusSnapshot(day);
  }
  return removed;
}

async function materializeClosedOperationalAbsenteeismForClosure(day) {
  const refreshed = await refreshEmployeeDailyStatusSnapshot(day);
  if (refreshed === null) return 0;

  const { data: statusRows, error } = await supabaseAdmin
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

    const { error: replacementError } = await supabaseAdmin.from('import_replacements').upsert({
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

    const { error: absenteeismError } = await supabaseAdmin.from('absenteeism').upsert({
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

  return changed;
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

function isoDatePart(value) {
  return String(value || '').slice(0, 10);
}

function withIsoDatePreservingTime(originalValue, newIsoDate) {
  const original = String(originalValue || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(newIsoDate || '').trim())) return originalValue;
  if (!original) return `${newIsoDate}T05:00:00+00:00`;
  if (original.length < 10) return `${newIsoDate}T05:00:00+00:00`;
  return `${newIsoDate}${original.slice(10)}`;
}

async function isOperationDayClosed(day) {
  const iso = String(day || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return false;
  const { data, error } = await supabaseAdmin
    .from('daily_closures')
    .select('locked,status')
    .eq('fecha', iso)
    .maybeSingle();
  if (error) throw error;
  if (!data) return false;
  return data.locked === true || String(data.status || '').trim().toLowerCase() === 'closed';
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
  if (raw.includes('vacaciones')) return '9';
  return '3';
}

async function propagateIncapacitiesToNextDay(day) {
  const nextDay = addDaysToIsoDate(day, 1);
  if (!nextDay) return;
  if (await isOperationDayClosed(nextDay)) return;

  const { data: incapRows, error: incapError } = await supabaseAdmin
    .from('incapacitados')
    .select('*')
    .eq('estado', 'activo')
    .lte('fecha_inicio', nextDay)
    .gte('fecha_fin', nextDay);
  if (incapError) throw incapError;

  for (const incap of incapRows || []) {
    const employeeId = incap?.employee_id || null;
    if (!employeeId) continue;

    const { data: employee, error: employeeError } = await supabaseAdmin
      .from('employees')
      .select('*')
      .eq('id', employeeId)
      .maybeSingle();
    if (employeeError) throw employeeError;
    if (!employee) continue;

    const documento = normalizeDocument(employee?.documento);
    if (!documento) continue;

    const { data: existingAttendance, error: existingAttendanceError } = await supabaseAdmin
      .from('attendance')
      .select('id')
      .eq('fecha', nextDay)
      .eq('documento', documento)
      .limit(1)
      .maybeSingle();
    if (existingAttendanceError) throw existingAttendanceError;
    if (existingAttendance?.id) continue;

    const noveltyCode = incapacitySourceToNoveltyCode(incap?.source);
    const attendanceId = buildDailyRecordId(nextDay, documento, employee.id);
    const { error: attendanceError } = await supabaseAdmin.from('attendance').upsert({
      id: attendanceId,
      fecha: nextDay,
      empleado_id: employee.id,
      documento,
      nombre: employee.nombre || null,
      sede_codigo: employee.sede_codigo || null,
      sede_nombre: employee.sede_nombre || null,
      asistio: false,
      novedad: noveltyCode
    }, { onConflict: 'id' });
    if (attendanceError) throw attendanceError;

    const { error: absenteeismError } = await supabaseAdmin.from('absenteeism').upsert({
      id: attendanceId,
      fecha: nextDay,
      empleado_id: employee.id,
      documento,
      nombre: employee.nombre || null,
      sede_codigo: employee.sede_codigo || null,
      sede_nombre: employee.sede_nombre || null,
      estado: 'programado_incapacidad',
      created_by_uid: null,
      created_by_email: 'cron@system'
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
  }

  await refreshOperationalState(nextDay);
}

async function findEmployeeByPhone(phone) {
  const normalized = normalizePhone(phone);
  if (!normalized) return null;
  const last10 = normalized.slice(-10);
  const variants = [...new Set([normalized, last10, `57${last10}`])];
  const orQuery = variants.map((value) => `telefono.eq.${value}`).join(',');
  let query = supabaseAdmin.from('employees').select('*');
  if (orQuery) query = query.or(orQuery);
  const { data, error } = await query.limit(20);
  if (error) throw error;

  let employee = await findEffectiveEmployeeForDate(
    (data || []).filter((row) => normalizePhone(row.telefono) === normalized),
    currentDate()
  );
  if (!employee) {
    const { data: fallback, error: fallbackError } = await supabaseAdmin.from('employees').select('*').ilike('telefono', `%${last10}%`).limit(50);
    if (fallbackError) throw fallbackError;
    employee = await findEffectiveEmployeeForDate(
      (fallback || []).filter((row) => normalizePhone(row.telefono) === normalized),
      currentDate()
    );
  }
  return employee || null;
}

async function findEmployeeByDocument(document) {
  const { data, error } = await supabaseAdmin.from('employees').select('*').eq('documento', document).maybeSingle();
  if (error) throw error;
  if (!data) return null;
  const employee = await hydrateEmployee(data);
  return isEmployeeEffectiveForDate(employee, currentDate()) ? employee : null;
}

async function hydrateEmployee(row) {
  const employee = { ...row };
  employee.telefono = normalizePhone(employee.telefono);
  await applyEmployeeAssignmentForDate(employee, currentDate());
  employee.isSupernumerario = await isEmployeeSupernumerario(employee);
  return employee;
}

async function findEffectiveEmployeeForDate(rows = [], date = currentDate()) {
  for (const row of rows || []) {
    const employee = await hydrateEmployee(row);
    if (isEmployeeEffectiveForDate(employee, date)) return employee;
  }
  return null;
}

function isEmployeeEffectiveForDate(employee = {}, date = currentDate()) {
  const day = String(date || '').trim();
  if (!day) return false;
  const estado = String(employee?.estado || 'activo').trim().toLowerCase();
  if (estado === 'eliminado') return false;
  const ingreso = toISODate(employee?.fecha_ingreso || employee?.fechaIngreso);
  if (ingreso && ingreso > day) return false;
  const retiro = toISODate(employee?.fecha_retiro || employee?.fechaRetiro);
  if (estado === 'inactivo') return Boolean(retiro && retiro >= day);
  return !retiro || retiro >= day;
}

async function applyEmployeeAssignmentForDate(employee, date) {
  const employeeId = String(employee?.id || '').trim();
  const day = String(date || '').trim();
  if (!employeeId || !day) return employee;

  const { data, error } = await supabaseAdmin
    .from('employee_cargo_history')
    .select('id, employee_id, cargo_codigo, cargo_nombre, sede_codigo, sede_nombre, fecha_ingreso, fecha_retiro, created_at')
    .eq('employee_id', employeeId)
    .order('fecha_ingreso', { ascending: false })
    .limit(50);
  if (error) throw error;

  const assignment = resolveEmployeeAssignmentHistoryOnDate(employee, day, data || []);
  if (!assignment) return employee;
  employee.cargo_codigo = assignment.cargo_codigo || employee.cargo_codigo || null;
  employee.cargo_nombre = assignment.cargo_nombre || employee.cargo_nombre || null;
  employee.sede_codigo = assignment.sede_codigo || employee.sede_codigo || null;
  employee.sede_nombre = assignment.sede_nombre || employee.sede_nombre || null;
  return employee;
}

async function isEmployeeSupernumerario(employee) {
  const cargoCodigo = String(employee?.cargo_codigo || '').trim();
  const cargoNombre = String(employee?.cargo_nombre || '').trim();
  if (!cargoCodigo && !cargoNombre) return false;

  let query = supabaseAdmin.from('cargos').select('codigo,nombre,alineacion_crud');
  if (cargoCodigo) query = query.eq('codigo', cargoCodigo);
  else query = query.eq('nombre', cargoNombre);
  const { data, error } = await query.maybeSingle();
  if (error) throw error;

  const alignment = String(data?.alineacion_crud || '').trim().toLowerCase();
  if (alignment === 'supernumerario') return true;
  const haystack = `${cargoCodigo} ${cargoNombre} ${data?.nombre || ''}`.toLowerCase();
  return haystack.includes('supernumerar');
}

async function findActiveIncapacity(documento, date) {
  const { data, error } = await supabaseAdmin.from('incapacitados').select('*').eq('documento', documento).eq('estado', 'activo').lte('fecha_inicio', date).gte('fecha_fin', date).limit(1).maybeSingle();
  if (error) throw error;
  return data || null;
}

async function findOverlappingIncapacity(documento, startDate, endDate) {
  const { data, error } = await supabaseAdmin
    .from('incapacitados')
    .select('*')
    .eq('documento', documento)
    .eq('estado', 'activo')
    .lte('fecha_inicio', endDate)
    .gte('fecha_fin', startDate)
    .order('fecha_inicio', { ascending: true })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function reloadEmployeeForAttendance(employee) {
  const employeeId = String(employee?.id || '').trim();
  if (employeeId) {
    const { data, error } = await supabaseAdmin.from('employees').select('*').eq('id', employeeId).maybeSingle();
    if (error) throw error;
    if (data) return hydrateEmployee(data);
  }
  const documento = normalizeDocument(employee?.documento);
  if (documento) {
    const found = await findEmployeeByDocument(documento);
    if (found) return found;
  }
  return employee || null;
}

async function searchSedes(keyword) {
  const { data, error } = await supabaseAdmin.from('sedes').select('id,codigo,nombre,zona_codigo,zona_nombre').eq('estado', 'activo').ilike('nombre', `%${keyword}%`).order('nombre', { ascending: true }).limit(10);
  if (error) throw error;
  return data || [];
}

async function getSession(phone) {
  const { data, error } = await supabaseAdmin.from('whatsapp_sessions').select('*').eq('id', phone).maybeSingle();
  if (error) throw error;
  return data || { id: phone, phone_number: phone, employee_id: null, documento: null, session_state: SESSION.IDLE, session_data: {}, last_message_at: null };
}

async function storeSession(phone, patch = {}) {
  const existing = await getSession(phone);
  const payload = {
    id: phone,
    phone_number: phone,
    employee_id: patch.employee_id === undefined ? existing.employee_id || null : patch.employee_id,
    documento: patch.documento === undefined ? existing.documento || null : patch.documento,
    session_state: patch.session_state || SESSION.IDLE,
    session_data: patch.session_data || {},
    last_message_at: new Date().toISOString()
  };
  const { error } = await supabaseAdmin.from('whatsapp_sessions').upsert(payload, { onConflict: 'id' });
  if (error) throw error;
}

async function resetSession(phone, session, extraData) {
  await storeSession(phone, {
    employee_id: session?.employee_id || null,
    documento: session?.documento || null,
    session_state: SESSION.IDLE,
    session_data: extraData || {}
  });
}

async function loadEmployeeFromSession(session) {
  const sessionEmployeeData = session?.session_data?.employee || null;
  const employeeId = session?.employee_id || sessionEmployeeData?.id || null;
  const documento = session?.documento || sessionEmployeeData?.documento || null;

  if (employeeId) {
    const { data, error } = await supabaseAdmin.from('employees').select('*').eq('id', employeeId).maybeSingle();
    if (error) throw error;
    if (data) return hydrateEmployee(data);
  }
  if (documento) return findEmployeeByDocument(documento);
  return null;
}

function sessionEmployee(employee) {
  return {
    id: employee.id,
    documento: employee.documento,
    nombre: employee.nombre,
    telefono: employee.telefono || null,
    cargo_codigo: employee.cargo_codigo || null,
    cargo_nombre: employee.cargo_nombre || null,
    sede_codigo: employee.sede_codigo || null,
    sede_nombre: employee.sede_nombre || null,
    zona_codigo: employee.zona_codigo || null,
    zona_nombre: employee.zona_nombre || null,
    isSupernumerario: Boolean(employee.isSupernumerario)
  };
}

function parseInboundAction(message) {
  const textValue = extractMessageText(message);
  const interactive = message?.interactive || {};
  const buttonReply = interactive?.button_reply || null;
  const listReply = interactive?.list_reply || null;
  return {
    id: String(buttonReply?.id || listReply?.id || '').trim(),
    title: String(buttonReply?.title || listReply?.title || '').trim(),
    value: String(buttonReply?.title || listReply?.title || textValue || '').trim(),
    location: extractMessageLocation(message)
  };
}

function extractMessageLocation(payload) {
  const location = payload?.location || null;
  if (!location) return null;
  const latitude = Number(location.latitude);
  const longitude = Number(location.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  return {
    latitude,
    longitude,
    name: location.name || null,
    address: location.address || null
  };
}

function extractMessageText(payload) {
  if (!payload) return null;
  if (payload?.text?.body) return String(payload.text.body).trim();
  const interactive = payload?.interactive || {};
  if (interactive?.button_reply?.title) return String(interactive.button_reply.title).trim();
  if (interactive?.list_reply?.title) return String(interactive.list_reply.title).trim();
  return null;
}

function mapActionChoice(parsed, isSupernumerario, hasMainMenu) {
  const normalizedId = normalizeKey(parsed.id);
  const normalizedValue = normalizeKey(parsed.value);
  const isWorkingAction =
    normalizedId === normalizeKey(MENU_IDS.ACTION_WORKING) ||
    normalizedId === 'dailytrabajando' ||
    normalizedValue === 'trabajando';
  const isCompensatoryAction =
    normalizedId === normalizeKey(MENU_IDS.ACTION_COMPENSATORY) ||
    normalizedId === 'dailycompensatorio' ||
    normalizedValue === 'compensatorio';
  const isNoveltyAction =
    normalizedId === normalizeKey(MENU_IDS.ACTION_NOVELTY) ||
    normalizedId === 'dailynovedad' ||
    normalizedValue === 'novedad';
  if (!isSupernumerario && !hasMainMenu) {
    if (normalizedId === normalizeKey(MENU_IDS.IDENTITY_YES) || normalizedValue === 'soyyo') return 'identity_yes';
    if (normalizedId === normalizeKey(MENU_IDS.IDENTITY_NO) || normalizedValue === 'nosoyyo') return 'identity_no';
  }
  if (normalizedId === normalizeKey(MENU_IDS.UPDATE_DATA) || normalizedValue === 'actualizardatos') return 'update_data';
  if (isWorkingAction) return 'working';
  if (isCompensatoryAction) return 'compensatory';
  if (isNoveltyAction) return 'novelty';
  return null;
}

function mapNovelty(parsed) {
  const normalizedId = normalizeKey(parsed.id);
  const normalizedValue = normalizeKey(parsed.value);
  if (normalizedId === normalizeKey(MENU_IDS.NOVELTY_SICKNESS) || normalizedValue === 'enfermedadgeneral') return NOVELTIES.SICKNESS;
  if (normalizedId === normalizeKey(MENU_IDS.NOVELTY_ACCIDENT) || normalizedValue === 'accidentelaboral') return NOVELTIES.ACCIDENT;
  if (normalizedId === normalizeKey(MENU_IDS.NOVELTY_CALAMITY) || normalizedValue === 'calamidad') return NOVELTIES.CALAMITY;
  if (normalizedId === normalizeKey(MENU_IDS.NOVELTY_UNPAID) || normalizedValue === 'licencianoremunerada') return NOVELTIES.UNPAID_LEAVE;
  if (normalizedId === normalizeKey(MENU_IDS.NOVELTY_PAID) || normalizedValue === 'licenciaremunerada') return NOVELTIES.PAID_LEAVE;
  if (normalizedId === normalizeKey(MENU_IDS.NOVELTY_VACATIONS) || normalizedValue === 'vacaciones') return NOVELTIES.VACATIONS;
  return null;
}

function buildNoveltyRows(isSupernumerario) {
  const rows = [
    { id: MENU_IDS.NOVELTY_SICKNESS, title: 'Enfermedad General' },
    { id: MENU_IDS.NOVELTY_ACCIDENT, title: 'Accidente Laboral' },
    { id: MENU_IDS.NOVELTY_CALAMITY, title: 'Calamidad' },
    { id: MENU_IDS.NOVELTY_UNPAID, title: 'Licencia No Remunerada' },
    { id: MENU_IDS.NOVELTY_VACATIONS, title: 'Vacaciones' }
  ];
  if (!isSupernumerario) {
    rows.push({ id: MENU_IDS.NOVELTY_PAID, title: 'Licencia Remunerada' });
  }
  return rows;
}

function getNoveltyDatePrompts(novelty) {
  if (novelty?.dateContext === 'vacaciones') {
    return {
      startIntro: 'Selecciona las fechas de vacaciones:\n\nFecha de inicio de vacaciones, por favor escribe DD/MM/AAAA:',
      startOnly: 'Fecha de inicio de vacaciones, por favor escribe DD/MM/AAAA:',
      endOnly: 'Fecha de terminación de vacaciones, por favor escribe DD/MM/AAAA:'
    };
  }

  if (novelty?.dateContext === 'licencia') {
    return {
      startIntro: 'Selecciona las fechas de licencia:\n\nFecha de inicio de licencia, por favor escribe DD/MM/AAAA:',
      startOnly: 'Fecha de inicio de licencia, por favor escribe DD/MM/AAAA:',
      endOnly: 'Fecha de terminación de licencia, por favor escribe DD/MM/AAAA:'
    };
  }

  return {
    startIntro: 'Selecciona las fechas de incapacidad:\n\nFecha de inicio de incapacidad, por favor escribe DD/MM/AAAA:',
    startOnly: 'Fecha de inicio de incapacidad, por favor escribe DD/MM/AAAA:',
    endOnly: 'Fecha de terminación de incapacidad, por favor escribe DD/MM/AAAA:'
  };
}

function buildOverlapMessage(novelty) {
  if (novelty?.requiresSupport) {
    return 'Usted ya registró una incapacidad para estas fechas, por favor corrija el registro escribiendo "Hola" o comunícate con el Supervisor.';
  }
  return 'Usted ya registró una novedad para estas fechas, por favor corrija el registro escribiendo "Hola" o comunícate con el Supervisor.';
}

function buildSupportMessage(novelty, incapacity) {
  if (!novelty?.requiresSupport || !incapacity?.startDate || !incapacity?.endDate) return null;

  const days = countInclusiveDays(incapacity.startDate, incapacity.endDate);
  const reminder = days > 3
    ? '\n\nRECUERDA: Si es mayor a tres días debes cargar la historia clínica o Epicrisis.'
    : '';

  return `Por favor cargue el soporte ingresando al siguiente link:\n${EMPLOYEE_PORTAL_URL}${reminder}`;
}

function resolveSedeSelection(session, parsed, prefix) {
  const optionId = String(parsed.id || '').trim();
  if (!optionId.startsWith(prefix)) return null;
  const selectedId = optionId.slice(prefix.length);
  const options = Array.isArray(session?.session_data?.sedeOptions) ? session.session_data.sedeOptions : [];
  return options.find((item) => String(item.id) === selectedId) || null;
}
async function sendText(to, body) {
  await sendWhatsAppMessage(to, { type: 'text', text: { body } });
}

async function sendButtons(to, body, buttons) {
  await sendWhatsAppMessage(to, {
    type: 'interactive',
    interactive: {
      type: 'button',
      body: { text: body },
      action: {
        buttons: buttons.map((button) => ({
          type: 'reply',
          reply: { id: button.id, title: truncate(button.title, 20) }
        }))
      }
    }
  });
}

async function sendList(to, body, buttonText, sections) {
  await sendWhatsAppMessage(to, {
    type: 'interactive',
    interactive: {
      type: 'list',
      body: { text: body },
      action: {
        button: truncate(buttonText, 20),
        sections: sections.map((section) => ({
          title: truncate(section.title, 24),
          rows: section.rows.map((row) => ({
            id: row.id,
            title: truncate(row.title, 24),
            description: row.description ? truncate(row.description, 72) : undefined
          }))
        }))
      }
    }
  });
}

async function sendQrImage(to, token, caption) {
  const imageUrl = buildQrImageUrl(token);
  try {
    await sendWhatsAppMessage(to, {
      type: 'image',
      image: {
        link: imageUrl,
        caption
      }
    });
  } catch (error) {
    console.error('No se pudo enviar QR como imagen, enviando enlace:', error);
    await sendText(to, `${caption}\n\nAbre este enlace para mostrar el QR:\n${imageUrl}`);
  }
}

async function sendWhatsAppMessage(to, payload) {
  if (!config.whatsappAccessToken || !config.whatsappPhoneNumberId) {
    throw new Error('missing_whatsapp_credentials_or_recipient');
  }

  const response = await fetch(`https://graph.facebook.com/${config.whatsappGraphVersion}/${config.whatsappPhoneNumberId}/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${config.whatsappAccessToken}`
    },
    body: JSON.stringify({
      messaging_product: 'whatsapp',
      recipient_type: 'individual',
      to,
      ...payload
    })
  });

  if (!response.ok) {
    const detail = await safeJson(response);
    throw new Error(`send_failed_${response.status}:${JSON.stringify(detail)}`);
  }
}

async function safeJson(response) {
  try {
    return await response.json();
  } catch {
    return { error: 'invalid_json_response' };
  }
}

function normalizePhone(value) {
  const digits = String(value || '').replace(/\D+/g, '');
  if (!digits) return '';
  if (digits.startsWith('57') && digits.length >= 12) return digits.slice(0, 12);
  if (digits.length === 10) return `57${digits}`;
  if (digits.length > 10) return digits;
  return '';
}

function normalizeDocument(value) {
  return String(value || '').replace(/\D+/g, '').trim();
}

async function assertNoEmployeeAttendanceTodayBeforeSedeTransfer(employee = {}, transferDate) {
  const day = String(transferDate || '').trim();
  const employeeId = String(employee?.id || '').trim();
  const documento = normalizeDocument(employee?.documento);
  const previousSede = String(employee?.sede_codigo || '').trim();
  if (!day || !employeeId) return;

  const queries = [
    supabaseAdmin
      .from('attendance')
      .select('id, sede_codigo, sede_nombre')
      .eq('fecha', day)
      .eq('empleado_id', employeeId)
  ];
  if (documento) {
    queries.push(
      supabaseAdmin
        .from('attendance')
        .select('id, sede_codigo, sede_nombre')
        .eq('fecha', day)
        .eq('documento', documento)
    );
  }

  const results = await Promise.all(queries);
  const rows = [];
  for (const { data, error } of results) {
    if (error) throw error;
    rows.push(...(data || []));
  }

  const matchingRegistration = rows.find((row) => {
    const sede = String(row?.sede_codigo || '').trim();
    return !sede || !previousSede || sede === previousSede;
  });
  if (matchingRegistration) {
    const error = new Error('employee_registered_before_transfer');
    error.details = matchingRegistration.sede_nombre || matchingRegistration.sede_codigo || previousSede || null;
    throw error;
  }
}

function normalizeKey(value) {
  return String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-zA-Z0-9]+/g, '').toLowerCase().trim();
}

function parseInputDate(value) {
  const match = String(value || '').trim().match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if (!match) return null;
  const [, rawDay, rawMonth, year] = match;
  const day = String(rawDay).padStart(2, '0');
  const month = String(rawMonth).padStart(2, '0');
  const iso = `${year}-${month}-${day}`;
  const date = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  const utcYear = date.getUTCFullYear();
  const utcMonth = String(date.getUTCMonth() + 1).padStart(2, '0');
  const utcDay = String(date.getUTCDate()).padStart(2, '0');
  if (`${utcYear}-${utcMonth}-${utcDay}` !== iso) return null;
  return iso;
}

function currentDate() {
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'America/Bogota',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date());
}

function currentTime() {
  return new Intl.DateTimeFormat('es-CO', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'America/Bogota'
  }).format(new Date());
}

function formatDateForHumans(value) {
  const [year, month, day] = String(value || '').split('-');
  if (!year || !month || !day) return value;
  return `${day}/${month}/${year}`;
}

function countInclusiveDays(startDate, endDate) {
  const start = new Date(`${startDate}T00:00:00Z`);
  const end = new Date(`${endDate}T00:00:00Z`);
  const diffMs = end.getTime() - start.getTime();
  if (Number.isNaN(diffMs) || diffMs < 0) return 0;
  return Math.floor(diffMs / 86400000) + 1;
}

function truncate(value, max) {
  const text = String(value || '').trim();
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 3))}...`;
}

export default app;





