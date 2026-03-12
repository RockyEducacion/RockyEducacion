import crypto from 'node:crypto';
import express from 'express';
import { config } from './config.js';
import { supabaseAdmin } from './supabase.js';

const app = express();
app.use(express.json({
  verify(req, _res, buf) {
    req.rawBody = buf;
  }
}));

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true });
});

app.get('/webhooks/whatsapp', (req, res) => {
  const mode = String(req.query['hub.mode'] || '').trim();
  const token = String(req.query['hub.verify_token'] || '').trim();
  const challenge = String(req.query['hub.challenge'] || '').trim();
  if (mode === 'subscribe' && token === config.whatsappVerifyToken) {
    res.status(200).send(challenge);
    return;
  }
  res.status(403).send('Forbidden');
});

app.post('/webhooks/whatsapp', async (req, res) => {
  try {
    if (!isValidWhatsAppSignature(req)) {
      res.status(401).json({ ok: false, error: 'Invalid signature' });
      return;
    }

    const payload = req.body || {};
    if (payload.object && payload.object !== 'whatsapp_business_account') {
      res.status(400).json({ ok: false, error: 'Unsupported webhook object' });
      return;
    }

    let stored = 0;
    const entries = Array.isArray(payload.entry) ? payload.entry : [];

    for (const entry of entries) {
      const changes = Array.isArray(entry?.changes) ? entry.changes : [];
      for (const change of changes) {
        const value = change?.value || {};
        const metadata = value?.metadata || {};
        const phoneNumberId = String(metadata.phone_number_id || '').trim() || null;
        const displayPhoneNumber = String(metadata.display_phone_number || '').trim() || null;

        const messages = Array.isArray(value.messages) ? value.messages : [];
        for (const msg of messages) {
          const messageId = String(msg?.id || '').trim();
          if (!messageId) continue;
          const from = String(msg?.from || '').trim() || null;
          const textBody = extractIncomingText(msg);
          const row = {
            id: messageId,
            source: 'whatsapp_cloud_api',
            event_type: 'message',
            message_id: messageId,
            wa_from: from,
            wa_timestamp: String(msg?.timestamp || '').trim() || null,
            wa_type: String(msg?.type || '').trim() || 'unknown',
            text_body: textBody,
            phone_number_id: phoneNumberId,
            display_phone_number: displayPhoneNumber,
            raw_payload: msg,
            process_status: 'pending'
          };
          const { error } = await supabaseAdmin.from('whatsapp_incoming').upsert(row, { onConflict: 'id' });
          if (error) throw error;
          await processIncomingMessage({
            incomingId: messageId,
            from,
            textBody,
            phoneNumberId
          });
          stored += 1;
        }

        const statuses = Array.isArray(value.statuses) ? value.statuses : [];
        for (const status of statuses) {
          const statusId = String(status?.id || '').trim();
          if (!statusId) continue;
          const row = {
            id: `status_${statusId}_${String(status?.status || 'unknown').trim()}`,
            source: 'whatsapp_cloud_api',
            event_type: 'status',
            message_id: statusId,
            wa_from: String(status?.recipient_id || '').trim() || null,
            wa_timestamp: String(status?.timestamp || '').trim() || null,
            wa_type: 'status',
            text_body: null,
            phone_number_id: phoneNumberId,
            display_phone_number: displayPhoneNumber,
            raw_payload: status,
            process_status: 'ignored'
          };
          const { error } = await supabaseAdmin.from('whatsapp_incoming').upsert(row, { onConflict: 'id' });
          if (error) throw error;
          stored += 1;
        }
      }
    }

    res.status(200).json({ ok: true, stored });
  } catch (error) {
    console.error('whatsapp webhook error', error);
    res.status(500).json({ ok: false, error: String(error?.message || error) });
  }
});

export default app;

async function processIncomingMessage({ incomingId, from, textBody, phoneNumberId }) {
  const phone = digitsOnly(from);
  if (!phone) {
    await markIncomingProcessed(incomingId, 'ignored', 'missing_phone');
    return;
  }

  const normalizedText = normalizeIncomingText(textBody);
  const session = await getOrCreateSession(phone);

  if (normalizedText === 'hola') {
    const body = 'Hola. Bienvenido al registro diario. Por favor escribe tu numero de cedula sin puntos.';
    const sent = await sendWhatsAppText(phone, body, phoneNumberId);
    if (!sent.ok) {
      await markIncomingProcessed(incomingId, 'failed', sent.error || 'send_failed');
      return;
    }
    await updateSession(phone, {
      session_state: 'awaiting_document',
      last_message_at: new Date().toISOString(),
      session_data: {
        ...(session?.session_data || {}),
        last_prompt: 'awaiting_document'
      }
    });
    await markIncomingProcessed(incomingId, 'processed', 'sent_greeting');
    return;
  }

  if (session?.session_state === 'awaiting_document') {
    const doc = phoneDigitsOrText(textBody);
    if (!/^\d{5,15}$/.test(doc)) {
      const sent = await sendWhatsAppText(phone, 'Documento no valido. Escribe tu numero de cedula sin puntos.', phoneNumberId);
      if (!sent.ok) {
        await markIncomingProcessed(incomingId, 'failed', sent.error || 'send_failed');
        return;
      }
      await markIncomingProcessed(incomingId, 'processed', 'invalid_document_prompt');
      return;
    }

    const employee = await findEmployeeByDocument(doc);
    if (!employee) {
      const sent = await sendWhatsAppText(phone, 'No fue posible encontrar tu registro de empleado. Por favor contacta a administracion.', phoneNumberId);
      if (!sent.ok) {
        await markIncomingProcessed(incomingId, 'failed', sent.error || 'send_failed');
        return;
      }
      await updateSession(phone, {
        documento: doc,
        session_state: 'idle',
        last_message_at: new Date().toISOString(),
        session_data: {
          ...(session?.session_data || {}),
          last_lookup_status: 'employee_not_found'
        }
      });
      await markIncomingProcessed(incomingId, 'processed', 'employee_not_found');
      return;
    }

    const role = await resolveEmployeeRole(employee);
    const sent = await sendRoleWelcome(phone, employee, role, phoneNumberId);
    if (!sent.ok) {
      await markIncomingProcessed(incomingId, 'failed', sent.error || 'send_failed');
      return;
    }
    await updateSession(phone, {
      employee_id: employee.id,
      documento: employee.documento,
      session_state: 'identified',
      last_message_at: new Date().toISOString(),
      session_data: {
        ...(session?.session_data || {}),
        employee_nombre: employee.nombre || null,
        employee_role: role,
        phone_linked: true
      }
    });
    await markIncomingProcessed(incomingId, 'processed', `employee_identified_${role}`);
    return;
  }

  if (session?.session_state === 'identified') {
    const role = String(session?.session_data?.employee_role || '').trim() || 'empleado';
    const normalized = normalizeIncomingText(textBody);
    const handled = await handleIdentifiedMenu({
      phone,
      text: normalized,
      role,
      session,
      phoneNumberId
    });
    if (handled.ok) {
      await updateSession(phone, {
        last_message_at: new Date().toISOString(),
        session_data: {
          ...(session?.session_data || {}),
          last_menu_action: handled.action || null
        }
      });
      await markIncomingProcessed(incomingId, 'processed', handled.reason || 'identified_menu_handled');
      return;
    }
  }

  if (session?.session_state === 'awaiting_novedad') {
    const handled = await handleAwaitingNovedad({
      phone,
      text: normalizeIncomingText(textBody),
      session,
      phoneNumberId
    });
    if (handled.ok) {
      await markIncomingProcessed(incomingId, 'processed', handled.reason || 'awaiting_novedad_handled');
      return;
    }
  }

  const sent = await sendWhatsAppText(phone, 'No entendi tu mensaje. Si deseas iniciar el registro, por favor escribe "Hola".', phoneNumberId);
  if (!sent.ok) {
    await markIncomingProcessed(incomingId, 'failed', sent.error || 'send_failed');
    return;
  }
  await updateSession(phone, {
    last_message_at: new Date().toISOString()
  });
  await markIncomingProcessed(incomingId, 'processed', 'unsupported_message');
}

function extractIncomingText(message = {}) {
  if (message?.text?.body) return String(message.text.body).trim();
  if (message?.button?.text) return String(message.button.text).trim();
  if (message?.interactive?.button_reply?.title) return String(message.interactive.button_reply.title).trim();
  if (message?.interactive?.list_reply?.title) return String(message.interactive.list_reply.title).trim();
  return '';
}

function normalizeIncomingText(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

function digitsOnly(value) {
  return String(value || '').replace(/\D+/g, '').trim();
}

function phoneDigitsOrText(value) {
  return String(value || '').replace(/[^\d]/g, '').trim();
}

async function markIncomingProcessed(id, status, reason) {
  const { error } = await supabaseAdmin
    .from('whatsapp_incoming')
    .update({
      process_status: status,
      process_reason: reason,
      processed_at: new Date().toISOString()
    })
    .eq('id', id);
  if (error) throw error;
}

async function getOrCreateSession(phone) {
  const { data, error } = await supabaseAdmin
    .from('whatsapp_sessions')
    .select('*')
    .eq('id', phone)
    .maybeSingle();
  if (error) throw error;
  if (data) return data;
  const { data: created, error: createError } = await supabaseAdmin
    .from('whatsapp_sessions')
    .insert({
      id: phone,
      phone_number: phone,
      session_state: 'idle',
      session_data: {},
      last_message_at: new Date().toISOString()
    })
    .select('*')
    .single();
  if (createError) throw createError;
  return created;
}

async function updateSession(phone, patch = {}) {
  const { error } = await supabaseAdmin
    .from('whatsapp_sessions')
    .update({
      ...patch,
      updated_at: new Date().toISOString()
    })
    .eq('id', phone);
  if (error) throw error;
}

async function findEmployeeByDocument(documento) {
  const { data, error } = await supabaseAdmin
    .from('employees')
    .select('id, documento, nombre, estado, cargo_codigo, cargo_nombre, sede_codigo, sede_nombre, zona_codigo, zona_nombre')
    .eq('documento', documento)
    .maybeSingle();
  if (error) throw error;
  if (!data) return null;
  if (String(data.estado || '').trim().toLowerCase() === 'inactivo') return null;
  return data;
}

async function resolveEmployeeRole(employee) {
  const cargoCode = String(employee?.cargo_codigo || '').trim();
  if (cargoCode) {
    const { data, error } = await supabaseAdmin
      .from('cargos')
      .select('alineacion_crud, nombre')
      .eq('codigo', cargoCode)
      .maybeSingle();
    if (error) throw error;
    const alignment = normalizeRole(data?.alineacion_crud || employee?.cargo_nombre || data?.nombre || '');
    if (alignment !== 'empleado') return alignment;
  }
  const documento = String(employee?.documento || '').trim();
  if (documento) {
    const { data, error } = await supabaseAdmin
      .from('supervisor_profile')
      .select('id')
      .eq('documento', documento)
      .maybeSingle();
    if (error) throw error;
    if (data?.id) return 'supervisor';
  }
  return normalizeRole(employee?.cargo_nombre || '');
}

function normalizeRole(value) {
  const text = normalizeIncomingText(value);
  if (text.includes('supernumer')) return 'supernumerario';
  if (text.includes('supervisor')) return 'supervisor';
  if (['supernumerario', 'supervisor', 'empleado'].includes(text)) return text;
  return 'empleado';
}

async function sendRoleWelcome(phone, employee, role, phoneNumberId) {
  const name = String(employee?.nombre || '').trim() || 'usuario';
  if (role === 'supervisor') {
    return sendWhatsAppText(
      phone,
      `Gracias ${name}. Validacion correcta.\nOpciones disponibles:\n- TRABAJANDO\n- NOVEDAD\n- ACTUALIZAR DATOS`,
      phoneNumberId
    );
  }
  return sendWhatsAppText(
    phone,
    `Gracias ${name}. Validacion correcta.\nOpciones disponibles:\n- TRABAJANDO\n- COMPENSATORIO\n- NOVEDAD`,
    phoneNumberId
  );
}

async function handleIdentifiedMenu({ phone, text, role, session, phoneNumberId }) {
  if (role === 'supervisor') {
    if (text === 'trabajando') {
      const saved = await saveAttendanceFromSession({
        sessionPhone: phone,
        asistio: true,
        novedadCodigo: null,
        novedadNombre: null
      });
      const sent = await sendWhatsAppText(phone, 'Registro exitoso: TRABAJANDO.', phoneNumberId);
      if (!sent.ok) return sent;
      await updateSession(phone, {
        session_state: 'identified',
        last_message_at: new Date().toISOString(),
        session_data: {
          ...sessionSafeData(session),
          last_attendance_id: saved.attendanceId,
          last_attendance_kind: 'trabajando'
        }
      });
      return { ok: true, action: 'supervisor_trabajando', reason: 'menu_supervisor_trabajando' };
    }
    if (text === 'novedad') {
      const sent = await sendWhatsAppText(phone, 'Escribe tu novedad: ENFERMEDAD GENERAL, ACCIDENTE LABORAL, CALAMIDAD o LICENCIA NO REMUNERADA.', phoneNumberId);
      if (!sent.ok) return sent;
      await updateSession(phone, {
        session_state: 'awaiting_novedad',
        last_message_at: new Date().toISOString(),
        session_data: {
          ...sessionSafeData(session),
          pending_flow: 'novedad'
        }
      });
      return { ok: true, action: 'supervisor_novedad', reason: 'menu_supervisor_novedad_prompt' };
    }
    if (text === 'actualizar datos') {
      const sent = await sendWhatsAppText(phone, 'Recibido. Flujo de actualizacion de datos en construccion.', phoneNumberId);
      return sent.ok ? { ok: true, action: 'supervisor_actualizar_datos', reason: 'menu_supervisor_actualizar_datos' } : sent;
    }
    return { ok: false };
  }

  if (text === 'trabajando') {
    const saved = await saveAttendanceFromSession({
      sessionPhone: phone,
      asistio: true,
      novedadCodigo: null,
      novedadNombre: null
    });
    const sent = await sendWhatsAppText(phone, 'Registro exitoso: TRABAJANDO.', phoneNumberId);
    if (!sent.ok) return sent;
    await updateSession(phone, {
        session_state: 'identified',
        last_message_at: new Date().toISOString(),
        session_data: {
          ...sessionSafeData(session),
          last_attendance_id: saved.attendanceId,
          last_attendance_kind: 'trabajando'
        }
    });
    return { ok: true, action: 'employee_trabajando', reason: 'menu_employee_trabajando' };
  }
  if (text === 'compensatorio') {
    const saved = await saveAttendanceFromSession({
      sessionPhone: phone,
      asistio: true,
      novedadCodigo: '7',
      novedadNombre: 'COMPENSATORIO'
    });
    const sent = await sendWhatsAppText(phone, 'Registro exitoso: COMPENSATORIO.', phoneNumberId);
    if (!sent.ok) return sent;
    await updateSession(phone, {
        session_state: 'identified',
        last_message_at: new Date().toISOString(),
        session_data: {
          ...sessionSafeData(session),
          last_attendance_id: saved.attendanceId,
          last_attendance_kind: 'compensatorio'
        }
    });
    return { ok: true, action: 'employee_compensatorio', reason: 'menu_employee_compensatorio' };
  }
  if (text === 'novedad') {
    const sent = await sendWhatsAppText(phone, 'Escribe tu novedad: ENFERMEDAD GENERAL, ACCIDENTE LABORAL, CALAMIDAD o LICENCIA NO REMUNERADA.', phoneNumberId);
    if (!sent.ok) return sent;
    await updateSession(phone, {
        session_state: 'awaiting_novedad',
        last_message_at: new Date().toISOString(),
        session_data: {
          ...sessionSafeData(session),
          pending_flow: 'novedad'
        }
    });
    return { ok: true, action: 'employee_novedad', reason: 'menu_employee_novedad_prompt' };
  }

  return { ok: false };
}

async function handleAwaitingNovedad({ phone, text, session, phoneNumberId }) {
  const novedad = mapNovedadInput(text);
  if (!novedad) {
    const sent = await sendWhatsAppText(phone, 'Novedad no valida. Escribe: ENFERMEDAD GENERAL, ACCIDENTE LABORAL, CALAMIDAD o LICENCIA NO REMUNERADA.', phoneNumberId);
    if (!sent.ok) return sent;
    return { ok: true, reason: 'invalid_novedad_prompt' };
  }
  const saved = await saveAttendanceFromSession({
    sessionPhone: phone,
    asistio: false,
    novedadCodigo: novedad.codigo,
    novedadNombre: novedad.nombre
  });
  const sent = await sendWhatsAppText(phone, `Registro exitoso de novedad: ${novedad.nombre}.`, phoneNumberId);
  if (!sent.ok) return sent;
  await updateSession(phone, {
    session_state: 'identified',
    last_message_at: new Date().toISOString(),
    session_data: {
      ...sessionSafeData(session),
      pending_flow: null,
      last_attendance_id: saved.attendanceId,
      last_attendance_kind: 'novedad',
      last_novedad_codigo: novedad.codigo
    }
  });
  return { ok: true, reason: 'novedad_registered' };
}

function mapNovedadInput(text) {
  const value = normalizeIncomingText(text);
  const options = [
    { codigo: '8', nombre: 'ENFERMEDAD GENERAL', aliases: ['enfermedad general', 'enfermedad'] },
    { codigo: '9', nombre: 'ACCIDENTE LABORAL', aliases: ['accidente laboral', 'accidente'] },
    { codigo: '10', nombre: 'CALAMIDAD', aliases: ['calamidad'] },
    { codigo: '11', nombre: 'LICENCIA NO REMUNERADA', aliases: ['licencia no remunerada', 'licencia'] }
  ];
  return options.find((item) => item.aliases.includes(value)) || null;
}

function sessionSafeData(session) {
  return session?.session_data && typeof session.session_data === 'object' ? session.session_data : {};
}

async function saveAttendanceFromSession({ sessionPhone, asistio, novedadCodigo, novedadNombre }) {
  const session = await getOrCreateSession(sessionPhone);
  const employeeId = session?.employee_id;
  if (!employeeId) throw new Error('session_without_employee');
  const { data: employee, error } = await supabaseAdmin
    .from('employees')
    .select('id, documento, nombre, sede_codigo, sede_nombre')
    .eq('id', employeeId)
    .single();
  if (error) throw error;
  const fecha = todayBogota();
  const attendanceId = `${fecha}_${employee.id}`;
  const attendancePayload = {
    id: attendanceId,
    fecha,
    empleado_id: employee.id,
    documento: employee.documento || null,
    nombre: employee.nombre || null,
    sede_codigo: employee.sede_codigo || null,
    sede_nombre: employee.sede_nombre || null,
    asistio: asistio === true,
    novedad: novedadNombre || null
  };
  const { error: attendanceError } = await supabaseAdmin
    .from('attendance')
    .upsert(attendancePayload, { onConflict: 'id' });
  if (attendanceError) throw attendanceError;

  if (asistio === false) {
    const { error: absError } = await supabaseAdmin
      .from('absenteeism')
      .upsert({
        id: attendanceId,
        fecha,
        empleado_id: employee.id,
        documento: employee.documento || null,
        nombre: employee.nombre || null,
        sede_codigo: employee.sede_codigo || null,
        sede_nombre: employee.sede_nombre || null,
        estado: 'pendiente'
      }, { onConflict: 'id' });
    if (absError) throw absError;
  }

  await recomputeDailyMetricsForDate(fecha);
  return { attendanceId, fecha };
}

async function recomputeDailyMetricsForDate(fecha) {
  const day = String(fecha || '').trim();
  if (!day) return;
  const [{ data: attendance }, { data: replacements }, { data: closures }] = await Promise.all([
    supabaseAdmin.from('attendance').select('id, documento, empleado_id, asistio, novedad').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('id, decision').eq('fecha', day),
    supabaseAdmin.from('daily_closures').select('id, status, locked').eq('fecha', day).maybeSingle()
  ]);
  const attendanceRows = attendance || [];
  const replacementRows = replacements || [];
  const absenteeism = attendanceRows.filter((row) => row.asistio !== true).length;
  const expected = attendanceRows.length;
  const planned = expected;
  const paidServices = attendanceRows.filter((row) => row.asistio === true || normalizeIncomingText(row.novedad) === 'compensatorio').length + replacementRows.filter((r) => r.decision === 'reemplazo').length;
  const noContracted = Math.max(0, planned - expected);
  const uniqueCount = new Set(attendanceRows.map((row) => String(row.documento || row.empleado_id || '')).filter(Boolean)).size;
  const { error } = await supabaseAdmin
    .from('daily_metrics')
    .upsert({
      id: day,
      fecha: day,
      planned,
      expected,
      unique_count: uniqueCount,
      missing: 0,
      attendance_count: attendanceRows.length,
      absenteeism,
      paid_services: paidServices,
      no_contracted: noContracted,
      closed: closures?.locked === true || String(closures?.status || '').trim() === 'closed'
    }, { onConflict: 'id' });
  if (error) throw error;
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Bogota',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date());
}

async function sendWhatsAppText(toDigits, bodyText, phoneNumberIdHint = null) {
  return sendWhatsAppPayload(
    digitsOnly(toDigits),
    {
      type: 'text',
      text: { body: String(bodyText || '').trim() }
    },
    phoneNumberIdHint
  );
}

async function sendWhatsAppPayload(toDigits, payload, phoneNumberIdHint = null) {
  const token = config.whatsappAccessToken;
  const phoneNumberId = String(phoneNumberIdHint || config.whatsappPhoneNumberId || '').trim();
  if (!token || !phoneNumberId || !toDigits) {
    return { ok: false, error: 'missing_whatsapp_credentials_or_recipient' };
  }

  const url = `https://graph.facebook.com/${config.whatsappGraphVersion}/${phoneNumberId}/messages`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      messaging_product: 'whatsapp',
      to: toDigits,
      ...payload
    })
  });

  if (!resp.ok) {
    const body = await resp.text();
    console.error('WhatsApp send API error', { status: resp.status, body });
    return { ok: false, error: `send_failed_${resp.status}` };
  }

  return { ok: true };
}

function isValidWhatsAppSignature(req) {
  if (!config.whatsappAppSecret) return true;
  const signature = String(req.get('x-hub-signature-256') || '').trim();
  if (!signature.startsWith('sha256=')) return false;
  const rawBody = Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.from(JSON.stringify(req.body || {}), 'utf8');
  const expected = `sha256=${crypto.createHmac('sha256', config.whatsappAppSecret).update(rawBody).digest('hex')}`;
  const a = Buffer.from(signature, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}
