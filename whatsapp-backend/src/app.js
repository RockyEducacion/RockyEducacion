
import crypto from 'node:crypto';
import express from 'express';
import { config } from './config.js';
import { supabaseAdmin } from './supabase.js';

const app = express();

app.use(express.json({
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
  WORKING: { code: '1', label: 'Trabajando', absenteeism: false, requiresDates: false },
  ACCIDENT: { code: '2', label: 'Accidente Laboral', absenteeism: true, requiresDates: true },
  SICKNESS: { code: '3', label: 'Enfermedad General', absenteeism: true, requiresDates: true },
  CALAMITY: { code: '4', label: 'Calamidad', absenteeism: true, requiresDates: true },
  UNPAID_LEAVE: { code: '5', label: 'Licencia No Remunerada', absenteeism: true, requiresDates: false },
  COMPENSATORY: { code: '7', label: 'Compensatorio', absenteeism: false, requiresDates: false }
};

const MENU_IDS = {
  IDENTITY_YES: 'identity_yes',
  IDENTITY_NO: 'identity_no',
  UPDATE_DATA: 'update_data',
  ACTION_WORKING: 'action_working',
  ACTION_COMPENSATORY: 'action_compensatory',
  ACTION_NOVELTY: 'action_novelty',
  UPDATE_TRANSFER: 'update_transfer',
  UPDATE_PHONE: 'update_phone',
  NOVELTY_SICKNESS: 'novelty_3',
  NOVELTY_ACCIDENT: 'novelty_2',
  NOVELTY_CALAMITY: 'novelty_4',
  NOVELTY_UNPAID: 'novelty_5'
};

const NO_REGISTERED_MESSAGE = 'No estás registrado en nuestra base de datos, por favor comunícate con tu supervisor.';

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

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
        await markIncomingProcessed(incomingId, 'failed', error.message || 'processing_failed');
      }
    }

    res.json({ ok: true });
  } catch (error) {
    console.error('Error general webhook WhatsApp:', error);
    res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

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

async function processIncomingMessage(message) {
  const phone = normalizePhone(message?.from);
  if (!phone) throw new Error('missing_phone_number');

  const session = await getSession(phone);
  const parsed = parseInboundAction(message);

  if (!parsed.value && !parsed.id) {
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
      rows: [
        { id: MENU_IDS.NOVELTY_SICKNESS, title: 'Enfermedad General' },
        { id: MENU_IDS.NOVELTY_ACCIDENT, title: 'Accidente Laboral' },
        { id: MENU_IDS.NOVELTY_CALAMITY, title: 'Calamidad' },
        { id: MENU_IDS.NOVELTY_UNPAID, title: 'Licencia No Remunerada' }
      ]
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

  const { error } = await supabaseAdmin.from('employees').update({
    sede_codigo: selected.codigo || null,
    sede_nombre: selected.nombre || null,
    zona_codigo: selected.zona_codigo || null,
    zona_nombre: selected.zona_nombre || null,
    last_modified_at: new Date().toISOString()
  }).eq('id', employee.id);
  if (error) throw error;

  const refreshed = { ...employee, sede_codigo: selected.codigo, sede_nombre: selected.nombre, zona_codigo: selected.zona_codigo, zona_nombre: selected.zona_nombre };
  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.COMPLETED,
    session_data: { employee: sessionEmployee(refreshed) }
  });
  await sendText(phone, 'Información actualizada correctamente, si no haz realizado el registro por favor escribe nuevamente "Hola".');
}

async function handleWorkingSedeSelection(phone, session, parsed) {
  const employee = await loadEmployeeFromSession(session);
  const selected = resolveSedeSelection(session, parsed, 'work_sede_');
  const novelty = session?.session_data?.pendingNovelty || NOVELTIES.WORKING;
  if (!employee || !selected) {
    await sendText(phone, 'Selecciona una sede válida del listado.');
    return;
  }

  await registerNovelty(phone, employee, novelty, selected);
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
  await sendText(phone, 'Selecciona las fechas de incapacidad:\n\nFecha de inicio de incapacidad, por favor escribe DD/MM/AAAA:');
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
    await sendText(phone, 'Fecha de inicio de incapacidad, por favor escribe DD/MM/AAAA:');
    return;
  }

  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.AWAITING_DATE_END,
    session_data: { ...(session.session_data || {}), employee: sessionEmployee(employee), pendingNovelty: novelty, incapacityStart: parsedDate }
  });
  await sendText(phone, 'Fecha de terminación de incapacidad, por favor escribe DD/MM/AAAA:');
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

  if (!endDate || endDate < startDate) {
    await sendText(phone, 'Fecha de terminación de incapacidad, por favor escribe DD/MM/AAAA:');
    return;
  }

  await registerNovelty(phone, employee, novelty, null, { startDate, endDate });
}

async function registerNovelty(phone, employee, novelty, selectedSede = null, incapacity = null) {
  const date = currentDate();
  const time = currentTime();
  const documento = normalizeDocument(employee.documento);
  const attendanceId = buildDailyRecordId(date, documento, employee.id);
  const sedeCodigo = selectedSede?.codigo || employee.sede_codigo || null;
  const sedeNombre = selectedSede?.nombre || employee.sede_nombre || null;

  const { error: attendanceError } = await supabaseAdmin.from('attendance').upsert({
    id: attendanceId,
    fecha: date,
    empleado_id: employee.id,
    documento,
    nombre: employee.nombre,
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
      empleado_id: employee.id,
      documento,
      nombre: employee.nombre,
      sede_codigo: sedeCodigo,
      sede_nombre: sedeNombre,
      estado: 'reportado_whatsapp'
    }, { onConflict: 'id' });
    if (absenteeismError) throw absenteeismError;
  }

  if (incapacity?.startDate && incapacity?.endDate) {
    const { error: incapacityError } = await supabaseAdmin.from('incapacitados').insert({
      employee_id: employee.id,
      documento,
      nombre: employee.nombre,
      fecha_inicio: incapacity.startDate,
      fecha_fin: incapacity.endDate,
      estado: 'activo',
      source: novelty.label,
      whatsapp_message_id: `${attendanceId}_${novelty.code}`
    });
    if (incapacityError) throw incapacityError;
  }

  await recomputeDailyMetrics(date);
  await storeSession(phone, {
    employee_id: employee.id,
    documento: employee.documento,
    session_state: SESSION.COMPLETED,
    session_data: { employee: sessionEmployee(employee) }
  });
  await sendText(phone, `Registro confirmado. Fecha: ${formatDateForHumans(date)}, Hora: ${time}, Novedad: ${novelty.label}, Muchas Gracias.`);
}

async function recomputeDailyMetrics(date) {
  const day = String(date || '').trim();
  const [
    { data: attendance, error: attendanceError },
    { data: replacements, error: replacementsError },
    sedesRows,
    employeesRows,
    cargosRows,
    novedadesRows
  ] = await Promise.all([
    supabaseAdmin.from('attendance').select('*').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes'),
    selectAllRows('employees'),
    selectAllRows('cargos', 'codigo, alineacion_crud, nombre'),
    selectAllRows('novedades', 'codigo, codigo_novedad, nombre, reemplazo')
  ]);
  if (attendanceError) throw attendanceError;
  if (replacementsError) throw replacementsError;

  const attRows = Array.isArray(attendance) ? attendance : [];
  const repRows = Array.isArray(replacements) ? replacements : [];
  const sedes = (sedesRows || []).filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
  const activeSedeCodes = new Set(
    sedes
      .map((row) => String(row?.codigo || '').trim())
      .filter(Boolean)
  );
  const cargoMap = new Map((cargosRows || []).map((row) => [String(row?.codigo || '').trim(), row]));
  const replacementRules = buildNovedadReplacementRules(novedadesRows || []);
  const replacementMap = new Map(repRows.map((row) => [metricReplacementKey({
    fecha: row?.fecha,
    employeeId: row?.empleado_id || row?.employee_id
  }), row]));
  const fallbackExpected = (employeesRows || []).filter((emp) => {
    if (String(emp?.estado || '').trim().toLowerCase() !== 'activo') return false;
    const sedeCodigo = String(emp?.sede_codigo || '').trim();
    if (!sedeCodigo || !activeSedeCodes.has(sedeCodigo)) return false;
    return !isEmployeeSupernumerarioByCargoMap(emp, cargoMap);
  }).length;
  const planned = sedes.reduce((sum, sede) => {
    const count = Number(sede?.numero_operarios ?? 0);
    return sum + (Number.isFinite(count) && count > 0 ? count : 0);
  }, 0);
  const expected = fallbackExpected;
  const uniqueDocs = new Set(attRows.map((row) => String(row?.documento || row?.empleado_id || '').trim()).filter(Boolean));
  const attendanceCount = attRows.filter((row) => metricAttendanceCountsAsService(row, replacementMap, replacementRules)).length;
  const absenteeism = attRows.filter((row) => metricAttendanceCountsAsAbsenteeism(row, replacementMap, replacementRules)).length;
  const paidServices = attendanceCount;
  const noContracted = Math.max(0, planned - expected);
  const { error } = await supabaseAdmin.from('daily_metrics').upsert({
    id: day,
    fecha: day,
    planned,
    expected,
    unique_count: uniqueDocs.size,
    missing: Math.max(0, expected - attendanceCount),
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
  const [
    { data: attendance, error: attendanceError },
    { data: replacements, error: replacementsError },
    sedesRows,
    employeesRows,
    cargosRows,
    novedadesRows
  ] = await Promise.all([
    supabaseAdmin.from('attendance').select('*').eq('fecha', day),
    supabaseAdmin.from('import_replacements').select('*').eq('fecha', day),
    selectAllRows('sedes'),
    selectAllRows('employees'),
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

  (employeesRows || []).forEach((emp) => {
    const empId = String(emp?.id || '').trim();
    const doc = String(emp?.documento || '').trim();
    if (empId) employeeById.set(empId, emp);
    if (doc) employeeByDoc.set(doc, emp);
    if (!isEmployeeAssignedToActiveSedeOnDate(emp, day, activeSedeCodes)) return;
    if (isEmployeeSupernumerarioByCargoMap(emp, cargoMap)) return;
    const sedeCode = String(emp?.sede_codigo || '').trim();
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
    const sedeCode = String(employee?.sede_codigo || row?.sede_codigo || '').trim();
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
    const contracted = Number(contractedBySede.get(sedeCode)?.size || 0);
    const registered = Number(registeredBySede.get(sedeCode)?.size || 0);
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

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = String(selectedDate || '').trim();
  if (!iso || !sede) return false;
  const [year, month, day] = iso.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
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
  if (['2', '3', '4', '5', '8'].includes(code)) return true;
  if (code && rules?.byCode?.has(code)) return rules.byCode.get(code) === true;
  const name = normalizeMetricText(baseMetricNovedadName(row?.novedad_nombre || row?.novedadNombre || row?.novedad || ''));
  if (name && rules?.byName?.has(name)) return rules.byName.get(name) === true;
  return false;
}

function metricReplacementKey(row = {}) {
  return `${String(row?.fecha || '').trim()}_${String(row?.empleado_id || row?.empleadoId || row?.employee_id || row?.employeeId || '').trim()}`;
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

function isEmployeeAssignedToActiveSedeOnDate(emp, selectedDate, activeSedeCodes = new Set()) {
  if (!selectedDate) return false;
  const ingreso = toISODate(emp?.fecha_ingreso || emp?.fechaIngreso);
  if (!ingreso || ingreso > selectedDate) return false;
  const retiro = toISODate(emp?.fecha_retiro || emp?.fechaRetiro);
  const estado = String(emp?.estado || '').trim().toLowerCase();
  if (estado === 'inactivo') return Boolean(retiro && retiro >= selectedDate);
  if (retiro && retiro < selectedDate) return false;
  const sedeCodigo = String(emp?.sede_codigo || emp?.sedeCodigo || '').trim();
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

async function closeOperationDay(date) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(date || '').trim())) {
    throw new Error('invalid_date');
  }

  const day = String(date).trim();
  const { data: existingClosure, error: existingClosureError } = await supabaseAdmin
    .from('daily_closures')
    .select('*')
    .eq('fecha', day)
    .maybeSingle();
  if (existingClosureError) throw existingClosureError;

  if (existingClosure?.locked === true || String(existingClosure?.status || '').trim().toLowerCase() === 'closed') {
    return { date: day, status: 'already_closed' };
  }

  await finalizePendingAbsenteeismForClosure(day);
  await recomputeSedeStatusSnapshot(day);

  const { data: metricRow, error: metricError } = await supabaseAdmin
    .from('daily_metrics')
    .select('*')
    .eq('fecha', day)
    .maybeSingle();
  if (metricError) throw metricError;

  const metrics = metricRow || (await recomputeAndFetchDailyMetrics(day));
  const { error: closureError } = await supabaseAdmin
    .from('daily_closures')
    .upsert({
      id: day,
      fecha: day,
      status: 'closed',
      locked: true,
      planeados: Number(metrics?.planned || 0),
      contratados: Number(metrics?.expected || 0),
      asistencias: Number(metrics?.attendance_count || metrics?.attendanceCount || 0),
      ausentismos: Number(metrics?.absenteeism || 0),
      no_contratados: Number(metrics?.no_contracted || metrics?.noContracted || 0),
      closed_by_uid: null,
      closed_by_email: 'cron@system'
    }, { onConflict: 'id' });
  if (closureError) throw closureError;

  const { error: metricCloseError } = await supabaseAdmin
    .from('daily_metrics')
    .update({ closed: true })
    .eq('fecha', day);
  if (metricCloseError) throw metricCloseError;

  await propagateIncapacitiesToNextDay(day);

  return { date: day, status: 'closed' };
}

async function recomputeAndFetchDailyMetrics(date) {
  await recomputeDailyMetrics(date);
  const { data, error } = await supabaseAdmin
    .from('daily_metrics')
    .select('*')
    .eq('fecha', date)
    .maybeSingle();
  if (error) throw error;
  return data || null;
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

  await recomputeDailyMetrics(nextDay);
}

async function findEmployeeByPhone(phone) {
  const normalized = normalizePhone(phone);
  if (!normalized) return null;
  const last10 = normalized.slice(-10);
  const variants = [...new Set([normalized, last10, `57${last10}`])];
  const orQuery = variants.map((value) => `telefono.eq.${value}`).join(',');
  let query = supabaseAdmin.from('employees').select('*').eq('estado', 'activo');
  if (orQuery) query = query.or(orQuery);
  const { data, error } = await query.limit(5);
  if (error) throw error;

  let employee = (data || []).find((row) => normalizePhone(row.telefono) === normalized);
  if (!employee) {
    const { data: fallback, error: fallbackError } = await supabaseAdmin.from('employees').select('*').eq('estado', 'activo').ilike('telefono', `%${last10}%`).limit(20);
    if (fallbackError) throw fallbackError;
    employee = (fallback || []).find((row) => normalizePhone(row.telefono) === normalized) || null;
  }
  return employee ? hydrateEmployee(employee) : null;
}

async function findEmployeeByDocument(document) {
  const { data, error } = await supabaseAdmin.from('employees').select('*').eq('documento', document).eq('estado', 'activo').maybeSingle();
  if (error) throw error;
  return data ? hydrateEmployee(data) : null;
}

async function hydrateEmployee(row) {
  const employee = { ...row };
  employee.telefono = normalizePhone(employee.telefono);
  employee.isSupernumerario = await isEmployeeSupernumerario(employee);
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
    value: String(buttonReply?.title || listReply?.title || textValue || '').trim()
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
  return null;
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

function normalizeKey(value) {
  return String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-zA-Z0-9]+/g, '').toLowerCase().trim();
}

function parseInputDate(value) {
  const match = String(value || '').trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  const [, day, month, year] = match;
  const iso = `${year}-${month}-${day}`;
  const date = new Date(`${iso}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
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

function truncate(value, max) {
  const text = String(value || '').trim();
  if (text.length <= max) return text;
  return `${text.slice(0, Math.max(0, max - 3))}...`;
}

export default app;



