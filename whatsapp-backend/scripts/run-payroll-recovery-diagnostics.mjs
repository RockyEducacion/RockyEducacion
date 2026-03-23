import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..', '..');

const args = new Map(
  process.argv.slice(2).map((arg) => {
    const [key, ...rest] = String(arg || '').split('=');
    return [key, rest.join('=')];
  })
);

const fromDate = args.get('--from') || '2026-03-19';
const toDate = args.get('--to') || '2026-03-23';
const backupDirArg = args.get('--backup-dir') || '';
const outputFileArg = args.get('--output') || '';

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '').trim());
}

if (!isIsoDate(fromDate) || !isIsoDate(toDate)) {
  throw new Error('Usa fechas ISO validas, por ejemplo --from=2026-03-19 --to=2026-03-23');
}

function normalizeDigits(value) {
  return String(value || '').replace(/\D+/g, '');
}

function toBogotaDateFromTimestamp(value, fallback) {
  if (/^\d+$/.test(String(value || '').trim())) {
    const dt = new Date(Number(value) * 1000);
    return new Intl.DateTimeFormat('sv-SE', {
      timeZone: 'America/Bogota',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).format(dt);
  }
  if (fallback) {
    const dt = new Date(fallback);
    if (!Number.isNaN(dt.getTime())) {
      return new Intl.DateTimeFormat('sv-SE', {
        timeZone: 'America/Bogota',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      }).format(dt);
    }
  }
  return null;
}

function compareIso(a, b) {
  return String(a || '').localeCompare(String(b || ''));
}

function inRange(value, from, to) {
  return compareIso(value, from) >= 0 && compareIso(value, to) <= 0;
}

function lower(value) {
  return String(value || '').trim().toLowerCase();
}

async function findLatestBackupDir() {
  const backupsRoot = path.join(projectRoot, 'backups');
  const entries = await fs.readdir(backupsRoot, { withFileTypes: true });
  const dirs = entries
    .filter((entry) => entry.isDirectory() && entry.name.startsWith('supabase-backup-'))
    .map((entry) => entry.name)
    .sort((a, b) => b.localeCompare(a));
  if (!dirs.length) throw new Error('No se encontraron backups en /backups');
  return path.join(backupsRoot, dirs[0]);
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

function countBy(rows, getKey) {
  const map = new Map();
  for (const row of rows) {
    const key = getKey(row);
    map.set(key, (map.get(key) || 0) + 1);
  }
  return map;
}

function sumBy(rows, getKey, getValue) {
  const map = new Map();
  for (const row of rows) {
    const key = getKey(row);
    map.set(key, (map.get(key) || 0) + Number(getValue(row) || 0));
  }
  return map;
}

function buildDateList(from, to) {
  const out = [];
  const cursor = new Date(`${from}T00:00:00Z`);
  const limit = new Date(`${to}T00:00:00Z`);
  while (cursor <= limit) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

async function main() {
  const backupDir = backupDirArg ? path.resolve(projectRoot, backupDirArg) : await findLatestBackupDir();
  const outputFile = outputFileArg
    ? path.resolve(projectRoot, outputFileArg)
    : path.join(
        backupDir,
        `payroll-recovery-diagnostics-${fromDate}_to_${toDate}.json`
      );

  const [
    attendance,
    importReplacements,
    sedeStatus,
    dailyMetrics,
    dailyClosures,
    importHistory,
    employees,
    whatsappSessions,
    whatsappIncoming
  ] = await Promise.all([
    readJson(path.join(backupDir, 'attendance.json')),
    readJson(path.join(backupDir, 'import_replacements.json')),
    readJson(path.join(backupDir, 'sede_status.json')),
    readJson(path.join(backupDir, 'daily_metrics.json')),
    readJson(path.join(backupDir, 'daily_closures.json')),
    readJson(path.join(backupDir, 'import_history.json')),
    readJson(path.join(backupDir, 'employees.json')),
    readJson(path.join(backupDir, 'whatsapp_sessions.json')),
    readJson(path.join(backupDir, 'whatsapp_incoming.json'))
  ]);

  const attendanceRange = attendance.filter((row) => inRange(String(row.fecha || ''), fromDate, toDate));
  const replacementsRange = importReplacements.filter((row) => inRange(String(row.fecha || ''), fromDate, toDate));
  const sedeStatusRange = sedeStatus.filter((row) => inRange(String(row.fecha || ''), fromDate, toDate));
  const metricsRange = dailyMetrics.filter((row) => inRange(String(row.fecha || ''), fromDate, toDate));
  const closuresRange = dailyClosures.filter((row) => inRange(String(row.fecha || ''), fromDate, toDate));
  const importHistoryRange = importHistory.filter((row) => isIsoDate(row.fecha_operacion) && inRange(String(row.fecha_operacion), fromDate, toDate));

  const attendanceByDay = countBy(attendanceRange, (row) => String(row.fecha || ''));
  const attendancePresentByDay = new Map();
  for (const row of attendanceRange) {
    const key = String(row.fecha || '');
    if (row.asistio === true) attendancePresentByDay.set(key, (attendancePresentByDay.get(key) || 0) + 1);
  }
  const replacementsByDay = countBy(replacementsRange, (row) => String(row.fecha || ''));
  const replacementsEffectiveByDay = new Map();
  for (const row of replacementsRange) {
    const key = String(row.fecha || '');
    if (lower(row.decision) === 'reemplazo') replacementsEffectiveByDay.set(key, (replacementsEffectiveByDay.get(key) || 0) + 1);
  }
  const sedeStatusRowsByDay = countBy(sedeStatusRange, (row) => String(row.fecha || ''));
  const sedeStatusExpectedByDay = sumBy(sedeStatusRange, (row) => String(row.fecha || ''), (row) => row.operarios_esperados);
  const sedeStatusPresentByDay = sumBy(sedeStatusRange, (row) => String(row.fecha || ''), (row) => row.operarios_presentes);
  const sedeStatusMissingByDay = sumBy(sedeStatusRange, (row) => String(row.fecha || ''), (row) => row.faltantes);
  const metricsByDay = new Map(metricsRange.map((row) => [String(row.fecha || ''), row]));
  const closuresByDay = new Map(closuresRange.map((row) => [String(row.fecha || ''), row]));
  const importHistoryRowsByDay = countBy(importHistoryRange, (row) => String(row.fecha_operacion || ''));
  const importPlannedByDay = sumBy(importHistoryRange, (row) => String(row.fecha_operacion || ''), (row) => row.planned_count);
  const importExpectedByDay = sumBy(importHistoryRange, (row) => String(row.fecha_operacion || ''), (row) => row.expected_count);
  const importFoundByDay = sumBy(importHistoryRange, (row) => String(row.fecha_operacion || ''), (row) => row.found_count);
  const importMissingByDay = sumBy(importHistoryRange, (row) => String(row.fecha_operacion || ''), (row) => row.missing_count);

  const employeeById = new Map(employees.map((row) => [String(row.id || ''), row]));
  const employeeByDocumento = new Map(employees.map((row) => [String(row.documento || ''), row]));
  const sessionByPhone = new Map(whatsappSessions.map((row) => [normalizeDigits(row.id), row]));

  const incomingBase = whatsappIncoming
    .filter((row) => String(row.event_type || '') === 'message')
    .map((row) => {
      const operationDate = toBogotaDateFromTimestamp(row.wa_timestamp, row.processed_at || row.received_at);
      return {
        id: row.id,
        messageId: row.message_id || null,
        waFrom: row.wa_from || null,
        waFromDigits: normalizeDigits(row.wa_from),
        textBody: row.text_body || null,
        processStatus: row.process_status || null,
        processReason: row.process_reason || null,
        receivedAt: row.received_at || null,
        processedAt: row.processed_at || null,
        rawPayload: row.raw_payload || {},
        interactiveId:
          row.raw_payload?.interactive?.button_reply?.id ||
          row.raw_payload?.interactive?.list_reply?.id ||
          row.raw_payload?.button?.id ||
          '',
        interactiveTitle:
          row.raw_payload?.interactive?.button_reply?.title ||
          row.raw_payload?.interactive?.list_reply?.title ||
          row.raw_payload?.button?.text ||
          '',
        operationDate,
        eventTs: row.processed_at || row.received_at || null
      };
    })
    .filter((row) => row.operationDate && inRange(row.operationDate, fromDate, toDate));

  const finalActions = incomingBase
    .map((row) => {
      const interactiveId = lower(row.interactiveId);
      const textBody = lower(row.textBody);
      let noveltyCode = null;
      if (['action_working', 'daily_trabajando'].includes(interactiveId) || textBody === 'trabajando') noveltyCode = '1';
      if (['action_compensatory', 'daily_compensatorio'].includes(interactiveId) || textBody === 'compensatorio') noveltyCode = '7';
      return { ...row, noveltyCode };
    })
    .filter((row) => row.noveltyCode);

  const incomingByPhoneDay = new Map();
  for (const row of incomingBase) {
    const key = `${row.waFromDigits}|${row.operationDate}`;
    if (!incomingByPhoneDay.has(key)) incomingByPhoneDay.set(key, []);
    incomingByPhoneDay.get(key).push(row);
  }
  for (const rows of incomingByPhoneDay.values()) {
    rows.sort((a, b) => String(b.eventTs || '').localeCompare(String(a.eventTs || '')));
  }

  const recoveryRows = finalActions.map((row) => {
    const session = sessionByPhone.get(row.waFromDigits) || null;
    const previousMsgs = incomingByPhoneDay.get(`${row.waFromDigits}|${row.operationDate}`) || [];
    const docMsg = previousMsgs.find((prev) => {
      if (String(prev.id) === String(row.id)) return false;
      if (String(prev.eventTs || '') > String(row.eventTs || '')) return false;
      return /^\d{5,}$/.test(normalizeDigits(prev.textBody));
    }) || null;
    const inferredDocumento = docMsg ? normalizeDigits(docMsg.textBody) : null;
    const resolvedDocumento = String(session?.documento || '').trim() || inferredDocumento || null;
    const employee =
      (session?.employee_id && employeeById.get(String(session.employee_id))) ||
      (resolvedDocumento && employeeByDocumento.get(String(resolvedDocumento))) ||
      null;
    const attendanceExists = attendanceRange.find((att) => {
      if (String(att.fecha || '') !== row.operationDate) return false;
      if (employee?.id && String(att.empleado_id || '') === String(employee.id)) return true;
      if (employee?.documento && String(att.documento || '') === String(employee.documento)) return true;
      return false;
    }) || null;

    let recoveryStatus = 'ready_to_insert';
    if (row.processStatus !== 'processed') recoveryStatus = 'skip_not_processed';
    else if (attendanceExists) recoveryStatus = 'skip_attendance_exists';
    else if (!employee) recoveryStatus = 'skip_employee_not_resolved';
    else if (lower(employee.estado || 'activo') !== 'activo') recoveryStatus = 'skip_employee_inactive';

    return {
      operationDate: row.operationDate,
      waFrom: row.waFrom,
      interactiveId: row.interactiveId,
      textBody: row.textBody,
      noveltyCode: row.noveltyCode,
      processStatus: row.processStatus,
      processReason: row.processReason,
      sessionState: session?.session_state || null,
      sessionEmployeeId: session?.employee_id || null,
      sessionDocumento: session?.documento || null,
      inferredDocumento,
      resolvedDocumento,
      employeeId: employee?.id || null,
      employeeDocumento: employee?.documento || null,
      employeeNombre: employee?.nombre || null,
      employeeSedeCodigo: employee?.sede_codigo || null,
      employeeSedeNombre: employee?.sede_nombre || null,
      employeeEstado: employee?.estado || null,
      attendanceId: attendanceExists?.id || null,
      processedAt: row.processedAt,
      receivedAt: row.receivedAt,
      recoveryStatus,
      targetAttendanceId: employee?.documento
        ? `${row.operationDate}_${normalizeDigits(employee.documento)}`
        : employee?.id
          ? `${row.operationDate}_${employee.id}`
          : null
    };
  });

  const recoverySummaryMap = new Map();
  for (const row of recoveryRows) {
    const key = `${row.operationDate}|${row.recoveryStatus}`;
    recoverySummaryMap.set(key, (recoverySummaryMap.get(key) || 0) + 1);
  }

  const previewCandidates = Array.from(
    recoveryRows
      .filter((row) => row.recoveryStatus === 'ready_to_insert' && row.targetAttendanceId)
      .reduce((map, row) => {
        const current = map.get(row.targetAttendanceId);
        if (!current || String(row.processedAt || row.receivedAt || '') > String(current.processedAt || current.receivedAt || '')) {
          map.set(row.targetAttendanceId, row);
        }
        return map;
      }, new Map()).values()
  ).sort((a, b) => {
    const byDate = compareIso(a.operationDate, b.operationDate);
    if (byDate !== 0) return byDate;
    return String(a.employeeNombre || a.employeeDocumento || '').localeCompare(String(b.employeeNombre || b.employeeDocumento || ''));
  });

  const daySummary = buildDateList(fromDate, toDate).map((date) => {
    const metrics = metricsByDay.get(date) || null;
    const closure = closuresByDay.get(date) || null;
    return {
      operationDate: date,
      attendanceRows: attendanceByDay.get(date) || 0,
      attendancePresentRows: attendancePresentByDay.get(date) || 0,
      replacementRows: replacementsByDay.get(date) || 0,
      replacementEffectiveRows: replacementsEffectiveByDay.get(date) || 0,
      sedeStatusRows: sedeStatusRowsByDay.get(date) || 0,
      sedeStatusExpected: sedeStatusExpectedByDay.get(date) || 0,
      sedeStatusPresent: sedeStatusPresentByDay.get(date) || 0,
      sedeStatusMissing: sedeStatusMissingByDay.get(date) || 0,
      metricsPlanned: Number(metrics?.planned || 0),
      metricsExpected: Number(metrics?.expected || 0),
      metricsUnique: Number(metrics?.unique_count || 0),
      metricsMissing: Number(metrics?.missing || 0),
      metricsAttendanceCount: Number(metrics?.attendance_count || 0),
      metricsAbsenteeism: Number(metrics?.absenteeism || 0),
      metricsPaidServices: Number(metrics?.paid_services || 0),
      metricsNoContracted: Number(metrics?.no_contracted || 0),
      metricsClosed: metrics?.closed === true,
      closureStatus: closure?.status || null,
      closureLocked: closure?.locked === true,
      closurePlanned: Number(closure?.planeados || 0),
      closureExpected: Number(closure?.contratados || 0),
      closureAttendance: Number(closure?.asistencias || 0),
      closureAbsenteeism: Number(closure?.ausentismos || 0),
      closureNoContracted: Number(closure?.no_contratados || 0),
      importHistoryRows: importHistoryRowsByDay.get(date) || 0,
      importPlanned: importPlannedByDay.get(date) || 0,
      importExpected: importExpectedByDay.get(date) || 0,
      importFound: importFoundByDay.get(date) || 0,
      importMissing: importMissingByDay.get(date) || 0,
      deltaMetricsVsAttendance: Number(metrics?.attendance_count || 0) - (attendancePresentByDay.get(date) || 0),
      deltaClosureVsAttendance: Number(closure?.asistencias || 0) - (attendancePresentByDay.get(date) || 0)
    };
  });

  const result = {
    sourceBackupDir: backupDir,
    fromDate,
    toDate,
    createdAt: new Date().toISOString(),
    daySummary,
    recoverySummary: Array.from(recoverySummaryMap.entries())
      .map(([key, total]) => {
        const [operationDate, recoveryStatus] = key.split('|');
        return { operationDate, recoveryStatus, total };
      })
      .sort((a, b) => {
        const byDate = compareIso(a.operationDate, b.operationDate);
        if (byDate !== 0) return byDate;
        return String(a.recoveryStatus).localeCompare(String(b.recoveryStatus));
      }),
    previewCandidates
  };

  await fs.writeFile(outputFile, JSON.stringify(result, null, 2), 'utf8');

  console.log(`Backup analizado: ${backupDir}`);
  console.log(`Rango: ${fromDate} a ${toDate}`);
  console.log('');
  console.log('Resumen por dia:');
  for (const row of daySummary) {
    console.log(
      [
        row.operationDate,
        `attendance=${row.attendancePresentRows}`,
        `metrics=${row.metricsAttendanceCount}`,
        `closure=${row.closureAttendance}`,
        `deltaMetrics=${row.deltaMetricsVsAttendance}`,
        `deltaClosure=${row.deltaClosureVsAttendance}`,
        `reemplazos=${row.replacementEffectiveRows}`
      ].join(' | ')
    );
  }
  console.log('');
  console.log('Recovery summary:');
  for (const row of result.recoverySummary) {
    console.log(`${row.operationDate} | ${row.recoveryStatus} | ${row.total}`);
  }
  console.log('');
  console.log(`Preview candidates: ${previewCandidates.length}`);
  console.log(`Reporte guardado en ${outputFile}`);
}

main().catch((error) => {
  console.error('No se pudo ejecutar el diagnostico:', error);
  process.exitCode = 1;
});

