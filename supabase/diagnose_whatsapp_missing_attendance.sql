-- Diagnostico de mensajes WhatsApp procesados que no terminaron en attendance.
-- Ajusta las fechas del bloque params antes de ejecutar.

drop table if exists tmp_whatsapp_attendance_diagnostic;

create temporary table tmp_whatsapp_attendance_diagnostic as
with params as (
  select
    date '2026-03-01' as from_date,
    date '2026-03-31' as to_date
),
incoming_base as (
  select
    wi.id,
    wi.message_id,
    wi.event_type,
    wi.wa_from,
    regexp_replace(coalesce(wi.wa_from, ''), '\D', '', 'g') as wa_from_digits,
    wi.text_body,
    wi.process_status,
    wi.process_reason,
    wi.received_at,
    wi.processed_at,
    wi.wa_timestamp,
    case
      when coalesce(wi.wa_timestamp, '') ~ '^\d+$'
        then (timezone('America/Bogota', to_timestamp(wi.wa_timestamp::bigint)))::date
      else (timezone('America/Bogota', coalesce(wi.processed_at, wi.received_at)))::date
    end as operation_date,
    wi.raw_payload
  from public.whatsapp_incoming wi
  where wi.event_type = 'message'
),
session_match as (
  select
    ib.*,
    ws.id as session_id,
    ws.employee_id as session_employee_id,
    ws.documento as session_documento,
    ws.session_state,
    ws.updated_at as session_updated_at
  from incoming_base ib
  left join public.whatsapp_sessions ws
    on regexp_replace(coalesce(ws.id, ''), '\D', '', 'g') = ib.wa_from_digits
),
attendance_match as (
  select
    sm.*,
    a.id as attendance_id,
    a.fecha as attendance_fecha,
    a.documento as attendance_documento,
    a.empleado_id as attendance_employee_id,
    a.nombre as attendance_nombre,
    a.sede_codigo as attendance_sede_codigo,
    a.novedad as attendance_novedad,
    a.created_at as attendance_created_at
  from session_match sm
  left join public.attendance a
    on a.fecha = sm.operation_date::text
   and (
     (sm.session_documento is not null and a.documento = sm.session_documento)
     or
     (sm.session_employee_id is not null and a.empleado_id = sm.session_employee_id)
   )
)
select
  am.*,
  case
    when am.process_status <> 'processed' then 'incoming_failed'
    when am.attendance_id is not null then 'attendance_exists'
    when am.session_documento is null and am.session_employee_id is null then 'processed_without_session_identity'
    when am.session_state <> 'completed' then 'session_not_completed'
    else 'processed_completed_without_attendance'
  end as diagnostic_status
from attendance_match am, params
where am.operation_date between params.from_date and params.to_date;

-- 1) Resumen por estado diagnostico
select
  diagnostic_status,
  count(*) as total
from tmp_whatsapp_attendance_diagnostic
group by diagnostic_status
order by total desc;

-- 2) Detalle de casos procesados/completados sin attendance
select
  operation_date,
  wa_from,
  text_body,
  process_status,
  process_reason,
  session_documento,
  session_employee_id,
  session_state,
  session_updated_at,
  attendance_id,
  diagnostic_status,
  received_at,
  processed_at
from tmp_whatsapp_attendance_diagnostic
where diagnostic_status = 'processed_completed_without_attendance'
order by operation_date desc, processed_at desc nulls last, received_at desc;

-- 3) Casos que si tienen attendance para contrastar
select
  operation_date,
  wa_from,
  text_body,
  session_documento,
  attendance_id,
  attendance_fecha,
  attendance_documento,
  attendance_novedad,
  attendance_created_at
from tmp_whatsapp_attendance_diagnostic
where diagnostic_status = 'attendance_exists'
order by operation_date desc, attendance_created_at desc nulls last;

-- 4) Casos procesados sin identidad en sesion
select
  operation_date,
  wa_from,
  text_body,
  process_status,
  process_reason,
  session_state,
  received_at,
  processed_at
from tmp_whatsapp_attendance_diagnostic
where diagnostic_status = 'processed_without_session_identity'
order by operation_date desc, processed_at desc nulls last;
