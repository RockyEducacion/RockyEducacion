-- Diagnostico de acciones finales de WhatsApp que deberian generar attendance.
-- Ajusta las fechas del bloque params antes de ejecutar.

drop table if exists tmp_whatsapp_final_actions_diagnostic;

create temporary table tmp_whatsapp_final_actions_diagnostic as
with params as (
  select
    date '2026-03-01' as from_date,
    date '2026-03-31' as to_date
),
incoming_base as (
  select
    wi.id,
    wi.message_id,
    wi.wa_from,
    regexp_replace(coalesce(wi.wa_from, ''), '\D', '', 'g') as wa_from_digits,
    wi.text_body,
    wi.process_status,
    wi.process_reason,
    wi.received_at,
    wi.processed_at,
    wi.wa_timestamp,
    wi.raw_payload,
    coalesce(
      wi.raw_payload #>> '{interactive,button_reply,id}',
      wi.raw_payload #>> '{interactive,list_reply,id}',
      wi.raw_payload #>> '{button,id}'
    ) as interactive_id,
    coalesce(
      wi.raw_payload #>> '{interactive,button_reply,title}',
      wi.raw_payload #>> '{interactive,list_reply,title}',
      wi.raw_payload #>> '{button,text}'
    ) as interactive_title,
    case
      when coalesce(wi.wa_timestamp, '') ~ '^\d+$'
        then (timezone('America/Bogota', to_timestamp(wi.wa_timestamp::bigint)))::date
      else (timezone('America/Bogota', coalesce(wi.processed_at, wi.received_at)))::date
    end as operation_date
  from public.whatsapp_incoming wi
  where wi.event_type = 'message'
),
final_actions as (
  select
    ib.*,
    lower(coalesce(ib.interactive_id, '')) as interactive_id_norm,
    lower(coalesce(ib.interactive_title, '')) as interactive_title_norm,
    lower(coalesce(ib.text_body, '')) as text_body_norm
  from incoming_base ib
  where
    lower(coalesce(ib.interactive_id, '')) in (
      'action_working',
      'action_compensatory',
      'action_novelty'
    )
    or lower(coalesce(ib.interactive_id, '')) like 'work_sede_%'
    or lower(coalesce(ib.interactive_id, '')) like 'novelty_%'
    or lower(coalesce(ib.text_body, '')) in (
      'trabajando',
      'compensatorio'
    )
),
session_match as (
  select
    fa.*,
    ws.id as session_id,
    ws.employee_id as session_employee_id,
    ws.documento as session_documento,
    ws.session_state,
    ws.updated_at as session_updated_at
  from final_actions fa
  left join public.whatsapp_sessions ws
    on regexp_replace(coalesce(ws.id, ''), '\D', '', 'g') = fa.wa_from_digits
),
attendance_match as (
  select
    sm.*,
    a.id as attendance_id,
    a.fecha as attendance_fecha,
    a.documento as attendance_documento,
    a.empleado_id as attendance_employee_id,
    a.nombre as attendance_nombre,
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
    when am.session_documento is null and am.session_employee_id is null then 'final_action_without_identity'
    else 'final_action_without_attendance'
  end as diagnostic_status
from attendance_match am, params
where am.operation_date between params.from_date and params.to_date;

-- 1) Resumen
select
  diagnostic_status,
  count(*) as total
from tmp_whatsapp_final_actions_diagnostic
group by diagnostic_status
order by total desc;

-- 2) Casos finales procesados sin attendance
select
  operation_date,
  wa_from,
  interactive_id,
  interactive_title,
  text_body,
  process_status,
  process_reason,
  session_documento,
  session_employee_id,
  session_state,
  attendance_id,
  diagnostic_status,
  received_at,
  processed_at
from tmp_whatsapp_final_actions_diagnostic
where diagnostic_status = 'final_action_without_attendance'
order by operation_date desc, processed_at desc nulls last, received_at desc;

-- 3) Casos finales que si llegaron a attendance
select
  operation_date,
  wa_from,
  interactive_id,
  interactive_title,
  text_body,
  session_documento,
  attendance_id,
  attendance_fecha,
  attendance_documento,
  attendance_novedad,
  attendance_created_at
from tmp_whatsapp_final_actions_diagnostic
where diagnostic_status = 'attendance_exists'
order by operation_date desc, attendance_created_at desc nulls last;

-- 4) Casos finales sin identidad de sesion
select
  operation_date,
  wa_from,
  interactive_id,
  interactive_title,
  text_body,
  process_status,
  process_reason,
  session_state,
  received_at,
  processed_at
from tmp_whatsapp_final_actions_diagnostic
where diagnostic_status = 'final_action_without_identity'
order by operation_date desc, processed_at desc nulls last;
