-- Recuperacion de attendance faltante desde whatsapp_incoming.
-- Version optimizada para SQL Editor de Supabase.
--
-- Recomendacion:
-- 1) Corre por un solo dia primero.
-- 2) Revisa el preview.
-- 3) Si todo se ve bien, descomenta el INSERT.

drop table if exists tmp_recovery_incoming_base;
drop table if exists tmp_recovery_final_actions;
drop table if exists tmp_whatsapp_attendance_recovery;

-- Ajusta estas fechas. Idealmente usa un solo dia por corrida.
create temporary table tmp_recovery_params as
select
  date '2026-03-12' as from_date,
  date '2026-03-12' as to_date;

create temporary table tmp_recovery_incoming_base as
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
  end as operation_date,
  coalesce(wi.processed_at, wi.received_at, now()) as event_ts
from public.whatsapp_incoming wi, tmp_recovery_params p
where wi.event_type = 'message'
  and (
    (coalesce(wi.wa_timestamp, '') ~ '^\d+$'
      and (timezone('America/Bogota', to_timestamp(wi.wa_timestamp::bigint)))::date between p.from_date and p.to_date)
    or
    (not (coalesce(wi.wa_timestamp, '') ~ '^\d+$')
      and (timezone('America/Bogota', coalesce(wi.processed_at, wi.received_at)))::date between p.from_date and p.to_date)
  );

create index tmp_recovery_incoming_base_phone_date_ts_idx
  on tmp_recovery_incoming_base (wa_from_digits, operation_date, event_ts desc);

create temporary table tmp_recovery_final_actions as
select
  ib.*,
  case
    when lower(coalesce(ib.interactive_id, '')) in ('action_working', 'daily_trabajando')
      or lower(coalesce(ib.text_body, '')) = 'trabajando'
      then '1'
    when lower(coalesce(ib.interactive_id, '')) in ('action_compensatory', 'daily_compensatorio')
      or lower(coalesce(ib.text_body, '')) = 'compensatorio'
      then '7'
    else null
  end as novelty_code
from tmp_recovery_incoming_base ib
where
  lower(coalesce(ib.interactive_id, '')) in (
    'action_working',
    'action_compensatory',
    'daily_trabajando',
    'daily_compensatorio'
  )
  or lower(coalesce(ib.text_body, '')) in ('trabajando', 'compensatorio');

create index tmp_recovery_final_actions_phone_date_ts_idx
  on tmp_recovery_final_actions (wa_from_digits, operation_date, event_ts desc);

create temporary table tmp_whatsapp_attendance_recovery as
with inferred_docs as (
  select
    fa.id as final_id,
    prev_doc.documento as inferred_documento
  from tmp_recovery_final_actions fa
  left join lateral (
    select regexp_replace(coalesce(prev.text_body, ''), '\D', '', 'g') as documento
    from tmp_recovery_incoming_base prev
    where prev.wa_from_digits = fa.wa_from_digits
      and prev.operation_date = fa.operation_date
      and prev.event_ts <= fa.event_ts
      and regexp_replace(coalesce(prev.text_body, ''), '\D', '', 'g') ~ '^\d{5,}$'
    order by prev.event_ts desc
    limit 1
  ) prev_doc on true
),
session_match as (
  select
    fa.*,
    ws.employee_id as session_employee_id,
    ws.documento as session_documento,
    ws.session_state
  from tmp_recovery_final_actions fa
  left join public.whatsapp_sessions ws
    on regexp_replace(coalesce(ws.id, ''), '\D', '', 'g') = fa.wa_from_digits
),
resolved_identity as (
  select
    sm.*,
    idoc.inferred_documento,
    coalesce(nullif(sm.session_documento, ''), nullif(idoc.inferred_documento, '')) as resolved_documento
  from session_match sm
  left join inferred_docs idoc
    on idoc.final_id = sm.id
),
employee_match as (
  select
    ri.*,
    e.id as employee_id,
    e.documento as employee_documento,
    e.nombre as employee_nombre,
    e.sede_codigo as employee_sede_codigo,
    e.sede_nombre as employee_sede_nombre,
    e.estado as employee_estado
  from resolved_identity ri
  left join public.employees e
    on (
      (ri.session_employee_id is not null and e.id = ri.session_employee_id)
      or
      (ri.resolved_documento is not null and e.documento = ri.resolved_documento)
    )
),
attendance_match as (
  select
    em.*,
    a.id as attendance_id
  from employee_match em
  left join public.attendance a
    on a.fecha = em.operation_date::text
   and (
     (em.employee_id is not null and a.empleado_id = em.employee_id)
     or
     (em.employee_documento is not null and a.documento = em.employee_documento)
   )
)
select
  am.id as incoming_id,
  am.message_id,
  am.operation_date,
  am.wa_from,
  am.interactive_id,
  am.interactive_title,
  am.text_body,
  am.novelty_code,
  am.process_status,
  am.process_reason,
  am.session_state,
  am.session_employee_id,
  am.session_documento,
  am.inferred_documento,
  am.resolved_documento,
  am.employee_id,
  am.employee_documento,
  am.employee_nombre,
  am.employee_sede_codigo,
  am.employee_sede_nombre,
  am.employee_estado,
  am.attendance_id,
  am.received_at,
  am.processed_at,
  case
    when am.process_status <> 'processed' then 'skip_not_processed'
    when am.novelty_code is null then 'skip_unknown_action'
    when am.attendance_id is not null then 'skip_attendance_exists'
    when am.employee_id is null then 'skip_employee_not_resolved'
    when coalesce(am.employee_estado, 'activo') <> 'activo' then 'skip_employee_inactive'
    else 'ready_to_insert'
  end as recovery_status,
  case
    when coalesce(am.employee_documento, '') <> '' then am.operation_date::text || '_' || regexp_replace(am.employee_documento, '\D', '', 'g')
    else am.operation_date::text || '_' || am.employee_id::text
  end as target_attendance_id
from attendance_match am;

-- 1) Resumen de recuperacion
select
  recovery_status,
  count(*) as total
from tmp_whatsapp_attendance_recovery
group by recovery_status
order by total desc;

-- 2) Preview de candidatos listos para insertar
with ranked as (
  select
    *,
    row_number() over (
      partition by target_attendance_id
      order by processed_at desc nulls last, received_at desc, incoming_id desc
    ) as rn,
    count(*) over (partition by target_attendance_id) as dup_count
  from tmp_whatsapp_attendance_recovery
  where recovery_status = 'ready_to_insert'
)
select
  operation_date,
  wa_from,
  interactive_id,
  text_body,
  novelty_code,
  employee_documento,
  employee_nombre,
  employee_sede_codigo,
  employee_sede_nombre,
  target_attendance_id,
  dup_count,
  processed_at
from ranked
where rn = 1
order by operation_date desc, processed_at desc nulls last, received_at desc;

-- 3) Insercion de recovery.
-- Descomenta solo despues de validar el preview.
/*
with ranked as (
  select
    *,
    row_number() over (
      partition by target_attendance_id
      order by processed_at desc nulls last, received_at desc, incoming_id desc
    ) as rn
  from tmp_whatsapp_attendance_recovery
  where recovery_status = 'ready_to_insert'
)
insert into public.attendance (
  id,
  fecha,
  empleado_id,
  documento,
  nombre,
  sede_codigo,
  sede_nombre,
  asistio,
  novedad,
  created_at
)
select
  target_attendance_id,
  operation_date::text,
  employee_id,
  employee_documento,
  employee_nombre,
  employee_sede_codigo,
  employee_sede_nombre,
  true,
  novelty_code,
  coalesce(processed_at, received_at, now())
from ranked
where rn = 1
on conflict (id) do nothing;
*/

-- 4) Validacion posterior
with ranked as (
  select
    *,
    row_number() over (
      partition by target_attendance_id
      order by processed_at desc nulls last, received_at desc, incoming_id desc
    ) as rn
  from tmp_whatsapp_attendance_recovery
  where recovery_status = 'ready_to_insert'
)
select
  operation_date,
  employee_documento,
  target_attendance_id
from ranked
where rn = 1
order by operation_date desc, processed_at desc nulls last;
