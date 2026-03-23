-- Playbook para recuperar datos del reporte de nomina cuando attendance quedo
-- incompleto pero existen rastros en otras tablas.
--
-- Uso sugerido:
-- 1) Ajusta from_date y to_date. Para este caso: 2026-03-19 a 2026-03-23.
-- 2) Ejecuta completo y revisa los bloques de diagnostico.
-- 3) Si el preview de recovery es correcto, descomenta el INSERT.
-- 4) Reejecuta las validaciones finales.

drop table if exists tmp_payroll_recovery_params;
drop table if exists tmp_payroll_day_summary;
drop table if exists tmp_payroll_recovery_incoming_base;
drop table if exists tmp_payroll_recovery_final_actions;
drop table if exists tmp_payroll_attendance_recovery;

create temporary table tmp_payroll_recovery_params as
select
  date '2026-03-19' as from_date,
  date '2026-03-23' as to_date;

create temporary table tmp_payroll_day_summary as
with params as (
  select from_date, to_date from tmp_payroll_recovery_params
),
days as (
  select generate_series(from_date, to_date, interval '1 day')::date as operation_date
  from params
),
attendance_by_day as (
  select
    a.fecha::date as operation_date,
    count(*) as attendance_rows,
    count(*) filter (where a.asistio = true) as attendance_present_rows
  from public.attendance a, params p
  where a.fecha::date between p.from_date and p.to_date
  group by a.fecha::date
),
replacement_by_day as (
  select
    ir.fecha::date as operation_date,
    count(*) as replacement_rows,
    count(*) filter (where lower(coalesce(ir.decision, '')) = 'reemplazo') as replacement_effective_rows
  from public.import_replacements ir, params p
  where ir.fecha::date between p.from_date and p.to_date
  group by ir.fecha::date
),
status_by_day as (
  select
    ss.fecha::date as operation_date,
    count(*) as sede_status_rows,
    sum(coalesce(ss.operarios_esperados, 0)) as sede_status_expected,
    sum(coalesce(ss.operarios_presentes, 0)) as sede_status_present,
    sum(coalesce(ss.faltantes, 0)) as sede_status_missing
  from public.sede_status ss, params p
  where ss.fecha::date between p.from_date and p.to_date
  group by ss.fecha::date
),
metrics_by_day as (
  select
    dm.fecha::date as operation_date,
    dm.planned,
    dm.expected,
    dm.unique_count,
    dm.missing,
    dm.attendance_count,
    dm.absenteeism,
    dm.paid_services,
    dm.no_contracted,
    dm.closed
  from public.daily_metrics dm, params p
  where dm.fecha::date between p.from_date and p.to_date
),
closures_by_day as (
  select
    dc.fecha::date as operation_date,
    dc.status,
    dc.locked,
    dc.planeados,
    dc.contratados,
    dc.asistencias,
    dc.ausentismos,
    dc.no_contratados
  from public.daily_closures dc, params p
  where dc.fecha::date between p.from_date and p.to_date
),
imports_by_day as (
  select
    ih.fecha_operacion::date as operation_date,
    count(*) as import_history_rows,
    sum(coalesce(ih.planned_count, 0)) as import_planned,
    sum(coalesce(ih.expected_count, 0)) as import_expected,
    sum(coalesce(ih.found_count, 0)) as import_found,
    sum(coalesce(ih.missing_count, 0)) as import_missing
  from public.import_history ih, params p
  where nullif(ih.fecha_operacion, '') is not null
    and ih.fecha_operacion::date between p.from_date and p.to_date
  group by ih.fecha_operacion::date
)
select
  d.operation_date,
  coalesce(a.attendance_rows, 0) as attendance_rows,
  coalesce(a.attendance_present_rows, 0) as attendance_present_rows,
  coalesce(r.replacement_rows, 0) as replacement_rows,
  coalesce(r.replacement_effective_rows, 0) as replacement_effective_rows,
  coalesce(s.sede_status_rows, 0) as sede_status_rows,
  coalesce(s.sede_status_expected, 0) as sede_status_expected,
  coalesce(s.sede_status_present, 0) as sede_status_present,
  coalesce(s.sede_status_missing, 0) as sede_status_missing,
  m.planned as metrics_planned,
  m.expected as metrics_expected,
  m.unique_count as metrics_unique,
  m.missing as metrics_missing,
  m.attendance_count as metrics_attendance_count,
  m.absenteeism as metrics_absenteeism,
  m.paid_services as metrics_paid_services,
  m.no_contracted as metrics_no_contracted,
  m.closed as metrics_closed,
  c.status as closure_status,
  c.locked as closure_locked,
  c.planeados as closure_planned,
  c.contratados as closure_expected,
  c.asistencias as closure_attendance,
  c.ausentismos as closure_absenteeism,
  c.no_contratados as closure_no_contracted,
  coalesce(i.import_history_rows, 0) as import_history_rows,
  coalesce(i.import_planned, 0) as import_planned,
  coalesce(i.import_expected, 0) as import_expected,
  coalesce(i.import_found, 0) as import_found,
  coalesce(i.import_missing, 0) as import_missing
from days d
left join attendance_by_day a on a.operation_date = d.operation_date
left join replacement_by_day r on r.operation_date = d.operation_date
left join status_by_day s on s.operation_date = d.operation_date
left join metrics_by_day m on m.operation_date = d.operation_date
left join closures_by_day c on c.operation_date = d.operation_date
left join imports_by_day i on i.operation_date = d.operation_date
order by d.operation_date;

-- 1) Diagnostico general por dia.
select *
from tmp_payroll_day_summary
order by operation_date;

create temporary table tmp_payroll_recovery_incoming_base as
with params as (
  select from_date, to_date from tmp_payroll_recovery_params
)
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
from public.whatsapp_incoming wi, params p
where wi.event_type = 'message'
  and (
    (coalesce(wi.wa_timestamp, '') ~ '^\d+$'
      and (timezone('America/Bogota', to_timestamp(wi.wa_timestamp::bigint)))::date between p.from_date and p.to_date)
    or
    (not (coalesce(wi.wa_timestamp, '') ~ '^\d+$')
      and (timezone('America/Bogota', coalesce(wi.processed_at, wi.received_at)))::date between p.from_date and p.to_date)
  );

create index tmp_payroll_recovery_incoming_base_phone_date_ts_idx
  on tmp_payroll_recovery_incoming_base (wa_from_digits, operation_date, event_ts desc);

create temporary table tmp_payroll_recovery_final_actions as
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
from tmp_payroll_recovery_incoming_base ib
where
  lower(coalesce(ib.interactive_id, '')) in (
    'action_working',
    'action_compensatory',
    'daily_trabajando',
    'daily_compensatorio'
  )
  or lower(coalesce(ib.text_body, '')) in ('trabajando', 'compensatorio');

create index tmp_payroll_recovery_final_actions_phone_date_ts_idx
  on tmp_payroll_recovery_final_actions (wa_from_digits, operation_date, event_ts desc);

create temporary table tmp_payroll_attendance_recovery as
with inferred_docs as (
  select
    fa.id as final_id,
    prev_doc.documento as inferred_documento
  from tmp_payroll_recovery_final_actions fa
  left join lateral (
    select regexp_replace(coalesce(prev.text_body, ''), '\D', '', 'g') as documento
    from tmp_payroll_recovery_incoming_base prev
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
  from tmp_payroll_recovery_final_actions fa
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

-- 2) Resumen de candidatos de recovery desde WhatsApp.
select
  operation_date,
  recovery_status,
  count(*) as total
from tmp_payroll_attendance_recovery
group by operation_date, recovery_status
order by operation_date, recovery_status;

-- 3) Preview de filas que pueden insertarse en attendance.
with ranked as (
  select
    *,
    row_number() over (
      partition by target_attendance_id
      order by processed_at desc nulls last, received_at desc, incoming_id desc
    ) as rn,
    count(*) over (partition by target_attendance_id) as dup_count
  from tmp_payroll_attendance_recovery
  where recovery_status = 'ready_to_insert'
)
select
  operation_date,
  employee_documento,
  employee_nombre,
  employee_sede_codigo,
  employee_sede_nombre,
  novelty_code,
  target_attendance_id,
  dup_count,
  processed_at
from ranked
where rn = 1
order by operation_date, employee_nombre, employee_documento;

-- 4) Insercion opcional.
-- Descomenta solo despues de validar el preview anterior.
/*
with ranked as (
  select
    *,
    row_number() over (
      partition by target_attendance_id
      order by processed_at desc nulls last, received_at desc, incoming_id desc
    ) as rn
  from tmp_payroll_attendance_recovery
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

-- 5) Validacion posterior a recovery.
with params as (
  select from_date, to_date from tmp_payroll_recovery_params
),
attendance_post as (
  select
    a.fecha::date as operation_date,
    count(*) as attendance_rows,
    count(*) filter (where a.asistio = true) as attendance_present_rows
  from public.attendance a, params p
  where a.fecha::date between p.from_date and p.to_date
  group by a.fecha::date
)
select
  s.operation_date,
  s.attendance_present_rows as attendance_before,
  coalesce(ap.attendance_present_rows, 0) as attendance_after,
  s.metrics_attendance_count,
  s.closure_attendance,
  s.metrics_paid_services,
  s.replacement_effective_rows
from tmp_payroll_day_summary s
left join attendance_post ap on ap.operation_date = s.operation_date
order by s.operation_date;

