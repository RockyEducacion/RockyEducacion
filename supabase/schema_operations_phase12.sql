create or replace function public.recompute_sede_status_from_employee_daily_status(p_fecha text)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows integer := 0;
begin
  if p_fecha is null or p_fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    raise exception 'Fecha invalida para sede_status: %', p_fecha;
  end if;

  delete from public.sede_status where fecha = p_fecha;

  with active_sedes as (
    select s.*
    from public.sedes s
    where lower(trim(coalesce(s.estado, 'activo'))) <> 'inactivo'
      and public.is_sede_scheduled_for_date_sql(s.jornada, p_fecha)
  ),
  contracted_by_sede as (
    select
      eds.sede_codigo,
      count(*)::integer as contratados,
      count(*) filter (where eds.cuenta_pago_servicio = true)::integer as cubiertos
    from public.employee_daily_status eds
    where eds.fecha = p_fecha
      and eds.tipo_personal = 'empleado'
      and eds.servicio_programado = true
    group by eds.sede_codigo
  )
  insert into public.sede_status (
    id,
    fecha,
    sede_codigo,
    sede_nombre,
    operarios_esperados,
    operarios_presentes,
    faltantes
  )
  select
    concat(p_fecha, '_', s.codigo) as id,
    p_fecha,
    s.codigo,
    s.nombre,
    coalesce(c.contratados, 0),
    coalesce(c.cubiertos, 0),
    greatest(coalesce(c.contratados, 0) - coalesce(c.cubiertos, 0), 0)
  from active_sedes s
  left join contracted_by_sede c on c.sede_codigo = s.codigo;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.recompute_daily_metrics_from_employee_daily_status(p_fecha text)
returns public.daily_metrics
language plpgsql
security definer
set search_path = public
as $$
declare
  v_result public.daily_metrics;
begin
  if p_fecha is null or p_fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    raise exception 'Fecha invalida para daily_metrics: %', p_fecha;
  end if;

  with active_sedes as (
    select s.*
    from public.sedes s
    where lower(trim(coalesce(s.estado, 'activo'))) <> 'inactivo'
      and public.is_sede_scheduled_for_date_sql(s.jornada, p_fecha)
  ),
  scheduled_service_rows as (
    select *
    from public.employee_daily_status eds
    where eds.fecha = p_fecha
      and eds.tipo_personal = 'empleado'
      and eds.servicio_programado = true
  ),
  actual_rows as (
    select *
    from public.employee_daily_status eds
    where eds.fecha = p_fecha
      and (eds.asistio = true or eds.asistio = false)
  ),
  closure_state as (
    select dc.*
    from public.daily_closures dc
    where dc.fecha = p_fecha
  ),
  closure_flag as (
    select exists (
      select 1
      from closure_state cs
      where cs.locked = true or lower(trim(coalesce(cs.status, ''))) = 'closed'
    ) as is_closed
  ),
  base_counts as (
    select
      p_fecha as fecha,
      coalesce((select sum(greatest(coalesce(s.numero_operarios, 0), 0))::integer from active_sedes s), 0) as planned,
      coalesce((select count(*)::integer from scheduled_service_rows), 0) as expected,
      coalesce((select count(*)::integer from actual_rows where source_attendance_id is not null), 0) as unique_count,
      (select is_closed from closure_flag) as is_closed
  ),
  metrics as (
    select
      bc.fecha,
      bc.planned,
      bc.expected,
      bc.unique_count,
      case
        when bc.planned = 0 and bc.expected = 0 then coalesce((select count(*)::integer from actual_rows ar where ar.asistio = true), 0)
        else coalesce((select count(*)::integer from scheduled_service_rows sr where sr.cuenta_pago_servicio = true), 0)
      end as attendance_count,
      case
        when bc.planned = 0 and bc.expected = 0 then coalesce((select count(*)::integer from actual_rows ar where ar.asistio = false), 0)
        when bc.is_closed then coalesce((select count(*)::integer from scheduled_service_rows sr where coalesce(sr.cuenta_pago_servicio, false) = false), 0)
        else coalesce((select count(*)::integer from scheduled_service_rows sr where coalesce(sr.decision_cobertura, '') = 'ausentismo' or sr.estado_dia = 'ausente_sin_reemplazo'), 0)
      end as absenteeism,
      coalesce(bc.is_closed, false) as is_closed
    from base_counts bc
  )
  insert into public.daily_metrics (
    id,
    fecha,
    planned,
    expected,
    unique_count,
    missing,
    attendance_count,
    absenteeism,
    paid_services,
    no_contracted,
    closed
  )
  select
    p_fecha,
    m.fecha,
    m.planned,
    m.expected,
    m.unique_count,
    case
      when m.planned = 0 and m.expected = 0 then 0
      when m.is_closed then m.absenteeism
      else greatest(m.expected - m.attendance_count, 0)
    end,
    m.attendance_count,
    m.absenteeism,
    m.attendance_count,
    greatest(m.planned - m.expected, 0),
    m.is_closed
  from metrics m
  on conflict (id) do update
  set
    fecha = excluded.fecha,
    planned = excluded.planned,
    expected = excluded.expected,
    unique_count = excluded.unique_count,
    missing = excluded.missing,
    attendance_count = excluded.attendance_count,
    absenteeism = excluded.absenteeism,
    paid_services = excluded.paid_services,
    no_contracted = excluded.no_contracted,
    closed = excluded.closed,
    updated_at = now()
  returning * into v_result;

  return v_result;
end;
$$;

create or replace function public.refresh_operational_snapshots_from_employee_daily_status(p_fecha text)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_sedes integer := 0;
  v_metrics public.daily_metrics;
begin
  v_sedes := public.recompute_sede_status_from_employee_daily_status(p_fecha);
  v_metrics := public.recompute_daily_metrics_from_employee_daily_status(p_fecha);

  return jsonb_build_object(
    'fecha', p_fecha,
    'sede_status_rows', v_sedes,
    'daily_metrics_id', v_metrics.id,
    'attendance_count', v_metrics.attendance_count,
    'expected', v_metrics.expected,
    'planned', v_metrics.planned
  );
end;
$$;
