create or replace function public.bool_from_text_nullable(value text)
returns boolean
language sql
immutable
as $$
  select case
    when value is null then null
    when lower(trim(value)) in ('si', 'yes', 'true', '1', 'paga', 'pago', 'remunerado', 'liquida') then true
    when lower(trim(value)) in ('no', 'false', '0', 'no_paga', 'nopaga', 'no pago', 'sin pago', 'no remunerado') then false
    else null
  end;
$$;

create or replace function public.bool_from_text_truthy(value text)
returns boolean
language sql
immutable
as $$
  select coalesce(public.bool_from_text_nullable(value), false);
$$;

create or replace function public.easter_sunday_sql(p_year integer)
returns date
language plpgsql
immutable
as $$
declare
  a integer;
  b integer;
  c integer;
  d integer;
  e integer;
  f integer;
  g integer;
  h integer;
  i integer;
  k integer;
  l integer;
  m integer;
  v_month integer;
  v_day integer;
begin
  a := p_year % 19;
  b := floor(p_year / 100);
  c := p_year % 100;
  d := floor(b / 4);
  e := b % 4;
  f := floor((b + 8) / 25);
  g := floor((b - f + 1) / 3);
  h := (19 * a + b - d - g + 15) % 30;
  i := floor(c / 4);
  k := c % 4;
  l := (32 + 2 * e + 2 * i - h - k) % 7;
  m := floor((a + 11 * h + 22 * l) / 451);
  v_month := floor((h + l - 7 * m + 114) / 31);
  v_day := ((h + l - 7 * m + 114) % 31) + 1;
  return make_date(p_year, v_month, v_day);
end;
$$;

create or replace function public.move_to_following_monday_sql(p_fecha date)
returns date
language sql
immutable
as $$
  select case
    when p_fecha is null then null
    when extract(isodow from p_fecha)::integer = 1 then p_fecha
    else p_fecha + (8 - extract(isodow from p_fecha)::integer)
  end;
$$;

create or replace function public.is_colombia_holiday_sql(fecha text)
returns boolean
language plpgsql
immutable
as $$
declare
  v_fecha date;
  v_year integer;
  v_easter date;
begin
  if fecha is null or fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    return false;
  end if;

  v_fecha := fecha::date;
  v_year := extract(year from v_fecha);
  v_easter := public.easter_sunday_sql(v_year);

  return v_fecha in (
    make_date(v_year, 1, 1),
    make_date(v_year, 5, 1),
    make_date(v_year, 7, 20),
    make_date(v_year, 8, 7),
    make_date(v_year, 12, 8),
    make_date(v_year, 12, 25),
    public.move_to_following_monday_sql(make_date(v_year, 1, 6)),
    public.move_to_following_monday_sql(make_date(v_year, 3, 19)),
    public.move_to_following_monday_sql(make_date(v_year, 6, 29)),
    public.move_to_following_monday_sql(make_date(v_year, 8, 15)),
    public.move_to_following_monday_sql(make_date(v_year, 10, 12)),
    public.move_to_following_monday_sql(make_date(v_year, 11, 1)),
    public.move_to_following_monday_sql(make_date(v_year, 11, 11)),
    v_easter - 3,
    v_easter - 2,
    public.move_to_following_monday_sql(v_easter + 39),
    public.move_to_following_monday_sql(v_easter + 60),
    public.move_to_following_monday_sql(v_easter + 68)
  );
end;
$$;

create or replace function public.is_sede_scheduled_for_date_sql(jornada text, fecha text)
returns boolean
language plpgsql
immutable
as $$
declare
  v_fecha date;
  v_weekday integer;
  v_jornada text;
begin
  if fecha is null or fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    return false;
  end if;
  v_fecha := fecha::date;
  v_weekday := extract(dow from v_fecha);
  v_jornada := lower(trim(coalesce(jornada, 'lun_vie')));

  if v_jornada = 'lun_dom' then
    return true;
  end if;

  if public.is_colombia_holiday_sql(fecha) then
    return false;
  end if;

  if v_jornada = 'lun_sab' then
    return v_weekday between 1 and 6;
  end if;

  return v_weekday between 1 and 5;
end;
$$;

create or replace function public.is_employee_effective_for_date_sql(
  estado text,
  fecha_ingreso timestamptz,
  fecha_retiro timestamptz,
  fecha text
)
returns boolean
language plpgsql
immutable
as $$
declare
  v_fecha date;
  v_ingreso date;
  v_retiro date;
  v_estado text;
begin
  if fecha is null or fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    return false;
  end if;

  v_fecha := fecha::date;
  v_ingreso := fecha_ingreso::date;
  v_retiro := fecha_retiro::date;
  v_estado := lower(trim(coalesce(estado, 'activo')));

  if v_ingreso is null or v_ingreso > v_fecha then
    return false;
  end if;

  if v_retiro is not null and v_retiro < v_fecha then
    return false;
  end if;

  if v_estado = 'inactivo' then
    return v_retiro is not null and v_retiro >= v_fecha;
  end if;

  return v_estado <> 'eliminado';
end;
$$;

create or replace function public.refresh_employee_daily_status(p_fecha text)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_rows integer := 0;
begin
  if p_fecha is null or p_fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    raise exception 'Fecha invalida para employee_daily_status: %', p_fecha;
  end if;

  delete from public.employee_daily_status where fecha = p_fecha;

  with active_sedes as (
    select s.*
    from public.sedes s
    where lower(trim(coalesce(s.estado, 'activo'))) <> 'inactivo'
      and public.is_sede_scheduled_for_date_sql(s.jornada, p_fecha)
  ),
  sedes_lookup as (
    select s.*
    from public.sedes s
  ),
  cargos_lookup as (
    select
      c.codigo,
      c.nombre,
      lower(trim(coalesce(c.alineacion_crud, 'empleado'))) as alineacion_crud
    from public.cargos c
  ),
  employees_catalog as (
    select
      e.id::text as employee_id,
      e.id as employee_uuid,
      e.codigo,
      e.documento,
      e.nombre,
      e.cargo_codigo,
      e.cargo_nombre,
      e.sede_codigo as home_sede_codigo,
      e.sede_nombre as home_sede_nombre,
      e.zona_codigo as home_zona_codigo,
      e.zona_nombre as home_zona_nombre,
      e.fecha_ingreso,
      e.fecha_retiro,
      lower(trim(coalesce(e.estado, 'activo'))) as estado_empleado,
      case
        when coalesce(cl.alineacion_crud, '') = 'supernumerario' then 'supernumerario'
        when lower(coalesce(e.cargo_nombre, '')) like '%supernumerar%' then 'supernumerario'
        else 'empleado'
      end as tipo_personal
    from public.employees e
    left join cargos_lookup cl on cl.codigo = e.cargo_codigo
  ),
  expected_base as (
    select
      ec.employee_id,
      ec.employee_uuid,
      ec.documento,
      ec.nombre,
      ec.cargo_codigo,
      ec.cargo_nombre,
      ec.tipo_personal,
      s.codigo as sede_codigo,
      coalesce(s.nombre, ec.home_sede_nombre) as sede_nombre_snapshot,
      s.zona_codigo as zona_codigo_snapshot,
      s.zona_nombre as zona_nombre_snapshot,
      s.dependencia_codigo as dependencia_codigo_snapshot,
      s.dependencia_nombre as dependencia_nombre_snapshot,
      true as servicio_programado
    from employees_catalog ec
    join active_sedes s on s.codigo = ec.home_sede_codigo
    where ec.tipo_personal = 'empleado'
      and public.is_employee_effective_for_date_sql(ec.estado_empleado, ec.fecha_ingreso, ec.fecha_retiro, p_fecha)
  ),
  attendance_day as (
    select *
    from (
      select
        a.*,
        row_number() over (
          partition by coalesce(a.empleado_id::text, a.documento, a.id)
          order by a.created_at desc nulls last, a.id desc
        ) as rn
      from public.attendance a
      where a.fecha = p_fecha
    ) x
    where x.rn = 1
  ),
  replacements_by_employee as (
    select *
    from (
      select
        r.*,
        row_number() over (
          partition by coalesce(r.empleado_id::text, r.documento, r.id)
          order by r.ts desc nulls last, r.id desc
        ) as rn
      from public.import_replacements r
      where r.fecha = p_fecha
    ) x
    where x.rn = 1
  ),
  replacements_by_supernumerario as (
    select *
    from (
      select
        r.*,
        row_number() over (
          partition by coalesce(r.supernumerario_id::text, r.supernumerario_documento, r.id)
          order by r.ts desc nulls last, r.id desc
        ) as rn
      from public.import_replacements r
      where r.fecha = p_fecha
        and coalesce(r.supernumerario_id::text, r.supernumerario_documento, '') <> ''
    ) x
    where x.rn = 1
  ),
  absenteeism_day as (
    select *
    from (
      select
        ab.*,
        row_number() over (
          partition by coalesce(ab.empleado_id::text, ab.documento, ab.id)
          order by ab.created_at desc nulls last, ab.id desc
        ) as rn
      from public.absenteeism ab
      where ab.fecha = p_fecha
    ) x
    where x.rn = 1
  ),
  incapacity_day as (
    select *
    from (
      select
        i.*,
        row_number() over (
          partition by coalesce(i.employee_id::text, i.documento, i.id::text)
          order by i.updated_at desc nulls last, i.created_at desc nulls last, i.id desc
        ) as rn
      from public.incapacitados i
      where lower(trim(coalesce(i.estado, 'activo'))) = 'activo'
        and p_fecha::date between i.fecha_inicio and i.fecha_fin
    ) x
    where x.rn = 1
  ),
  employee_activity_scope as (
    select distinct
      ec.employee_id,
      ec.employee_uuid,
      ec.documento,
      ec.nombre,
      ec.cargo_codigo,
      ec.cargo_nombre,
      ec.tipo_personal,
      ec.home_sede_codigo as sede_codigo,
      coalesce(sl.nombre, ec.home_sede_nombre) as sede_nombre_snapshot,
      sl.zona_codigo as zona_codigo_snapshot,
      sl.zona_nombre as zona_nombre_snapshot,
      sl.dependencia_codigo as dependencia_codigo_snapshot,
      sl.dependencia_nombre as dependencia_nombre_snapshot,
      false as servicio_programado
    from employees_catalog ec
    left join sedes_lookup sl on sl.codigo = ec.home_sede_codigo
    where ec.tipo_personal = 'empleado'
      and not exists (
        select 1 from expected_base eb where eb.employee_id = ec.employee_id
      )
      and (
        exists (
          select 1
          from attendance_day a
          where a.empleado_id::text = ec.employee_id
             or (a.empleado_id is null and a.documento = ec.documento)
        )
        or exists (
          select 1
          from replacements_by_employee r
          where r.empleado_id::text = ec.employee_id
             or (r.empleado_id is null and r.documento = ec.documento)
        )
        or exists (
          select 1
          from incapacity_day i
          where i.employee_id::text = ec.employee_id
             or (i.employee_id is null and i.documento = ec.documento)
        )
      )
  ),
  supernumerario_scope as (
    select distinct
      ec.employee_id,
      ec.employee_uuid,
      ec.documento,
      ec.nombre,
      ec.cargo_codigo,
      ec.cargo_nombre,
      ec.tipo_personal,
      ec.home_sede_codigo as sede_codigo,
      coalesce(sl.nombre, ec.home_sede_nombre) as sede_nombre_snapshot,
      sl.zona_codigo as zona_codigo_snapshot,
      sl.zona_nombre as zona_nombre_snapshot,
      sl.dependencia_codigo as dependencia_codigo_snapshot,
      sl.dependencia_nombre as dependencia_nombre_snapshot,
      false as servicio_programado
    from employees_catalog ec
    left join sedes_lookup sl on sl.codigo = ec.home_sede_codigo
    where ec.tipo_personal = 'supernumerario'
      and (
        exists (
          select 1
          from attendance_day a
          where a.empleado_id::text = ec.employee_id
             or (a.empleado_id is null and a.documento = ec.documento)
        )
        or exists (
          select 1
          from replacements_by_supernumerario r
          where r.supernumerario_id::text = ec.employee_id
             or (r.supernumerario_id is null and r.supernumerario_documento = ec.documento)
        )
        or exists (
          select 1
          from incapacity_day i
          where i.employee_id::text = ec.employee_id
             or (i.employee_id is null and i.documento = ec.documento)
        )
      )
  ),
  people_day as (
    select * from expected_base
    union all
    select * from employee_activity_scope
    union all
    select * from supernumerario_scope
  ),
  resolved_rows as (
    select
      pd.employee_id,
      pd.documento,
      pd.nombre,
      pd.cargo_codigo,
      pd.cargo_nombre,
      pd.tipo_personal,
      coalesce(rep_sup.sede_codigo, rep_emp.sede_codigo, att.sede_codigo, pd.sede_codigo) as effective_sede_codigo,
      coalesce(rep_sup.sede_nombre, rep_emp.sede_nombre, att.sede_nombre, pd.sede_nombre_snapshot) as effective_sede_nombre,
      pd.zona_codigo_snapshot as default_zona_codigo_snapshot,
      pd.zona_nombre_snapshot as default_zona_nombre_snapshot,
      pd.dependencia_codigo_snapshot as default_dependencia_codigo_snapshot,
      pd.dependencia_nombre_snapshot as default_dependencia_nombre_snapshot,
      pd.servicio_programado,
      att.id as attendance_id,
      att.asistio,
      att.novedad as attendance_novedad,
      rep_emp.id as replacement_employee_id,
      rep_emp.novedad_codigo as replacement_novedad_codigo,
      rep_emp.novedad_nombre as replacement_novedad_nombre,
      lower(trim(coalesce(rep_emp.decision, ''))) as replacement_employee_decision,
      rep_emp.supernumerario_id::text as reemplazado_por_employee_id,
      rep_emp.supernumerario_documento as reemplazado_por_documento,
      rep_emp.supernumerario_nombre as reemplazado_por_nombre,
      rep_sup.id as replacement_super_id,
      rep_sup.empleado_id::text as reemplaza_a_employee_id,
      rep_sup.documento as reemplaza_a_documento,
      rep_sup.nombre as reemplaza_a_nombre,
      absd.id as absenteeism_id,
      incap.id::text as incapacity_id,
      incap.source as incapacity_source,
      dc.locked as day_locked,
      dc.status as day_status,
      nav_att.codigo as nav_att_codigo,
      nav_att.codigo_novedad as nav_att_codigo_novedad,
      nav_att.nombre as nav_att_nombre,
      nav_att.reemplazo as nav_att_reemplazo,
      nav_att.nomina as nav_att_nomina,
      nav_rep.codigo as nav_rep_codigo,
      nav_rep.codigo_novedad as nav_rep_codigo_novedad,
      nav_rep.nombre as nav_rep_nombre,
      nav_rep.reemplazo as nav_rep_reemplazo,
      nav_rep.nomina as nav_rep_nomina,
      nav_incap.codigo as nav_incap_codigo,
      nav_incap.codigo_novedad as nav_incap_codigo_novedad,
      nav_incap.nombre as nav_incap_nombre,
      nav_incap.reemplazo as nav_incap_reemplazo,
      nav_incap.nomina as nav_incap_nomina,
      rep_emp.actor_email as replacement_employee_actor_email,
      rep_sup.actor_email as replacement_super_actor_email
    from people_day pd
    left join attendance_day att
      on att.empleado_id::text = pd.employee_id
      or (att.empleado_id is null and att.documento = pd.documento)
    left join replacements_by_employee rep_emp
      on rep_emp.empleado_id::text = pd.employee_id
      or (rep_emp.empleado_id is null and rep_emp.documento = pd.documento)
    left join replacements_by_supernumerario rep_sup
      on rep_sup.supernumerario_id::text = pd.employee_id
      or (rep_sup.supernumerario_id is null and rep_sup.supernumerario_documento = pd.documento)
    left join absenteeism_day absd
      on absd.empleado_id::text = pd.employee_id
      or (absd.empleado_id is null and absd.documento = pd.documento)
    left join incapacity_day incap
      on incap.employee_id::text = pd.employee_id
      or (incap.employee_id is null and incap.documento = pd.documento)
    left join public.daily_closures dc
      on dc.fecha = p_fecha
    left join lateral (
      select n.*
      from public.novedades n
      where (
        att.novedad ~ '^\d+$' and trim(coalesce(n.codigo_novedad, n.codigo, '')) = trim(att.novedad)
      ) or (
        lower(trim(coalesce(n.nombre, ''))) = lower(trim(coalesce(att.novedad, '')))
      )
      order by case when att.novedad ~ '^\d+$' and trim(coalesce(n.codigo_novedad, n.codigo, '')) = trim(att.novedad) then 0 else 1 end
      limit 1
    ) nav_att on true
    left join lateral (
      select n.*
      from public.novedades n
      where (
        trim(coalesce(n.codigo_novedad, n.codigo, '')) = trim(coalesce(rep_emp.novedad_codigo, ''))
      ) or (
        lower(trim(coalesce(n.nombre, ''))) = lower(trim(coalesce(rep_emp.novedad_nombre, '')))
      )
      order by case when trim(coalesce(n.codigo_novedad, n.codigo, '')) = trim(coalesce(rep_emp.novedad_codigo, '')) then 0 else 1 end
      limit 1
    ) nav_rep on true
    left join lateral (
      select n.*
      from public.novedades n
      where lower(trim(coalesce(n.nombre, ''))) = lower(trim(coalesce(incap.source, '')))
      limit 1
    ) nav_incap on true
  )
  insert into public.employee_daily_status (
    id,
    fecha,
    employee_id,
    documento,
    nombre,
    tipo_personal,
    sede_codigo,
    sede_nombre_snapshot,
    zona_codigo_snapshot,
    zona_nombre_snapshot,
    dependencia_codigo_snapshot,
    dependencia_nombre_snapshot,
    estado_dia,
    asistio,
    novedad_codigo,
    novedad_nombre,
    requiere_reemplazo,
    decision_cobertura,
    reemplaza_a_employee_id,
    reemplaza_a_documento,
    reemplaza_a_nombre,
    reemplazado_por_employee_id,
    reemplazado_por_documento,
    reemplazado_por_nombre,
    servicio_programado,
    servicio_cubierto,
    cuenta_pago_servicio,
    cuenta_nomina,
    paga_nomina,
    motivo_nomina,
    source_attendance_id,
    source_replacement_id,
    source_absenteeism_id,
    source_incapacity_id,
    origen,
    closed
  )
  select
    concat(p_fecha, '_', rr.employee_id) as id,
    p_fecha,
    rr.employee_id,
    rr.documento,
    rr.nombre,
    rr.tipo_personal,
    rr.effective_sede_codigo,
    coalesce(sl.nombre, rr.effective_sede_nombre),
    coalesce(sl.zona_codigo, rr.default_zona_codigo_snapshot),
    coalesce(sl.zona_nombre, rr.default_zona_nombre_snapshot),
    coalesce(sl.dependencia_codigo, rr.default_dependencia_codigo_snapshot),
    coalesce(sl.dependencia_nombre, rr.default_dependencia_nombre_snapshot),
    case
      when rr.replacement_super_id is not null then 'trabajado_reemplazo'
      when coalesce(rr.nav_att_codigo_novedad, rr.nav_att_codigo, '') = '9'
        or coalesce(rr.nav_rep_codigo_novedad, rr.nav_rep_codigo, '') = '9'
        or lower(trim(coalesce(rr.incapacity_source, ''))) = 'vacaciones' then 'vacaciones'
      when coalesce(rr.nav_att_codigo_novedad, rr.nav_att_codigo, '') = '7' then 'compensatorio'
      when coalesce(rr.asistio, false) = true then 'trabajado'
      when rr.incapacity_id is not null then 'incapacidad'
      when rr.replacement_employee_decision = 'ausentismo' or rr.absenteeism_id is not null then 'ausente_sin_reemplazo'
      when public.bool_from_text_truthy(coalesce(rr.nav_rep_reemplazo, rr.nav_att_reemplazo, rr.nav_incap_reemplazo)) then 'ausente_con_novedad'
      when rr.tipo_personal = 'empleado'
        and rr.servicio_programado
        and (
          coalesce(rr.day_locked, false)
          or lower(trim(coalesce(rr.day_status, ''))) = 'closed'
        ) then 'ausente_sin_reemplazo'
      when rr.servicio_programado then 'sin_registro'
      else 'no_programado'
    end as estado_dia,
    coalesce(rr.asistio, false),
    case
      when nullif(coalesce(rr.nav_rep_codigo_novedad, rr.nav_rep_codigo, rr.nav_att_codigo_novedad, rr.nav_att_codigo, rr.nav_incap_codigo_novedad, rr.nav_incap_codigo), '') is not null
        then coalesce(rr.nav_rep_codigo_novedad, rr.nav_rep_codigo, rr.nav_att_codigo_novedad, rr.nav_att_codigo, rr.nav_incap_codigo_novedad, rr.nav_incap_codigo)
      when rr.tipo_personal = 'empleado'
        and rr.servicio_programado
        and rr.incapacity_id is null
        and coalesce(rr.asistio, false) = false
        and (
          rr.replacement_employee_decision = 'ausentismo'
          or rr.absenteeism_id is not null
          or coalesce(rr.day_locked, false)
          or lower(trim(coalesce(rr.day_status, ''))) = 'closed'
        ) then '8'
      else null
    end,
    case
      when nullif(coalesce(rr.nav_rep_nombre, rr.nav_att_nombre, rr.nav_incap_nombre, rr.incapacity_source), '') is not null
        then coalesce(rr.nav_rep_nombre, rr.nav_att_nombre, rr.nav_incap_nombre, rr.incapacity_source)
      when rr.tipo_personal = 'empleado'
        and rr.servicio_programado
        and rr.incapacity_id is null
        and coalesce(rr.asistio, false) = false
        and (
          rr.replacement_employee_decision = 'ausentismo'
          or rr.absenteeism_id is not null
          or coalesce(rr.day_locked, false)
          or lower(trim(coalesce(rr.day_status, ''))) = 'closed'
        ) then 'AUSENCIA NO JUSTIFICADA'
      else null
    end,
    public.bool_from_text_truthy(coalesce(rr.nav_rep_reemplazo, rr.nav_att_reemplazo, rr.nav_incap_reemplazo)),
    case
      when rr.replacement_super_id is not null then 'reemplazo'
      when rr.replacement_employee_decision in ('reemplazo', 'ausentismo') then rr.replacement_employee_decision
      when rr.absenteeism_id is not null then 'ausentismo'
      when rr.tipo_personal = 'empleado'
        and rr.servicio_programado
        and coalesce(rr.asistio, false) = false
        and (
          coalesce(rr.day_locked, false)
          or lower(trim(coalesce(rr.day_status, ''))) = 'closed'
        ) then 'ausentismo'
      when public.bool_from_text_truthy(coalesce(rr.nav_rep_reemplazo, rr.nav_att_reemplazo, rr.nav_incap_reemplazo)) then 'pendiente'
      else 'no_aplica'
    end as decision_cobertura,
    rr.reemplaza_a_employee_id,
    rr.reemplaza_a_documento,
    rr.reemplaza_a_nombre,
    rr.reemplazado_por_employee_id,
    rr.reemplazado_por_documento,
    rr.reemplazado_por_nombre,
    rr.servicio_programado,
    case
      when rr.tipo_personal <> 'empleado' then false
      when rr.replacement_employee_decision = 'reemplazo' then true
      when coalesce(rr.asistio, false) = true then true
      else false
    end as servicio_cubierto,
    case
      when rr.tipo_personal <> 'empleado' then false
      when rr.replacement_employee_decision = 'reemplazo' then true
      when coalesce(rr.asistio, false) = true then true
      else false
    end as cuenta_pago_servicio,
    true as cuenta_nomina,
    case
      when rr.replacement_super_id is not null then true
      when coalesce(rr.asistio, false) = true then coalesce(public.bool_from_text_nullable(rr.nav_att_nomina), true)
      when rr.incapacity_id is not null then public.bool_from_text_nullable(coalesce(rr.nav_incap_nomina, rr.nav_att_nomina, rr.nav_rep_nomina))
      when rr.attendance_id is not null or rr.replacement_employee_id is not null or rr.absenteeism_id is not null then public.bool_from_text_nullable(coalesce(rr.nav_rep_nomina, rr.nav_att_nomina, rr.nav_incap_nomina))
      else null
    end as paga_nomina,
    case
      when rr.replacement_super_id is not null then 'Supernumerario en reemplazo.'
      when coalesce(rr.asistio, false) = true then 'Prestacion del servicio registrada.'
      when rr.incapacity_id is not null then concat('Incapacidad o novedad prolongada: ', coalesce(rr.nav_incap_nombre, rr.incapacity_source, 'Sin detalle'))
      when rr.replacement_employee_decision = 'reemplazo' then 'Ausencia cubierta con reemplazo.'
      when rr.replacement_employee_decision = 'ausentismo' then 'Ausencia sin reemplazo confirmada.'
      when public.bool_from_text_truthy(coalesce(rr.nav_rep_reemplazo, rr.nav_att_reemplazo, rr.nav_incap_reemplazo)) then 'Pendiente por definir reemplazo o ausentismo.'
      when rr.servicio_programado then 'Sin registro diario del empleado programado.'
      else 'Registro fuera de la programacion base de servicio.'
    end as motivo_nomina,
    rr.attendance_id,
    coalesce(rr.replacement_super_id, rr.replacement_employee_id),
    rr.absenteeism_id,
    rr.incapacity_id,
    case
      when coalesce(rr.replacement_super_actor_email, rr.replacement_employee_actor_email, '') = 'cron@system' then 'cierre'
      when rr.replacement_super_id is not null or rr.replacement_employee_id is not null then 'manual'
      when rr.incapacity_id is not null and rr.attendance_id is null then 'propagacion_incapacidad'
      when rr.attendance_id is not null then 'whatsapp'
      else 'manual'
    end as origen,
    coalesce(rr.day_locked, false) or lower(trim(coalesce(rr.day_status, ''))) = 'closed' as closed
  from resolved_rows rr
  left join sedes_lookup sl on sl.codigo = rr.effective_sede_codigo;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.refresh_employee_daily_status_range(p_fecha_desde text, p_fecha_hasta text)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_from date;
  v_to date;
  v_current date;
  v_total integer := 0;
begin
  if p_fecha_desde is null or p_fecha_hasta is null then
    raise exception 'Debes enviar un rango de fechas valido.';
  end if;

  v_from := p_fecha_desde::date;
  v_to := p_fecha_hasta::date;

  if v_from > v_to then
    raise exception 'La fecha inicial no puede ser mayor que la fecha final.';
  end if;

  v_current := v_from;
  while v_current <= v_to loop
    v_total := v_total + public.refresh_employee_daily_status(v_current::text);
    v_current := v_current + 1;
  end loop;

  return v_total;
end;
$$;
