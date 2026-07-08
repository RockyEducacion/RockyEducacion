-- Phase 19: global daily occupancy for supernumerario replacements.
-- Apply after phase 18.
-- Existing duplicated rows must be reviewed manually; the trigger prevents new duplicates.

create or replace function public.list_supernumerario_replacement_occupancy(p_fecha text)
returns table (
  id text,
  import_id uuid,
  fecha_operacion text,
  fecha text,
  empleado_id uuid,
  documento text,
  nombre text,
  sede_codigo text,
  sede_nombre text,
  novedad_codigo text,
  novedad_nombre text,
  decision text,
  supernumerario_id uuid,
  supernumerario_documento text,
  supernumerario_nombre text,
  ts timestamptz,
  actor_uid uuid,
  actor_email text
)
language sql
stable
security definer
set search_path = public
as $$
  select
    r.id,
    r.import_id,
    r.fecha_operacion,
    r.fecha,
    r.empleado_id,
    r.documento,
    r.nombre,
    r.sede_codigo,
    r.sede_nombre,
    r.novedad_codigo,
    r.novedad_nombre,
    r.decision,
    r.supernumerario_id,
    r.supernumerario_documento,
    r.supernumerario_nombre,
    r.ts,
    r.actor_uid,
    r.actor_email
  from public.import_replacements r
  left join public.employee_daily_status eds
    on eds.fecha = r.fecha
    and (
      (r.empleado_id is not null and eds.employee_id = r.empleado_id::text)
      or (
        nullif(trim(coalesce(r.documento, '')), '') is not null
        and eds.documento = nullif(trim(r.documento), '')
      )
    )
  where r.fecha = nullif(trim(p_fecha), '')
    and r.decision = 'reemplazo'
    and coalesce(eds.servicio_programado, true) = true
    and (
      public.current_profile_is_active_non_supervisor()
      or exists (
        select 1
        from public.profiles p
        where p.id = auth.uid()
          and p.estado = 'activo'
          and p.role::text = 'supervisor'
          and p.supervisor_eligible = true
      )
    )
  order by r.ts desc nulls last, r.supernumerario_nombre asc;
$$;

grant execute on function public.list_supernumerario_replacement_occupancy(text) to authenticated;

create or replace function public.prevent_duplicate_supernumerario_replacement()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  v_super_label text;
  v_target text;
begin
  if coalesce(new.decision, '') <> 'reemplazo' then
    return new;
  end if;

  if new.supernumerario_id is null and nullif(trim(coalesce(new.supernumerario_documento, '')), '') is null then
    return new;
  end if;

  if exists (
    select 1
    from public.employee_daily_status eds
    where eds.fecha = new.fecha
      and coalesce(eds.servicio_programado, false) = false
      and (
        (new.empleado_id is not null and eds.employee_id = new.empleado_id::text)
        or (
          nullif(trim(coalesce(new.documento, '')), '') is not null
          and eds.documento = nullif(trim(new.documento), '')
        )
      )
  ) then
    return new;
  end if;

  select coalesce(r.nombre, r.documento, r.id)
    into v_target
  from public.import_replacements r
  left join public.employee_daily_status eds
    on eds.fecha = r.fecha
    and (
      (r.empleado_id is not null and eds.employee_id = r.empleado_id::text)
      or (
        nullif(trim(coalesce(r.documento, '')), '') is not null
        and eds.documento = nullif(trim(r.documento), '')
      )
    )
  where r.fecha = new.fecha
    and r.id <> new.id
    and coalesce(r.decision, '') = 'reemplazo'
    and coalesce(eds.servicio_programado, true) = true
    and (
      (new.supernumerario_id is not null and r.supernumerario_id = new.supernumerario_id)
      or (
        nullif(trim(coalesce(new.supernumerario_documento, '')), '') is not null
        and nullif(trim(coalesce(r.supernumerario_documento, '')), '') = nullif(trim(new.supernumerario_documento), '')
      )
    )
  limit 1;

  if v_target is not null then
    v_super_label := coalesce(new.supernumerario_nombre, new.supernumerario_documento, new.supernumerario_id::text, 'El supernumerario');
    raise exception '% ya esta ocupado para la fecha % cubriendo a %.', v_super_label, new.fecha, v_target
      using errcode = '23505';
  end if;

  return new;
end;
$$;

drop trigger if exists import_replacements_prevent_duplicate_supernumerario on public.import_replacements;
create trigger import_replacements_prevent_duplicate_supernumerario
before insert or update of fecha, decision, supernumerario_id, supernumerario_documento
on public.import_replacements
for each row
execute function public.prevent_duplicate_supernumerario_replacement();
