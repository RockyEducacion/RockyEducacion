-- Phase 22: date-aware supernumerario availability for supervisor replacements.
-- Apply after phase 18.

create or replace function public.list_supernumerarios_for_current_supervisor(p_fecha text)
returns table (
  id uuid,
  documento text,
  nombre text,
  telefono text,
  estado text,
  sede_codigo text,
  sede_nombre text
)
language sql
stable
security definer
set search_path = public
as $$
  with params as (
    select nullif(trim(p_fecha), '')::date as day
  )
  select
    e.id,
    e.documento,
    e.nombre,
    e.telefono,
    e.estado,
    coalesce(a.sede_codigo, e.sede_codigo) as sede_codigo,
    coalesce(a.sede_nombre, e.sede_nombre) as sede_nombre
  from params
  join public.employees e on params.day is not null
  left join lateral (
    select h.*
    from public.employee_cargo_history h
    where h.employee_id = e.id
      and h.fecha_ingreso::date <= params.day
      and (h.fecha_retiro is null or h.fecha_retiro::date >= params.day)
    order by h.fecha_ingreso desc nulls last, h.created_at desc nulls last, h.id desc
    limit 1
  ) a on true
  left join public.cargos c on c.codigo = coalesce(a.cargo_codigo, e.cargo_codigo)
  where (
      coalesce(e.estado, 'activo') <> 'inactivo'
      or (e.fecha_retiro is not null and e.fecha_retiro::date >= params.day)
    )
    and coalesce(a.fecha_ingreso::date, e.fecha_ingreso::date) <= params.day
    and (
      lower(coalesce(c.alineacion_crud, '')) = 'supernumerario'
      or lower(coalesce(c.nombre, '')) like '%supernumer%'
      or lower(coalesce(a.cargo_nombre, e.cargo_nombre, '')) like '%supernumer%'
    )
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
  order by e.nombre asc;
$$;

grant execute on function public.list_supernumerarios_for_current_supervisor(text) to authenticated;

create or replace function public.list_supernumerario_incapacities_for_current_supervisor(p_fecha text)
returns table (
  id uuid,
  employee_id uuid,
  documento text,
  nombre text,
  fecha_inicio date,
  fecha_fin date,
  estado text,
  source text,
  canal_registro text,
  soporte_url text,
  soporte_nombre text,
  soporte_tipo text,
  soporte_storage_path text,
  whatsapp_message_id text,
  created_at timestamptz,
  updated_at timestamptz
)
language sql
stable
security definer
set search_path = public
as $$
  with params as (
    select nullif(trim(p_fecha), '')::date as day
  )
  select
    i.id,
    i.employee_id,
    i.documento,
    i.nombre,
    i.fecha_inicio,
    i.fecha_fin,
    i.estado,
    i.source,
    i.canal_registro,
    i.soporte_url,
    i.soporte_nombre,
    i.soporte_tipo,
    i.soporte_storage_path,
    i.whatsapp_message_id,
    i.created_at,
    i.updated_at
  from params
  join public.incapacitados i on params.day is not null
  join public.employees e
    on (
      (i.employee_id is not null and e.id = i.employee_id)
      or (
        nullif(trim(coalesce(i.documento, '')), '') is not null
        and e.documento = nullif(trim(i.documento), '')
      )
    )
  left join lateral (
    select h.*
    from public.employee_cargo_history h
    where h.employee_id = e.id
      and h.fecha_ingreso::date <= params.day
      and (h.fecha_retiro is null or h.fecha_retiro::date >= params.day)
    order by h.fecha_ingreso desc nulls last, h.created_at desc nulls last, h.id desc
    limit 1
  ) a on true
  left join public.cargos c on c.codigo = coalesce(a.cargo_codigo, e.cargo_codigo)
  where coalesce(i.estado, 'activo') = 'activo'
    and i.fecha_inicio <= params.day
    and i.fecha_fin >= params.day
    and (
      coalesce(e.estado, 'activo') <> 'inactivo'
      or (e.fecha_retiro is not null and e.fecha_retiro::date >= params.day)
    )
    and coalesce(a.fecha_ingreso::date, e.fecha_ingreso::date) <= params.day
    and (
      lower(coalesce(c.alineacion_crud, '')) = 'supernumerario'
      or lower(coalesce(c.nombre, '')) like '%supernumer%'
      or lower(coalesce(a.cargo_nombre, e.cargo_nombre, '')) like '%supernumer%'
    )
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
  order by i.fecha_inicio desc, i.nombre asc;
$$;

grant execute on function public.list_supernumerario_incapacities_for_current_supervisor(text) to authenticated;
