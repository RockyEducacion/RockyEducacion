-- Phase 20: active incapacity occupancy for global supernumerarios.
-- Apply after phase 19.

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
  from public.incapacitados i
  join public.employees e
    on (
      (i.employee_id is not null and e.id = i.employee_id)
      or (
        nullif(trim(coalesce(i.documento, '')), '') is not null
        and e.documento = nullif(trim(i.documento), '')
      )
    )
  left join public.cargos c on c.codigo = e.cargo_codigo
  where nullif(trim(p_fecha), '') is not null
    and coalesce(i.estado, 'activo') = 'activo'
    and i.fecha_inicio <= nullif(trim(p_fecha), '')::date
    and i.fecha_fin >= nullif(trim(p_fecha), '')::date
    and coalesce(e.estado, 'activo') <> 'inactivo'
    and (
      lower(coalesce(c.alineacion_crud, '')) = 'supernumerario'
      or lower(coalesce(c.nombre, '')) like '%supernumer%'
      or lower(coalesce(e.cargo_nombre, '')) like '%supernumer%'
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
