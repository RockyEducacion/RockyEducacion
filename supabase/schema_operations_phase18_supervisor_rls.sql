-- Phase 18: zone-scoped read access for supervisors.
-- Apply after phases 1, 2, 3, 9, 10, 15, 16 and whatsapp phase 4.

create or replace function public.current_profile_is_active_non_supervisor()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.profiles p
    where p.id = auth.uid()
      and p.estado = 'activo'
      and p.role::text <> 'supervisor'
  );
$$;

create or replace function public.current_supervisor_can_read_zone(zone_code text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.profiles p
    where p.id = auth.uid()
      and p.estado = 'activo'
      and p.role::text = 'supervisor'
      and p.supervisor_eligible = true
      and nullif(trim(zone_code), '') is not null
      and (
        nullif(trim(zone_code), '') = nullif(trim(p.zona_codigo), '')
        or nullif(trim(zone_code), '') = any(coalesce(p.zonas_permitidas, '{}'::text[]))
      )
  );
$$;

create or replace function public.can_read_zone_data(zone_code text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.current_profile_is_active_non_supervisor()
    or public.current_supervisor_can_read_zone(zone_code);
$$;

create or replace function public.can_read_sede_data(sede_code text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.current_profile_is_active_non_supervisor()
    or exists (
      select 1
      from public.sedes s
      where s.codigo = nullif(trim(sede_code), '')
        and public.current_supervisor_can_read_zone(s.zona_codigo)
    );
$$;

create or replace function public.can_read_employee_data(employee_id_value uuid, documento_value text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.current_profile_is_active_non_supervisor()
    or exists (
      select 1
      from public.employees e
      where (
          (employee_id_value is not null and e.id = employee_id_value)
          or (
            nullif(trim(documento_value), '') is not null
            and e.documento = nullif(trim(documento_value), '')
          )
        )
        and (
          public.current_supervisor_can_read_zone(e.zona_codigo)
          or public.can_read_sede_data(e.sede_codigo)
        )
    );
$$;

create or replace function public.can_read_operational_sede_or_employee(
  sede_code text,
  employee_id_value uuid,
  documento_value text
)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.can_read_sede_data(sede_code)
    or public.can_read_employee_data(employee_id_value, documento_value);
$$;

create or replace function public.current_supervisor_can_write_operational_replacement(
  sede_code text,
  employee_id_value uuid,
  documento_value text
)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.profiles p
    where p.id = auth.uid()
      and p.estado = 'activo'
      and p.role::text = 'supervisor'
      and p.supervisor_eligible = true
  )
  and public.can_read_operational_sede_or_employee(sede_code, employee_id_value, documento_value);
$$;

create or replace function public.can_view_qr_registry()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.profiles p
    where p.id = auth.uid()
      and p.estado = 'activo'
      and (
        p.role::text in ('superadmin', 'admin', 'editor')
        or (p.role::text = 'supervisor' and p.supervisor_eligible = true)
      )
  );
$$;

drop function if exists public.list_supernumerarios_for_current_supervisor();

create or replace function public.list_supernumerarios_for_current_supervisor()
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
  select
    e.id,
    e.documento,
    e.nombre,
    e.telefono,
    e.estado,
    e.sede_codigo,
    e.sede_nombre
  from public.employees e
  left join public.cargos c on c.codigo = e.cargo_codigo
  where coalesce(e.estado, 'activo') <> 'inactivo'
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
  order by e.nombre asc;
$$;

grant execute on function public.current_profile_is_active_non_supervisor() to authenticated;
grant execute on function public.current_supervisor_can_read_zone(text) to authenticated;
grant execute on function public.can_read_zone_data(text) to authenticated;
grant execute on function public.can_read_sede_data(text) to authenticated;
grant execute on function public.can_read_employee_data(uuid, text) to authenticated;
grant execute on function public.can_read_operational_sede_or_employee(text, uuid, text) to authenticated;
grant execute on function public.current_supervisor_can_write_operational_replacement(text, uuid, text) to authenticated;
grant execute on function public.can_view_qr_registry() to authenticated;
grant execute on function public.list_supernumerarios_for_current_supervisor() to authenticated;

drop policy if exists "zones_read_authenticated" on public.zones;
create policy "zones_read_authenticated"
on public.zones
for select
to authenticated
using (public.can_read_zone_data(codigo));

drop policy if exists "sedes_read_authenticated" on public.sedes;
create policy "sedes_read_authenticated"
on public.sedes
for select
to authenticated
using (public.can_read_zone_data(zona_codigo));

drop policy if exists "employees_read_authenticated" on public.employees;
create policy "employees_read_authenticated"
on public.employees
for select
to authenticated
using (
  public.can_read_zone_data(zona_codigo)
  or public.can_read_sede_data(sede_codigo)
);

drop policy if exists "employee_cargo_history_read_authenticated" on public.employee_cargo_history;
create policy "employee_cargo_history_read_authenticated"
on public.employee_cargo_history
for select
to authenticated
using (public.can_read_employee_data(employee_id, documento));

drop policy if exists "supervisor_profile_read_authenticated" on public.supervisor_profile;
create policy "supervisor_profile_read_authenticated"
on public.supervisor_profile
for select
to authenticated
using (
  public.can_read_zone_data(zona_codigo)
  or public.can_read_sede_data(sede_codigo)
);

drop policy if exists "attendance_read_authenticated" on public.attendance;
create policy "attendance_read_authenticated"
on public.attendance
for select
to authenticated
using (public.can_read_operational_sede_or_employee(sede_codigo, empleado_id, documento));

drop policy if exists "absenteeism_read_authenticated" on public.absenteeism;
create policy "absenteeism_read_authenticated"
on public.absenteeism
for select
to authenticated
using (public.can_read_operational_sede_or_employee(sede_codigo, empleado_id, documento));

drop policy if exists "sede_status_read_authenticated" on public.sede_status;
create policy "sede_status_read_authenticated"
on public.sede_status
for select
to authenticated
using (public.can_read_sede_data(sede_codigo));

drop policy if exists "import_replacements_read_authenticated" on public.import_replacements;
create policy "import_replacements_read_authenticated"
on public.import_replacements
for select
to authenticated
using (public.can_read_operational_sede_or_employee(sede_codigo, empleado_id, documento));

drop policy if exists "import_replacements_insert_supervisor" on public.import_replacements;
create policy "import_replacements_insert_supervisor"
on public.import_replacements
for insert
to authenticated
with check (public.current_supervisor_can_write_operational_replacement(sede_codigo, empleado_id, documento));

drop policy if exists "import_replacements_update_supervisor" on public.import_replacements;
create policy "import_replacements_update_supervisor"
on public.import_replacements
for update
to authenticated
using (public.current_supervisor_can_write_operational_replacement(sede_codigo, empleado_id, documento))
with check (public.current_supervisor_can_write_operational_replacement(sede_codigo, empleado_id, documento));

drop policy if exists "daily_sede_closures_read_authenticated" on public.daily_sede_closures;
create policy "daily_sede_closures_read_authenticated"
on public.daily_sede_closures
for select
to authenticated
using (
  public.can_read_zone_data(zona_codigo)
  or public.can_read_sede_data(sede_codigo)
);

drop policy if exists "employee_daily_status_read_authenticated" on public.employee_daily_status;
create policy "employee_daily_status_read_authenticated"
on public.employee_daily_status
for select
to authenticated
using (
  public.can_read_zone_data(zona_codigo_snapshot)
  or public.can_read_sede_data(sede_codigo)
);

drop policy if exists "sede_devices_read_authenticated" on public.sede_devices;
create policy "sede_devices_read_authenticated"
on public.sede_devices
for select
to authenticated
using (public.can_read_sede_data(sede_codigo));

drop policy if exists "sede_device_sites_read_authenticated" on public.sede_device_sites;
create policy "sede_device_sites_read_authenticated"
on public.sede_device_sites
for select
to authenticated
using (public.can_read_sede_data(sede_codigo));

drop policy if exists "attendance_qr_tokens_registry_read" on public.attendance_qr_tokens;
create policy "attendance_qr_tokens_registry_read"
on public.attendance_qr_tokens
for select
to authenticated
using (
  public.can_view_qr_registry()
  and public.can_read_operational_sede_or_employee(sede_codigo, employee_id, documento)
);

drop policy if exists "employee_daily_exits_read_authenticated" on public.employee_daily_exits;
create policy "employee_daily_exits_read_authenticated"
on public.employee_daily_exits
for select
to authenticated
using (public.can_read_operational_sede_or_employee(sede_codigo, employee_id, documento));

drop policy if exists "incapacitados_read_authenticated" on public.incapacitados;
create policy "incapacitados_read_authenticated"
on public.incapacitados
for select
to authenticated
using (public.can_read_employee_data(employee_id, documento));

-- These global summaries do not contain per-zone columns. Keep them available to
-- non-supervisor authenticated roles, but avoid exposing company-wide totals to supervisors.
drop policy if exists "import_history_read_authenticated" on public.import_history;
create policy "import_history_read_authenticated"
on public.import_history
for select
to authenticated
using (public.current_profile_is_active_non_supervisor());

drop policy if exists "daily_metrics_read_authenticated" on public.daily_metrics;
create policy "daily_metrics_read_authenticated"
on public.daily_metrics
for select
to authenticated
using (public.current_profile_is_active_non_supervisor());

drop policy if exists "daily_closures_read_authenticated" on public.daily_closures;
create policy "daily_closures_read_authenticated"
on public.daily_closures
for select
to authenticated
using (public.current_profile_is_active_non_supervisor());
