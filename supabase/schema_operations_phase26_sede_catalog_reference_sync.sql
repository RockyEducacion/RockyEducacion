-- Phase 26: keep live sede catalog references synchronized after sede edits.
-- Apply after phase 25.

create or replace function public.sync_sede_catalog_references()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if coalesce(new.codigo, '') = coalesce(old.codigo, '')
    and coalesce(new.nombre, '') = coalesce(old.nombre, '')
    and coalesce(new.zona_codigo, '') = coalesce(old.zona_codigo, '')
    and coalesce(new.zona_nombre, '') = coalesce(old.zona_nombre, '')
  then
    return new;
  end if;

  update public.employees
  set
    sede_codigo = new.codigo,
    sede_nombre = new.nombre,
    zona_codigo = new.zona_codigo,
    zona_nombre = new.zona_nombre,
    last_modified_at = now()
  where sede_codigo = old.codigo;

  update public.employee_cargo_history
  set
    sede_codigo = new.codigo,
    sede_nombre = new.nombre
  where sede_codigo = old.codigo;

  update public.supervisor_profile
  set
    sede_codigo = new.codigo,
    zona_codigo = new.zona_codigo,
    zona_nombre = new.zona_nombre,
    last_modified_at = now()
  where sede_codigo = old.codigo;

  update public.sede_devices
  set
    sede_codigo = new.codigo,
    sede_nombre = new.nombre
  where sede_codigo = old.codigo;

  update public.sede_device_sites
  set
    sede_codigo = new.codigo,
    sede_nombre = new.nombre
  where sede_codigo = old.codigo;

  return new;
end;
$$;

drop trigger if exists trg_sedes_sync_catalog_references on public.sedes;
create trigger trg_sedes_sync_catalog_references
after update of codigo, nombre, zona_codigo, zona_nombre on public.sedes
for each row
execute function public.sync_sede_catalog_references();

update public.employees e
set
  sede_nombre = s.nombre,
  zona_codigo = s.zona_codigo,
  zona_nombre = s.zona_nombre
from public.sedes s
where e.sede_codigo = s.codigo
  and (
    coalesce(e.sede_nombre, '') <> coalesce(s.nombre, '')
    or coalesce(e.zona_codigo, '') <> coalesce(s.zona_codigo, '')
    or coalesce(e.zona_nombre, '') <> coalesce(s.zona_nombre, '')
  );

update public.employee_cargo_history h
set sede_nombre = s.nombre
from public.sedes s
where h.sede_codigo = s.codigo
  and coalesce(h.sede_nombre, '') <> coalesce(s.nombre, '');

update public.sede_devices d
set sede_nombre = s.nombre
from public.sedes s
where d.sede_codigo = s.codigo
  and coalesce(d.sede_nombre, '') <> coalesce(s.nombre, '');

update public.sede_device_sites ds
set sede_nombre = s.nombre
from public.sedes s
where ds.sede_codigo = s.codigo
  and coalesce(ds.sede_nombre, '') <> coalesce(s.nombre, '');
