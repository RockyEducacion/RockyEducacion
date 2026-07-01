create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.zones (
  id uuid primary key default gen_random_uuid(),
  codigo text not null unique,
  nombre text,
  estado text not null default 'activo',
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.dependencies (
  id uuid primary key default gen_random_uuid(),
  codigo text not null unique,
  nombre text,
  estado text not null default 'activo',
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.sedes (
  id uuid primary key default gen_random_uuid(),
  codigo text not null unique,
  nombre text,
  dependencia_codigo text,
  dependencia_nombre text,
  zona_codigo text,
  zona_nombre text,
  numero_operarios integer,
  jornada text not null default 'lun_vie',
  estado text not null default 'activo',
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.cargos (
  id uuid primary key default gen_random_uuid(),
  codigo text not null unique,
  nombre text,
  salario numeric,
  alineacion_crud text not null default 'empleado',
  estado text not null default 'activo',
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.cargos
  add column if not exists salario numeric;

create table if not exists public.novedades (
  id uuid primary key default gen_random_uuid(),
  codigo text not null unique,
  codigo_novedad text unique,
  nombre text,
  reemplazo text,
  nomina text,
  estado text not null default 'activo',
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.zones enable row level security;
alter table public.dependencies enable row level security;
alter table public.sedes enable row level security;
alter table public.cargos enable row level security;
alter table public.novedades enable row level security;

drop trigger if exists trg_zones_updated_at on public.zones;
create trigger trg_zones_updated_at
before update on public.zones
for each row execute function public.set_updated_at();

drop trigger if exists trg_dependencies_updated_at on public.dependencies;
create trigger trg_dependencies_updated_at
before update on public.dependencies
for each row execute function public.set_updated_at();

drop trigger if exists trg_sedes_updated_at on public.sedes;
create trigger trg_sedes_updated_at
before update on public.sedes
for each row execute function public.set_updated_at();

drop trigger if exists trg_cargos_updated_at on public.cargos;
create trigger trg_cargos_updated_at
before update on public.cargos
for each row execute function public.set_updated_at();

drop trigger if exists trg_novedades_updated_at on public.novedades;
create trigger trg_novedades_updated_at
before update on public.novedades
for each row execute function public.set_updated_at();

drop policy if exists "zones_read_authenticated" on public.zones;
create policy "zones_read_authenticated"
on public.zones
for select
to authenticated
using (true);

drop policy if exists "zones_write_admin" on public.zones;
create policy "zones_write_admin"
on public.zones
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "dependencies_read_authenticated" on public.dependencies;
create policy "dependencies_read_authenticated"
on public.dependencies
for select
to authenticated
using (true);

drop policy if exists "dependencies_write_admin" on public.dependencies;
create policy "dependencies_write_admin"
on public.dependencies
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "sedes_read_authenticated" on public.sedes;
create policy "sedes_read_authenticated"
on public.sedes
for select
to authenticated
using (true);

drop policy if exists "sedes_write_admin" on public.sedes;
create policy "sedes_write_admin"
on public.sedes
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "cargos_read_authenticated" on public.cargos;
create policy "cargos_read_authenticated"
on public.cargos
for select
to authenticated
using (true);

drop policy if exists "cargos_write_admin" on public.cargos;
create policy "cargos_write_admin"
on public.cargos
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "novedades_read_authenticated" on public.novedades;
create policy "novedades_read_authenticated"
on public.novedades
for select
to authenticated
using (true);

drop policy if exists "novedades_write_admin" on public.novedades;
create policy "novedades_write_admin"
on public.novedades
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());
