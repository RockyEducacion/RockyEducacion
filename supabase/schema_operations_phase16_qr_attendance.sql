alter table public.sedes
  add column if not exists qr_enabled boolean not null default false;

alter table public.sedes
  add column if not exists qr_latitude double precision,
  add column if not exists qr_longitude double precision,
  add column if not exists qr_radius_meters integer not null default 500;

create table if not exists public.sede_devices (
  id uuid primary key default gen_random_uuid(),
  sede_id uuid references public.sedes(id) on delete cascade,
  sede_codigo text not null,
  sede_nombre text,
  device_name text not null,
  token_hash text not null unique,
  estado text not null default 'activo',
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text,
  last_seen_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_sede_devices_sede_codigo
  on public.sede_devices(sede_codigo);

create index if not exists idx_sede_devices_estado
  on public.sede_devices(estado);

alter table public.sede_devices
  add column if not exists last_modified_by_uid uuid references public.profiles(id) on delete set null,
  add column if not exists last_modified_by_email text,
  add column if not exists last_modified_at timestamptz,
  add column if not exists revoked_by_uid uuid references public.profiles(id) on delete set null,
  add column if not exists revoked_by_email text;

create table if not exists public.sede_device_sites (
  id uuid primary key default gen_random_uuid(),
  device_id uuid not null references public.sede_devices(id) on delete cascade,
  sede_id uuid references public.sedes(id) on delete cascade,
  sede_codigo text not null,
  sede_nombre text,
  created_at timestamptz not null default now(),
  unique(device_id, sede_codigo)
);

create index if not exists idx_sede_device_sites_device_id
  on public.sede_device_sites(device_id);

create index if not exists idx_sede_device_sites_sede_codigo
  on public.sede_device_sites(sede_codigo);

insert into public.sede_device_sites(device_id, sede_id, sede_codigo, sede_nombre)
select d.id, d.sede_id, d.sede_codigo, d.sede_nombre
from public.sede_devices d
where d.sede_codigo is not null
on conflict (device_id, sede_codigo) do nothing;

create table if not exists public.attendance_qr_tokens (
  id uuid primary key default gen_random_uuid(),
  token_hash text not null unique,
  action text not null check (action in ('entry', 'exit')),
  fecha text not null,
  employee_id uuid not null references public.employees(id) on delete cascade,
  documento text not null,
  nombre text,
  sede_codigo text not null,
  sede_nombre text,
  phone_number text,
  request_latitude double precision,
  request_longitude double precision,
  request_distance_meters integer,
  location_verified_at timestamptz,
  expires_at timestamptz not null,
  used_at timestamptz,
  used_by_device_id uuid references public.sede_devices(id) on delete set null,
  created_at timestamptz not null default now()
);

create index if not exists idx_attendance_qr_tokens_employee_fecha
  on public.attendance_qr_tokens(employee_id, fecha);

create index if not exists idx_attendance_qr_tokens_expires_at
  on public.attendance_qr_tokens(expires_at);

alter table public.attendance_qr_tokens
  add column if not exists request_latitude double precision,
  add column if not exists request_longitude double precision,
  add column if not exists request_distance_meters integer,
  add column if not exists location_verified_at timestamptz;

create table if not exists public.employee_daily_exits (
  id text primary key,
  fecha text not null,
  employee_id uuid not null references public.employees(id) on delete cascade,
  documento text not null,
  nombre text,
  sede_codigo text not null,
  sede_nombre text,
  qr_token_id uuid references public.attendance_qr_tokens(id) on delete set null,
  device_id uuid references public.sede_devices(id) on delete set null,
  entry_attendance_id text references public.attendance(id) on delete set null,
  exit_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create unique index if not exists employee_daily_exits_unique_fecha_documento
  on public.employee_daily_exits(fecha, documento)
  where documento is not null;

create table if not exists public.attendance_qr_scans (
  id uuid primary key default gen_random_uuid(),
  qr_token_id uuid references public.attendance_qr_tokens(id) on delete set null,
  device_id uuid references public.sede_devices(id) on delete set null,
  action text,
  fecha text,
  employee_id uuid references public.employees(id) on delete set null,
  documento text,
  sede_codigo text,
  ok boolean not null default false,
  reason text,
  ip text,
  user_agent text,
  created_at timestamptz not null default now()
);

create index if not exists idx_attendance_qr_scans_created_at
  on public.attendance_qr_scans(created_at desc);

do $$
declare
  tbl text;
begin
  foreach tbl in array array[
    'attendance_qr_tokens',
    'employee_daily_exits',
    'employee_daily_status'
  ]
  loop
    if not exists (
      select 1
      from pg_publication_tables
      where pubname = 'supabase_realtime'
        and schemaname = 'public'
        and tablename = tbl
    ) then
      execute format('alter publication supabase_realtime add table public.%I', tbl);
    end if;
  end loop;
end $$;

alter table public.sede_devices enable row level security;
alter table public.sede_device_sites enable row level security;
alter table public.attendance_qr_tokens enable row level security;
alter table public.employee_daily_exits enable row level security;
alter table public.attendance_qr_scans enable row level security;

drop trigger if exists trg_sede_devices_updated_at on public.sede_devices;
create trigger trg_sede_devices_updated_at
before update on public.sede_devices
for each row execute function public.set_updated_at();

drop policy if exists "sede_devices_read_authenticated" on public.sede_devices;
create policy "sede_devices_read_authenticated"
on public.sede_devices
for select
to authenticated
using (true);

drop policy if exists "sede_devices_write_admin" on public.sede_devices;
create policy "sede_devices_write_admin"
on public.sede_devices
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "sede_device_sites_read_authenticated" on public.sede_device_sites;
create policy "sede_device_sites_read_authenticated"
on public.sede_device_sites
for select
to authenticated
using (true);

drop policy if exists "sede_device_sites_write_admin" on public.sede_device_sites;
create policy "sede_device_sites_write_admin"
on public.sede_device_sites
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

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
        p.role in ('superadmin', 'admin', 'editor')
        or (p.role = 'supervisor' and p.supervisor_eligible = true)
      )
  );
$$;

drop policy if exists "attendance_qr_tokens_admin_read" on public.attendance_qr_tokens;
drop policy if exists "attendance_qr_tokens_registry_read" on public.attendance_qr_tokens;
create policy "attendance_qr_tokens_registry_read"
on public.attendance_qr_tokens
for select
to authenticated
using (public.can_view_qr_registry());

drop policy if exists "employee_daily_exits_read_authenticated" on public.employee_daily_exits;
create policy "employee_daily_exits_read_authenticated"
on public.employee_daily_exits
for select
to authenticated
using (true);

drop policy if exists "employee_daily_exits_write_admin" on public.employee_daily_exits;
create policy "employee_daily_exits_write_admin"
on public.employee_daily_exits
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "attendance_qr_scans_admin_read" on public.attendance_qr_scans;
create policy "attendance_qr_scans_admin_read"
on public.attendance_qr_scans
for select
to authenticated
using (public.is_admin_like());
