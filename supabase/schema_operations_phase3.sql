create table if not exists public.import_history (
  id uuid primary key default gen_random_uuid(),
  fecha_operacion text,
  ts timestamptz not null default now(),
  source text,
  planned_count integer not null default 0,
  expected_count integer not null default 0,
  found_count integer not null default 0,
  missing_count integer not null default 0,
  extra_count integer not null default 0,
  missing_supervisors_count integer not null default 0,
  missing_supernumerarios_count integer not null default 0,
  missing_docs jsonb not null default '[]'::jsonb,
  extra_docs jsonb not null default '[]'::jsonb,
  missing_supervisors jsonb not null default '[]'::jsonb,
  missing_supernumerarios jsonb not null default '[]'::jsonb,
  errores jsonb not null default '[]'::jsonb,
  confirmado_por_uid uuid references public.profiles(id) on delete set null,
  confirmado_por_email text
);

create table if not exists public.attendance (
  id text primary key,
  fecha text not null,
  empleado_id uuid references public.employees(id) on delete cascade,
  documento text,
  nombre text,
  sede_codigo text,
  sede_nombre text,
  asistio boolean not null default false,
  novedad text,
  created_at timestamptz not null default now()
);

create table if not exists public.absenteeism (
  id text primary key,
  fecha text not null,
  empleado_id uuid references public.employees(id) on delete cascade,
  documento text,
  nombre text,
  sede_codigo text,
  sede_nombre text,
  estado text not null default 'pendiente',
  reemplazo_id uuid references public.employees(id) on delete set null,
  reemplazo_documento text,
  created_at timestamptz not null default now(),
  created_by_uid uuid references public.profiles(id) on delete set null,
  created_by_email text
);

create table if not exists public.sede_status (
  id text primary key,
  fecha text not null,
  sede_codigo text not null,
  sede_nombre text,
  operarios_esperados integer not null default 0,
  operarios_presentes integer not null default 0,
  faltantes integer not null default 0,
  created_at timestamptz not null default now()
);

create table if not exists public.import_replacements (
  id text primary key,
  import_id uuid references public.import_history(id) on delete set null,
  fecha_operacion text,
  fecha text not null,
  empleado_id uuid references public.employees(id) on delete cascade,
  documento text,
  nombre text,
  sede_codigo text,
  sede_nombre text,
  novedad_codigo text,
  novedad_nombre text,
  decision text not null default 'ausentismo',
  supernumerario_id uuid references public.employees(id) on delete set null,
  supernumerario_documento text,
  supernumerario_nombre text,
  ts timestamptz not null default now(),
  actor_uid uuid references public.profiles(id) on delete set null,
  actor_email text
);

create table if not exists public.daily_metrics (
  id text primary key,
  fecha text not null unique,
  planned integer not null default 0,
  expected integer not null default 0,
  unique_count integer not null default 0,
  missing integer not null default 0,
  attendance_count integer not null default 0,
  absenteeism integer not null default 0,
  paid_services integer not null default 0,
  no_contracted integer not null default 0,
  closed boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.daily_closures (
  id text primary key,
  fecha text not null unique,
  status text not null default 'closed',
  locked boolean not null default true,
  planeados integer not null default 0,
  contratados integer not null default 0,
  asistencias integer not null default 0,
  ausentismos integer not null default 0,
  pagados integer not null default 0,
  no_contratados integer not null default 0,
  closed_by_uid uuid references public.profiles(id) on delete set null,
  closed_by_email text,
  closed_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.import_history enable row level security;
alter table public.attendance enable row level security;
alter table public.absenteeism enable row level security;
alter table public.sede_status enable row level security;
alter table public.import_replacements enable row level security;
alter table public.daily_metrics enable row level security;
alter table public.daily_closures enable row level security;

drop trigger if exists trg_daily_metrics_updated_at on public.daily_metrics;
create trigger trg_daily_metrics_updated_at
before update on public.daily_metrics
for each row execute function public.set_updated_at();

drop trigger if exists trg_daily_closures_updated_at on public.daily_closures;
create trigger trg_daily_closures_updated_at
before update on public.daily_closures
for each row execute function public.set_updated_at();

drop policy if exists "import_history_read_authenticated" on public.import_history;
create policy "import_history_read_authenticated"
on public.import_history
for select
to authenticated
using (true);

drop policy if exists "import_history_write_admin" on public.import_history;
create policy "import_history_write_admin"
on public.import_history
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "attendance_read_authenticated" on public.attendance;
create policy "attendance_read_authenticated"
on public.attendance
for select
to authenticated
using (true);

drop policy if exists "attendance_write_admin" on public.attendance;
create policy "attendance_write_admin"
on public.attendance
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "absenteeism_read_authenticated" on public.absenteeism;
create policy "absenteeism_read_authenticated"
on public.absenteeism
for select
to authenticated
using (true);

drop policy if exists "absenteeism_write_admin" on public.absenteeism;
create policy "absenteeism_write_admin"
on public.absenteeism
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "sede_status_read_authenticated" on public.sede_status;
create policy "sede_status_read_authenticated"
on public.sede_status
for select
to authenticated
using (true);

drop policy if exists "sede_status_write_admin" on public.sede_status;
create policy "sede_status_write_admin"
on public.sede_status
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "import_replacements_read_authenticated" on public.import_replacements;
create policy "import_replacements_read_authenticated"
on public.import_replacements
for select
to authenticated
using (true);

drop policy if exists "import_replacements_write_admin" on public.import_replacements;
create policy "import_replacements_write_admin"
on public.import_replacements
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "daily_metrics_read_authenticated" on public.daily_metrics;
create policy "daily_metrics_read_authenticated"
on public.daily_metrics
for select
to authenticated
using (true);

drop policy if exists "daily_metrics_write_admin" on public.daily_metrics;
create policy "daily_metrics_write_admin"
on public.daily_metrics
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());

drop policy if exists "daily_closures_read_authenticated" on public.daily_closures;
create policy "daily_closures_read_authenticated"
on public.daily_closures
for select
to authenticated
using (true);

drop policy if exists "daily_closures_write_admin" on public.daily_closures;
create policy "daily_closures_write_admin"
on public.daily_closures
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());
