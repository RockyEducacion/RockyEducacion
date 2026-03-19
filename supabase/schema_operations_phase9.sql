create table if not exists public.daily_sede_closures (
  id text primary key,
  fecha text not null,
  sede_codigo text not null,
  sede_nombre text,
  zona_codigo text,
  zona_nombre text,
  dependencia_codigo text,
  dependencia_nombre text,
  planeados integer not null default 0,
  contratados integer not null default 0,
  registrados integer not null default 0,
  faltantes integer not null default 0,
  sobrantes integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (fecha, sede_codigo)
);

alter table public.daily_sede_closures enable row level security;

drop trigger if exists trg_daily_sede_closures_updated_at on public.daily_sede_closures;
create trigger trg_daily_sede_closures_updated_at
before update on public.daily_sede_closures
for each row execute function public.set_updated_at();

drop policy if exists "daily_sede_closures_read_authenticated" on public.daily_sede_closures;
create policy "daily_sede_closures_read_authenticated"
on public.daily_sede_closures
for select
to authenticated
using (true);

drop policy if exists "daily_sede_closures_write_admin" on public.daily_sede_closures;
create policy "daily_sede_closures_write_admin"
on public.daily_sede_closures
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());
