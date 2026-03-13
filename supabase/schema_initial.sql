create type public.app_role as enum (
  'superadmin',
  'admin',
  'editor',
  'consultor',
  'supervisor',
  'empleado'
);

create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text unique,
  display_name text,
  documento text,
  role public.app_role not null default 'empleado',
  estado text not null default 'activo',
  zona_codigo text,
  zonas_permitidas text[] not null default '{}',
  supervisor_eligible boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.roles_matrix (
  role public.app_role primary key,
  permissions jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.user_overrides (
  user_id uuid primary key references public.profiles(id) on delete cascade,
  permissions jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.profiles enable row level security;
alter table public.roles_matrix enable row level security;
alter table public.user_overrides enable row level security;

create or replace function public.is_admin_like()
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
      and p.role in ('superadmin', 'admin')
      and p.estado = 'activo'
  );
$$;

create or replace function public.is_superadmin()
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
      and p.role = 'superadmin'
      and p.estado = 'activo'
  );
$$;

drop policy if exists "profiles_select_self_or_admin" on public.profiles;
create policy "profiles_select_self_or_admin"
on public.profiles
for select
to authenticated
using (
  auth.uid() = id
  or public.is_admin_like()
);

drop policy if exists "profiles_insert_self_or_admin" on public.profiles;
create policy "profiles_insert_self_or_admin"
on public.profiles
for insert
to authenticated
with check (
  auth.uid() = id
  or public.is_admin_like()
);

drop policy if exists "profiles_update_self_or_admin" on public.profiles;
create policy "profiles_update_self_or_admin"
on public.profiles
for update
to authenticated
using (
  auth.uid() = id
  or public.is_admin_like()
)
with check (
  auth.uid() = id
  or public.is_admin_like()
);

drop policy if exists "roles_matrix_read_authenticated" on public.roles_matrix;
create policy "roles_matrix_read_authenticated"
on public.roles_matrix
for select
to authenticated
using (true);

drop policy if exists "roles_matrix_write_superadmin" on public.roles_matrix;
create policy "roles_matrix_write_superadmin"
on public.roles_matrix
for all
to authenticated
using (public.is_superadmin())
with check (public.is_superadmin());

drop policy if exists "user_overrides_select_self_or_superadmin" on public.user_overrides;
create policy "user_overrides_select_self_or_superadmin"
on public.user_overrides
for select
to authenticated
using (
  user_id = auth.uid()
  or public.is_superadmin()
);

drop policy if exists "user_overrides_write_superadmin" on public.user_overrides;
create policy "user_overrides_write_superadmin"
on public.user_overrides
for all
to authenticated
using (public.is_superadmin())
with check (public.is_superadmin());

insert into public.roles_matrix (role, permissions)
values
  ('superadmin', '{}'::jsonb),
  ('admin', '{}'::jsonb),
  ('editor', '{}'::jsonb),
  ('consultor', '{}'::jsonb),
  ('supervisor', '{}'::jsonb),
  ('empleado', '{}'::jsonb)
on conflict (role) do nothing;
