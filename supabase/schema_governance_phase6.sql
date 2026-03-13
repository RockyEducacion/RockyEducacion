alter table public.profiles
  add column if not exists created_by_uid uuid references public.profiles(id) on delete set null,
  add column if not exists created_by_email text,
  add column if not exists last_modified_by_uid uuid references public.profiles(id) on delete set null,
  add column if not exists last_modified_by_email text,
  add column if not exists deleted_at timestamptz,
  add column if not exists deleted_by_uid uuid references public.profiles(id) on delete set null,
  add column if not exists deleted_by_email text;

create table if not exists public.audit_logs (
  id uuid primary key default gen_random_uuid(),
  actor_uid uuid references public.profiles(id) on delete set null,
  actor_email text,
  target_type text,
  target_id text,
  action text not null,
  before_data jsonb,
  after_data jsonb,
  note text,
  created_at timestamptz not null default now()
);

alter table public.audit_logs enable row level security;

create or replace function public.is_active_authenticated_user()
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from public.profiles p
    where p.id = auth.uid()
      and p.estado = 'activo'
  );
$$;

drop policy if exists "audit_logs_read_admin_like" on public.audit_logs;
create policy "audit_logs_read_admin_like"
on public.audit_logs
for select
to authenticated
using (public.is_admin_like());

drop policy if exists "audit_logs_insert_active_user" on public.audit_logs;
create policy "audit_logs_insert_active_user"
on public.audit_logs
for insert
to authenticated
with check (public.is_active_authenticated_user());
