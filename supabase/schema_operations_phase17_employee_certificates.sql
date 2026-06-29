alter table public.cargos
  add column if not exists salario numeric;

create table if not exists public.employee_certificate_audit (
  id uuid primary key default gen_random_uuid(),
  employee_id uuid references public.employees(id) on delete set null,
  employee_codigo text,
  documento text,
  nombre text,
  verification_code text,
  certificate_type text not null check (certificate_type in ('basic', 'with_salary')),
  channel text not null check (channel in ('admin', 'employee_portal')),
  requested_by_profile_id uuid references public.profiles(id) on delete set null,
  requested_by_email text,
  requested_by_employee_session_id uuid references public.employee_portal_sessions(id) on delete set null,
  ip text,
  user_agent text,
  created_at timestamptz not null default now()
);

alter table public.employee_certificate_audit
  add column if not exists verification_code text;

create index if not exists idx_employee_certificate_audit_employee_id
  on public.employee_certificate_audit(employee_id, created_at desc);

create index if not exists idx_employee_certificate_audit_documento
  on public.employee_certificate_audit(documento, created_at desc);

create index if not exists idx_employee_certificate_audit_channel
  on public.employee_certificate_audit(channel, created_at desc);

create unique index if not exists idx_employee_certificate_audit_verification_code
  on public.employee_certificate_audit(verification_code)
  where verification_code is not null;

alter table public.employee_certificate_audit enable row level security;

drop policy if exists "employee_certificate_audit_read_admin" on public.employee_certificate_audit;
create policy "employee_certificate_audit_read_admin"
on public.employee_certificate_audit
for select
to authenticated
using (public.is_admin_like());
