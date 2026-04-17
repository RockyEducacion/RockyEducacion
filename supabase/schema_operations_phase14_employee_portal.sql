create table if not exists public.employee_portal_sessions (
  id uuid primary key default gen_random_uuid(),
  employee_id uuid not null references public.employees(id) on delete cascade,
  documento_snapshot text not null,
  nombre_snapshot text,
  telefono_last4_snapshot text not null,
  token_hash text not null unique,
  ip text,
  user_agent text,
  last_seen_at timestamptz,
  expires_at timestamptz not null,
  revoked_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_employee_portal_sessions_employee_id
  on public.employee_portal_sessions(employee_id);

create index if not exists idx_employee_portal_sessions_expires_at
  on public.employee_portal_sessions(expires_at);

create table if not exists public.employee_portal_audit (
  id uuid primary key default gen_random_uuid(),
  employee_id uuid references public.employees(id) on delete set null,
  session_id uuid references public.employee_portal_sessions(id) on delete set null,
  documento text,
  action text not null,
  detail jsonb not null default '{}'::jsonb,
  ip text,
  user_agent text,
  created_at timestamptz not null default now()
);

create index if not exists idx_employee_portal_audit_employee_id
  on public.employee_portal_audit(employee_id, created_at desc);

create index if not exists idx_employee_portal_audit_action
  on public.employee_portal_audit(action, created_at desc);

alter table public.employee_portal_sessions enable row level security;
alter table public.employee_portal_audit enable row level security;

drop trigger if exists trg_employee_portal_sessions_updated_at on public.employee_portal_sessions;
create trigger trg_employee_portal_sessions_updated_at
before update on public.employee_portal_sessions
for each row execute function public.set_updated_at();
