create table if not exists public.employee_daily_status (
  id text primary key,
  fecha text not null,
  employee_id text not null,
  documento text,
  nombre text,
  tipo_personal text not null check (tipo_personal in ('empleado', 'supernumerario')),
  sede_codigo text,
  sede_nombre_snapshot text,
  zona_codigo_snapshot text,
  zona_nombre_snapshot text,
  dependencia_codigo_snapshot text,
  dependencia_nombre_snapshot text,
  estado_dia text not null check (estado_dia in (
    'trabajado',
    'trabajado_reemplazo',
    'ausente_con_novedad',
    'ausente_sin_reemplazo',
    'incapacidad',
    'vacaciones',
    'compensatorio',
    'sin_registro',
    'no_programado'
  )),
  asistio boolean not null default false,
  novedad_codigo text,
  novedad_nombre text,
  requiere_reemplazo boolean not null default false,
  decision_cobertura text not null default 'no_aplica' check (decision_cobertura in ('no_aplica', 'pendiente', 'reemplazo', 'ausentismo')),
  reemplaza_a_employee_id text,
  reemplaza_a_documento text,
  reemplaza_a_nombre text,
  reemplazado_por_employee_id text,
  reemplazado_por_documento text,
  reemplazado_por_nombre text,
  servicio_programado boolean not null default false,
  servicio_cubierto boolean not null default false,
  cuenta_pago_servicio boolean not null default false,
  cuenta_nomina boolean not null default true,
  paga_nomina boolean,
  motivo_nomina text,
  source_attendance_id text,
  source_replacement_id text,
  source_absenteeism_id text,
  source_incapacity_id text,
  origen text not null default 'manual',
  closed boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (fecha, employee_id),
  constraint employee_daily_status_supernumerario_service_check
    check (not (tipo_personal = 'supernumerario' and cuenta_pago_servicio = true))
);

create index if not exists idx_employee_daily_status_fecha on public.employee_daily_status (fecha);
create index if not exists idx_employee_daily_status_fecha_sede on public.employee_daily_status (fecha, sede_codigo);
create index if not exists idx_employee_daily_status_fecha_tipo on public.employee_daily_status (fecha, tipo_personal);
create index if not exists idx_employee_daily_status_fecha_estado on public.employee_daily_status (fecha, estado_dia);
create index if not exists idx_employee_daily_status_fecha_closed on public.employee_daily_status (fecha, closed);

alter table public.employee_daily_status enable row level security;

drop trigger if exists trg_employee_daily_status_updated_at on public.employee_daily_status;
create trigger trg_employee_daily_status_updated_at
before update on public.employee_daily_status
for each row execute function public.set_updated_at();

drop policy if exists "employee_daily_status_read_authenticated" on public.employee_daily_status;
create policy "employee_daily_status_read_authenticated"
on public.employee_daily_status
for select
to authenticated
using (true);

drop policy if exists "employee_daily_status_write_admin" on public.employee_daily_status;
create policy "employee_daily_status_write_admin"
on public.employee_daily_status
for all
to authenticated
using (public.is_admin_like())
with check (public.is_admin_like());
