alter table public.employee_cargo_history
  add column if not exists sede_codigo text,
  add column if not exists sede_nombre text;
