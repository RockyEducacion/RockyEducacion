-- Phase 25: extended employee information.
-- Adds optional detail fields kept outside the main employee table view.

alter table public.employees
  add column if not exists fecha_nacimiento date,
  add column if not exists eps text,
  add column if not exists afp text,
  add column if not exists arl_riesgo text,
  add column if not exists dotacion_camisa text,
  add column if not exists dotacion_pantalon text,
  add column if not exists dotacion_zapatos text;
