alter table public.daily_closures
  add column if not exists asistencias integer not null default 0;

update public.daily_closures
set asistencias = coalesce(nullif(asistencias, 0), pagados, 0)
where coalesce(asistencias, 0) = 0;

alter table public.daily_closures
  drop column if exists pagados;
