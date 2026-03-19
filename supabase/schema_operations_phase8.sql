alter table public.daily_closures
  add column if not exists faltan integer not null default 0,
  add column if not exists sobran integer not null default 0;
