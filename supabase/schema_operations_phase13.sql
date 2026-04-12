do $$
declare
  tbl text;
begin
  foreach tbl in array array[
    'roles_matrix',
    'user_overrides',
    'zones',
    'dependencies',
    'sedes',
    'cargos',
    'novedades',
    'employees',
    'employee_cargo_history',
    'supervisor_profile',
    'import_history',
    'daily_closures',
    'attendance',
    'import_replacements',
    'daily_metrics',
    'incapacitados'
  ]
  loop
    if not exists (
      select 1
      from pg_publication_tables
      where pubname = 'supabase_realtime'
        and schemaname = 'public'
        and tablename = tbl
    ) then
      execute format('alter publication supabase_realtime add table public.%I', tbl);
    end if;
  end loop;
end $$;
