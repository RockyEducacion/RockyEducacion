create unique index if not exists attendance_unique_fecha_documento
on public.attendance (fecha, documento)
where documento is not null;

create unique index if not exists absenteeism_unique_fecha_documento
on public.absenteeism (fecha, documento)
where documento is not null;

create unique index if not exists import_replacements_unique_fecha_documento
on public.import_replacements (fecha, documento)
where documento is not null;
