create index if not exists idx_employee_daily_status_report_range_order
on public.employee_daily_status (fecha, sede_codigo, nombre);

create index if not exists idx_employee_daily_status_report_employee_date
on public.employee_daily_status (employee_id, fecha);

create index if not exists idx_employee_daily_status_report_document_date
on public.employee_daily_status (documento, fecha);

create index if not exists idx_employee_cargo_history_fecha_ingreso_desc
on public.employee_cargo_history (fecha_ingreso desc);

create index if not exists idx_incapacitados_employee_report_range
on public.incapacitados (employee_id, fecha_inicio, fecha_fin);

create index if not exists idx_incapacitados_documento_report_range
on public.incapacitados (documento, fecha_inicio, fecha_fin);
