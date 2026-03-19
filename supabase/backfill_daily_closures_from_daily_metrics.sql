update public.daily_closures dc
set
  planeados = coalesce(dm.planned, dc.planeados, 0),
  contratados = coalesce(dm.expected, dc.contratados, 0),
  asistencias = coalesce(dm.attendance_count, dc.asistencias, 0),
  ausentismos = coalesce(dm.absenteeism, dc.ausentismos, 0),
  no_contratados = coalesce(dm.no_contracted, dc.no_contratados, 0),
  updated_at = now()
from public.daily_metrics dm
where dm.fecha = dc.fecha
  and (
    dc.locked = true
    or coalesce(dc.status, '') = 'closed'
  );
