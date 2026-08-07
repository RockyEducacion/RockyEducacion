-- Adds the July 9 national holiday introduced by Ley 2578 de 2026.
create or replace function public.is_colombia_holiday_sql(fecha text)
returns boolean
language plpgsql
immutable
as $$
declare
  v_fecha date;
  v_year integer;
  v_easter date;
begin
  if fecha is null or fecha !~ '^\d{4}-\d{2}-\d{2}$' then
    return false;
  end if;

  v_fecha := fecha::date;
  v_year := extract(year from v_fecha);
  v_easter := public.easter_sunday_sql(v_year);

  return v_fecha in (
    make_date(v_year, 1, 1),
    make_date(v_year, 5, 1),
    make_date(v_year, 7, 20),
    make_date(v_year, 8, 7),
    make_date(v_year, 12, 8),
    make_date(v_year, 12, 25),
    public.move_to_following_monday_sql(make_date(v_year, 1, 6)),
    public.move_to_following_monday_sql(make_date(v_year, 3, 19)),
    public.move_to_following_monday_sql(make_date(v_year, 6, 29)),
    public.move_to_following_monday_sql(make_date(v_year, 7, 9)),
    public.move_to_following_monday_sql(make_date(v_year, 8, 15)),
    public.move_to_following_monday_sql(make_date(v_year, 10, 12)),
    public.move_to_following_monday_sql(make_date(v_year, 11, 1)),
    public.move_to_following_monday_sql(make_date(v_year, 11, 11)),
    v_easter - 3,
    v_easter - 2,
    public.move_to_following_monday_sql(v_easter + 39),
    public.move_to_following_monday_sql(v_easter + 60),
    public.move_to_following_monday_sql(v_easter + 68)
  );
end;
$$;
