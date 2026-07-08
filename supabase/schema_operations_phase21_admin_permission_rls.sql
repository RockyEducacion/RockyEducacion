-- Phase 21: let eligible supervisors use administrative modules according to permissions.
-- Apply after phase 18.

create or replace function public.default_role_has_permission(role_value text, permission_key text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select case
    when role_value = 'superadmin' then true
    when role_value = 'admin' then permission_key = any(array[
      'viewUsers','editUsers','viewZones','editZones','viewDependencies','editDependencies',
      'viewSedes','editSedes','viewEmployees','editEmployees','manageEmployees',
      'manageEmployeeSchedules','viewSupervisors','editSupervisors','manageSupervisors',
      'viewSupernumerarios','editSupernumerarios','viewCargos','editCargos',
      'viewNovedades','editNovedades','viewQrScanner','viewQrDailyRegistry',
      'manageQrDevices'
    ])
    when role_value = 'supervisor' then permission_key = any(array[
      'viewSedes','editSedes','viewEmployees','editEmployees','manageEmployees',
      'viewSupervisors','editSupervisors','manageSupervisors',
      'viewQrScanner','viewQrDailyRegistry','manageQrDevices','uploadData'
    ])
    else false
  end;
$$;

create or replace function public.current_profile_has_permission(permission_key text, legacy_key text default null)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select coalesce((
    select case
      when p.estado <> 'activo' then false
      when p.role::text = 'superadmin' then true
      when p.role::text = 'supervisor' and p.supervisor_eligible is not true then false
      else coalesce(
        case when u.permissions ? permission_key then (u.permissions ->> permission_key)::boolean end,
        case when legacy_key is not null and u.permissions ? legacy_key then (u.permissions ->> legacy_key)::boolean end,
        case when rm.permissions ? permission_key then (rm.permissions ->> permission_key)::boolean end,
        case when legacy_key is not null and rm.permissions ? legacy_key then (rm.permissions ->> legacy_key)::boolean end,
        (
          public.default_role_has_permission(p.role::text, permission_key)
          or (
            legacy_key is not null
            and public.default_role_has_permission(p.role::text, legacy_key)
          )
        ),
        false
      )
    end
    from public.profiles p
    left join public.roles_matrix rm on rm.role = p.role
    left join public.user_overrides u on u.user_id = p.id
    where p.id = auth.uid()
  ), false);
$$;

grant execute on function public.default_role_has_permission(text, text) to authenticated;
grant execute on function public.current_profile_has_permission(text, text) to authenticated;

drop policy if exists "employees_write_admin" on public.employees;
create policy "employees_write_admin"
on public.employees
for all
to authenticated
using (
  public.is_admin_like()
  or (
    public.current_profile_has_permission('editEmployees', 'manageEmployees')
    and public.can_read_employee_data(id, documento)
  )
)
with check (
  public.is_admin_like()
  or (
    public.current_profile_has_permission('editEmployees', 'manageEmployees')
    and (
      public.current_supervisor_can_read_zone(zona_codigo)
      or public.can_read_sede_data(sede_codigo)
    )
  )
);

drop policy if exists "employee_cargo_history_write_admin" on public.employee_cargo_history;
create policy "employee_cargo_history_write_admin"
on public.employee_cargo_history
for all
to authenticated
using (
  public.is_admin_like()
  or (
    public.current_profile_has_permission('editEmployees', 'manageEmployees')
    and public.can_read_employee_data(employee_id, documento)
  )
)
with check (
  public.is_admin_like()
  or (
    public.current_profile_has_permission('editEmployees', 'manageEmployees')
    and public.can_read_employee_data(employee_id, documento)
  )
);
