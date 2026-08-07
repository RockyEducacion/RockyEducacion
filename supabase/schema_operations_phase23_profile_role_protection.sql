-- Phase 23: protect profile access fields from self-service overwrites.
-- Apply after phase 21.

create or replace function public.protect_profile_self_service_fields()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  current_uid uuid := auth.uid();
  current_is_superadmin boolean := public.is_superadmin();
begin
  if current_uid is null then
    return new;
  end if;

  if tg_op = 'INSERT' then
    if new.id = current_uid and current_is_superadmin is not true then
      new.role := 'empleado';
      new.estado := 'activo';
      new.supervisor_eligible := false;
      new.zona_codigo := null;
      new.zonas_permitidas := '{}'::text[];
      new.created_by_uid := null;
      new.created_by_email := null;
      new.last_modified_by_uid := null;
      new.last_modified_by_email := null;
      new.deleted_at := null;
      new.deleted_by_uid := null;
      new.deleted_by_email := null;
    end if;
    return new;
  end if;

  if tg_op = 'UPDATE' then
    if old.id = current_uid and current_is_superadmin is not true then
      if new.role is distinct from old.role
        or new.estado is distinct from old.estado
        or new.supervisor_eligible is distinct from old.supervisor_eligible
        or new.zona_codigo is distinct from old.zona_codigo
        or new.zonas_permitidas is distinct from old.zonas_permitidas
        or new.created_by_uid is distinct from old.created_by_uid
        or new.created_by_email is distinct from old.created_by_email
        or new.last_modified_by_uid is distinct from old.last_modified_by_uid
        or new.last_modified_by_email is distinct from old.last_modified_by_email
        or new.deleted_at is distinct from old.deleted_at
        or new.deleted_by_uid is distinct from old.deleted_by_uid
        or new.deleted_by_email is distinct from old.deleted_by_email
      then
        raise exception 'No puedes modificar rol, estado ni campos administrativos de tu propio perfil.';
      end if;
    end if;
    return new;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_profiles_protect_self_service_fields on public.profiles;
create trigger trg_profiles_protect_self_service_fields
before insert or update on public.profiles
for each row
execute function public.protect_profile_self_service_fields();
