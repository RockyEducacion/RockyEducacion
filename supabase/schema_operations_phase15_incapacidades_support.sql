alter table public.incapacitados
  add column if not exists canal_registro text,
  add column if not exists soporte_url text,
  add column if not exists soporte_nombre text,
  add column if not exists soporte_tipo text,
  add column if not exists soporte_storage_path text;

update public.incapacitados
set canal_registro = case
  when coalesce(whatsapp_message_id, '') <> '' then 'whatsapp'
  else 'portal_web'
end
where canal_registro is null;

drop policy if exists "incapacitados_write_admin" on public.incapacitados;
create policy "incapacitados_write_active_user"
on public.incapacitados
for all
to authenticated
using (public.is_active_authenticated_user())
with check (public.is_active_authenticated_user());

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'incapacidades-soportes',
  'incapacidades-soportes',
  true,
  10485760,
  array['application/pdf', 'image/jpeg', 'image/png', 'image/webp']
)
on conflict (id) do update
set public = excluded.public,
    file_size_limit = excluded.file_size_limit,
    allowed_mime_types = excluded.allowed_mime_types;

drop policy if exists "incapacidades_soportes_select_public" on storage.objects;
create policy "incapacidades_soportes_select_public"
on storage.objects
for select
to public
using (bucket_id = 'incapacidades-soportes');

drop policy if exists "incapacidades_soportes_insert_active_user" on storage.objects;
create policy "incapacidades_soportes_insert_active_user"
on storage.objects
for insert
to authenticated
with check (
  bucket_id = 'incapacidades-soportes'
  and public.is_active_authenticated_user()
);

drop policy if exists "incapacidades_soportes_update_active_user" on storage.objects;
create policy "incapacidades_soportes_update_active_user"
on storage.objects
for update
to authenticated
using (
  bucket_id = 'incapacidades-soportes'
  and public.is_active_authenticated_user()
)
with check (
  bucket_id = 'incapacidades-soportes'
  and public.is_active_authenticated_user()
);

drop policy if exists "incapacidades_soportes_delete_active_user" on storage.objects;
create policy "incapacidades_soportes_delete_active_user"
on storage.objects
for delete
to authenticated
using (
  bucket_id = 'incapacidades-soportes'
  and public.is_active_authenticated_user()
);
