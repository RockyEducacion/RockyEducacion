# Preparacion de Supabase para RockyEDU

## 1. Crear el proyecto
- Crea un proyecto nuevo en Supabase.
- Elige una region cercana a Colombia o a tu operacion principal.
- Guarda de inmediato estos datos:
  - `Project URL`
  - `Publishable key / anon key`
  - `service_role key`
  - password de PostgreSQL

## 2. Configurar autenticacion
- Ve a `Authentication > Providers`.
- Activa `Email`.
- Si vas a crear usuarios sin confirmacion por correo para contingencia, desactiva la confirmacion obligatoria mientras migramos.
- Ve a `Authentication > URL Configuration`.
- Configura:
  - `Site URL`: la URL principal de la app
  - `Redirect URLs`: agrega al menos:
    - `http://localhost:5500`
    - `http://127.0.0.1:5500`
    - la URL productiva actual de la app
- Ve a `Authentication > SMTP` y conecta un correo real antes de pasar a produccion.

## 3. Crear el esquema inicial
- Abre `SQL Editor`.
- Ejecuta el script `supabase/schema_initial.sql`.
- Ese script crea:
  - `public.profiles`
  - `public.roles_matrix`
  - `public.user_overrides`
  - funciones auxiliares para roles
  - politicas RLS iniciales

## 4. Crear el primer superadmin
- Crea manualmente un usuario desde `Authentication > Users`.
- Copia el `UUID` del usuario creado.
- Ejecuta este SQL cambiando los datos:

```sql
insert into public.profiles (
  id,
  email,
  display_name,
  documento,
  role,
  estado,
  supervisor_eligible
) values (
  'REEMPLAZA_UUID',
  'tu-correo@dominio.com',
  'Administrador Principal',
  '1234567890',
  'superadmin',
  'activo',
  true
)
on conflict (id) do update
set
  email = excluded.email,
  display_name = excluded.display_name,
  documento = excluded.documento,
  role = excluded.role,
  estado = excluded.estado,
  supervisor_eligible = excluded.supervisor_eligible,
  updated_at = now();
```

## 5. Configurar el frontend
- Abre `src/assets/js/config.js`.
- Completa:

```js
export const DATA_PROVIDER = 'firebase';
export const SUPABASE_URL = 'https://TU-PROYECTO.supabase.co';
export const SUPABASE_ANON_KEY = 'TU-ANON-KEY';
```

- Mantiene `DATA_PROVIDER = 'firebase'` mientras migramos modulos.
- Cuando quieras probar solo autenticacion/perfiles con Supabase, cambia temporalmente a:

```js
export const DATA_PROVIDER = 'supabase';
```

## 6. Flujo recomendado de migracion
- Fase 1: autenticacion + `profiles`
- Fase 2: `roles_matrix` + `user_overrides`
- Fase 3: catalogos maestros:
  - `zones`
  - `dependencies`
  - `cargos`
  - `novedades`
  - `sedes`
- Fase 4: operacion:
  - `employees`
  - `supervisor_profile`
  - `supernumerario_profile`
  - `attendance`
  - `import_history`
  - `import_replacements`
  - `daily_metrics`
  - `daily_closures`
- Fase 5: backend y procesos automaticos

## 7. Instalaciones necesarias

### Opcion minima inmediata
- No necesitas bundler para arrancar.
- El archivo `src/assets/js/supabase.js` usa el cliente web por CDN para comenzar la migracion.

### Opcion recomendada para siguientes fases
- Instala Node.js LTS.
- En la raiz del proyecto:

```bash
npm init -y
npm install -D supabase
```

- Si vas a usar Supabase local:
  - instala Docker Desktop
  - luego inicia sesion y vincula proyecto con la CLI

## 8. Archivos preparados en este repo
- Configuracion base: `src/assets/js/config.js:1`
- Cliente inicial Supabase: `src/assets/js/supabase.js:1`
- Script SQL inicial: `supabase/schema_initial.sql:1`

## 9. Importante antes de cortar Firebase
- Aun no esta migrada la capa completa de datos.
- El esqueleto actual en Supabase cubre solo autenticacion y perfil base.
- No cambies a `DATA_PROVIDER = 'supabase'` en produccion hasta migrar por lo menos:
  - usuarios/perfiles
  - permisos
  - dashboard base
  - modulos criticos de operacion
