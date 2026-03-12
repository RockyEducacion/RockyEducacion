# Supabase Setup - Estado actual

## Estado
- La ruta activa de datos ya es Supabase.
- El frontend usa `src/assets/js/config.js:1` con `DATA_PROVIDER = 'supabase'`.
- El cliente principal es `src/assets/js/supabase.js:1`.

## Esquemas SQL ya definidos
Ejecutar en este orden:
- `supabase/schema_initial.sql:1`
- `supabase/schema_catalogs_phase1.sql:1`
- `supabase/schema_operations_phase2.sql:1`
- `supabase/schema_operations_phase3.sql:1`
- `supabase/schema_whatsapp_phase4.sql:1`

## Variables activas del frontend
Configurar en `src/assets/js/config.js:1`:

```js
export const DATA_PROVIDER = 'supabase';
export const SUPABASE_URL = 'https://TU-PROYECTO.supabase.co';
export const SUPABASE_ANON_KEY = 'TU-ANON-KEY';
```

## Variables del backend WhatsApp
Configurar en Vercel para el proyecto backend:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `WHATSAPP_VERIFY_TOKEN`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_GRAPH_VERSION`
- `WHATSAPP_APP_SECRET`

## Tablas principales ya usadas
- `profiles`
- `roles_matrix`
- `user_overrides`
- `zones`
- `dependencies`
- `sedes`
- `cargos`
- `novedades`
- `employees`
- `employee_cargo_history`
- `supervisor_profile`
- `attendance`
- `absenteeism`
- `sede_status`
- `import_history`
- `import_replacements`
- `daily_metrics`
- `daily_closures`
- `whatsapp_incoming`
- `whatsapp_sessions`
- `incapacitados`

## Pendiente antes del retiro final de Firebase
- hardening final de produccion
- revision final de permisos y secretos
