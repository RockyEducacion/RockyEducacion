# Supabase Setup - Nueva cuenta

## Estado
- El frontend y el backend siguen preparados para Supabase.
- Las credenciales heredadas fueron retiradas del repositorio tracked.
- La base nueva debe ejecutar todas las fases disponibles del repo para habilitar tablas, cierres, auditoria y RPCs operativas.

## Esquemas SQL ya definidos
Ejecutar en este orden:
- `supabase/schema_foundation_phase0.sql:1`
- `supabase/schema_initial.sql:1`
- `supabase/schema_catalogs_phase1.sql:1`
- `supabase/schema_operations_phase2.sql:1`
- `supabase/schema_operations_phase3.sql:1`
- `supabase/schema_whatsapp_phase4.sql:1`
- `supabase/schema_constraints_phase5.sql:1`
- `supabase/schema_governance_phase6.sql:1`
- `supabase/schema_operations_phase6.sql:1`
- `supabase/schema_operations_phase7.sql:1`
- `supabase/schema_operations_phase8.sql:1`
- `supabase/schema_operations_phase9.sql:1`
- `supabase/schema_operations_phase10.sql:1`
- `supabase/schema_operations_phase11.sql:1`
- `supabase/schema_operations_phase12.sql:1`

## Que habilita cada bloque
- `phase0` instala `pgcrypto` para `gen_random_uuid()`.
- `initial` crea perfiles, roles, overrides y RLS base.
- `phase1` crea catalogos: zonas, dependencias, sedes, cargos y novedades.
- `phase2` crea empleados, historial de cargo y supervisores.
- `phase3` crea importaciones, asistencia, ausentismo, metricas y cierres.
- `phase4` crea WhatsApp: incoming, sessions e incapacidades.
- `phase5` agrega indices unicos por fecha/documento.
- `governance phase6` crea `audit_logs` y campos de auditoria en `profiles`.
- `operations 6-8` ajustan `daily_closures` e historial de cargos.
- `phase9` crea `daily_sede_closures`.
- `phase10` crea `employee_daily_status`.
- `phase11` crea RPCs para consolidar `employee_daily_status`.
- `phase12` crea RPCs para recalcular `sede_status` y `daily_metrics`.

## Variables activas del frontend
Configurar en `src/assets/js/config.js:1`:

```js
export const SUPABASE_URL = 'https://TU-PROYECTO.supabase.co';
export const SUPABASE_ANON_KEY = 'TU_SUPABASE_ANON_KEY';
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
- `audit_logs`
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
- `daily_sede_closures`
- `employee_daily_status`
- `whatsapp_incoming`
- `whatsapp_sessions`
- `incapacitados`

## RPCs requeridas por la app y el backend
- `refresh_employee_daily_status`
- `refresh_employee_daily_status_range`
- `recompute_sede_status_from_employee_daily_status`
- `recompute_daily_metrics_from_employee_daily_status`
- `refresh_operational_snapshots_from_employee_daily_status`

## Despues de ejecutar los esquemas
- Crear el primer usuario en Auth.
- Tomar el UUID real del usuario creado y usarlo en `supabase/create_first_superadmin.template.sql`.
- Ejecutar ese script para convertirlo en `superadmin`.
- Validar login web y luego probar el webhook de WhatsApp.
