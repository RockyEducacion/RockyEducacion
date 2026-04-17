# Backend WhatsApp - Estado actual

## Estado
- El webhook ya opera fuera de Firebase.
- El backend activo está en `whatsapp-backend/`.
- El despliegue objetivo es Vercel.

## Backend actual
Archivos principales:
- `whatsapp-backend/src/app.js:1`
- `whatsapp-backend/api/index.js:1`
- `whatsapp-backend/vercel.json:1`
- `whatsapp-backend/package.json:1`

## Variables requeridas
Definir en Vercel para el proyecto backend:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `CRON_SECRET`
- `EMPLOYEE_PORTAL_ALLOWED_ORIGINS`
- `EMPLOYEE_PORTAL_SESSION_HOURS`
- `WHATSAPP_VERIFY_TOKEN`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_GRAPH_VERSION`
- `WHATSAPP_APP_SECRET`

Tambien actualiza localmente `whatsapp-backend/.env` con esos mismos valores.

## Base de datos requerida
Ejecutar en una base nueva:
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
- `supabase/schema_operations_phase13.sql:1`
- `supabase/schema_operations_phase14_employee_portal.sql:1`

## Flujo ya operativo
- verificacion del webhook de Meta
- recepcion de mensajes
- escritura en `whatsapp_incoming`
- manejo de `whatsapp_sessions`
- saludo inicial con `hola`
- identificacion por documento
- menu por rol
- registro:
  - `TRABAJANDO`
  - `COMPENSATORIO`
  - `NOVEDAD`
- `ACTUALIZAR DATOS`
  - cambio de telefono
  - traslado de sede
- incapacidades con fechas

## Tablas ya usadas por el backend
- `whatsapp_incoming`
- `whatsapp_sessions`
- `employees`
- `cargos`
- `sedes`
- `attendance`
- `absenteeism`
- `daily_metrics`
- `daily_sede_closures`
- `employee_daily_status`
- `audit_logs`
- `supervisor_profile`
- `incapacitados`