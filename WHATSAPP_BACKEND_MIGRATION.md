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
- `WHATSAPP_VERIFY_TOKEN`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_GRAPH_VERSION`
- `WHATSAPP_APP_SECRET`

## Base de datos requerida
Ejecutar al menos:
- `supabase/schema_whatsapp_phase4.sql:1`
- `supabase/schema_operations_phase3.sql:1`
- `supabase/schema_operations_phase2.sql:1`
- `supabase/schema_catalogs_phase1.sql:1`

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
- `supervisor_profile`
- `incapacitados`

## Pendiente
- endurecer validacion final de firma con `WHATSAPP_APP_SECRET`
- ampliar reglas finas de negocio
- revisar rotacion de secretos expuestos historicamente
