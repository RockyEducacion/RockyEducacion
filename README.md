# RockyEDU

Plataforma de gestion operativa y administrativa para el seguimiento de servicios, personal y novedades.

## Estado actual
- Frontend desplegado en Vercel.
- Autenticacion y datos operando con Supabase/PostgreSQL.
- Backend de WhatsApp desplegado en Vercel.

## Flujo de acceso
- Entrada principal del proyecto: `index.html`
- Ingreso administrativo: `app.html#/login`
- Portal separado para empleados: `employee.html`

## Modulos principales
- Login
- Centro de permisos
- Gestion administrativa
- Operacion
- Consultas y reportes

## Supabase
- Configuracion activa del frontend en `src/assets/js/config.js`
- Cliente principal de datos en `src/assets/js/supabase.js`
- Scripts SQL de migracion en `supabase/`

## Backend WhatsApp
- Backend actual en `whatsapp-backend/`
- Guia de migracion y despliegue en `WHATSAPP_BACKEND_MIGRATION.md`
- Configurar nuevos secretos en `whatsapp-backend/.env` y en Vercel.

## Rutas de la app
- `#/login`
- `#/`
- `#/about`
- `#/notes`
- `#/permissions`
- `#/users`
- `#/zones`
- `#/dependencies`
- `#/sedes`
- `#/employees`
- `#/supervisors`
- `#/registros-vivo`
- `#/imports-replacements`
- `#/import-history`
- `#/payroll`
- `#/absenteeism`
- `#/reports`
- `#/upload`

## Portal de empleados
- Acceso dedicado: `employee.html`
- No usa registro en `Auth`.
- Valida `documento + ultimos 4 del celular` contra `employees`.
- Si el empleado tiene un perfil activo con rol superior (`supervisor`, `admin`, etc.), se redirige al portal principal.
- El backend de este portal vive en `whatsapp-backend/src/app.js`.
- Requiere variables backend en `whatsapp-backend`:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`
  - `EMPLOYEE_PORTAL_ALLOWED_ORIGINS`
  - `EMPLOYEE_PORTAL_SESSION_HOURS`
- El frontend usa `EMPLOYEE_PORTAL_API_BASE` en `src/assets/js/config.js` para apuntar al backend cuando esta en otro dominio.
- Requiere aplicar la migracion `supabase/schema_operations_phase14_employee_portal.sql`.

## Ejecucion local
1. Abrir `index.html` con Live Server.
2. Entrar al centro de accesos desde `access.html`.
3. Elegir `Administrativo` o `Empleados` segun el perfil.
4. Para probar `employee.html`, configurar `EMPLOYEE_PORTAL_API_BASE` hacia el dominio del backend `whatsapp-backend` que expone `/api/employee-*`; Live Server por si solo no sirve esas funciones.

## Documentacion operativa
- Supabase: `SUPABASE_SETUP.md`
- WhatsApp backend: `WHATSAPP_BACKEND_MIGRATION.md`
- Reconexion completa: `RECONNECTION_CHECKLIST.md`
