# RockyEDU

Plataforma de gestion operativa y administrativa para el seguimiento de servicios, personal y novedades.

## Estado actual
- Frontend desplegado en Vercel.
- Autenticacion y datos operando con Supabase/PostgreSQL.
- Backend de WhatsApp desplegado en Vercel.

## Flujo de acceso
- Entrada principal del proyecto: `index.html`
- Redireccion a la aplicacion: `app.html#/login`

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

## Ejecucion local
1. Abrir `index.html` con Live Server.
2. Esperar la redireccion automatica a `app.html#/login`.
3. Iniciar sesion y validar modulos segun rol/permisos.

## Documentacion operativa
- Supabase: `SUPABASE_SETUP.md`
- WhatsApp backend: `WHATSAPP_BACKEND_MIGRATION.md`
