import { PERMS } from '../../permissions.js';
import { countStream, isActive, renderModuleDashboard } from './ModuleDashboardUtils.js';

export const CargueMasivoDashboard = (mount, deps = {}) => renderModuleDashboard(mount, deps, {
  title: 'Cargue masivo',
  lead: 'Importaciones controladas para sedes y empleados.',
  sectionTitle: 'Flujo de importacion',
  className: 'module-dashboard--cargue',
  insights: [
    'Inicia cargues solo cuando las plantillas esten validadas.',
    'Gestiona sedes y empleados por rutas separadas para evitar mezclas.',
    'Revisa catalogos maestros antes de importar datos masivos.'
  ],
  actions: [
    { label: 'Cargue sedes', route: '/bulk-upload-sedes', perm: PERMS.VIEW_BULK_UPLOAD_SEDES, detail: 'Plantilla y validacion de sedes.' },
    { label: 'Cargue empleados', route: '/bulk-upload', perm: PERMS.VIEW_BULK_UPLOAD_EMPLOYEES, detail: 'Plantilla y validacion de empleados.' }
  ],
  metrics: [
    { label: 'Sedes activas', tone: 'green', load: async (deps) => countStream(deps.streamSedes, isActive) },
    { label: 'Empleados activos', tone: 'blue', load: async (deps) => countStream(deps.streamEmployees, isActive) },
    { label: 'Rutas de cargue', tone: 'indigo', load: async (_, actions) => actions.length }
  ]
});
