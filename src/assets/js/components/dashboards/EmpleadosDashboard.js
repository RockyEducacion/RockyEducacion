import { PERMS } from '../../permissions.js';
import { countIncapacitiesToday, countStream, isActive, renderModuleDashboard } from './ModuleDashboardUtils.js';

export const EmpleadosDashboard = (mount, deps = {}) => renderModuleDashboard(mount, deps, {
  title: 'Empleados',
  lead: 'Personas, supervisores, novedades e incapacidades.',
  sectionTitle: 'Gestion de personas',
  className: 'module-dashboard--empleados',
  insights: [
    'Consulta la base de empleados y sus novedades laborales.',
    'Revisa incapacidades del dia antes de operar reemplazos.',
    'Mantiene supervisores y empleados alineados con sedes y zonas.'
  ],
  actions: [
    { label: 'Empleados', route: '/employees', perm: PERMS.VIEW_EMPLOYEES, detail: 'Base de empleados y cambios de estado.' },
    { label: 'Novedades empleados', route: '/employee-novelties', perm: PERMS.VIEW_EMPLOYEES, detail: 'Historial laboral por empleado.' },
    { label: 'Supervisores', route: '/supervisors', perm: PERMS.VIEW_SUPERVISORS, detail: 'Asignacion de zonas.' },
    { label: 'Incapacidades', route: '/upload', perm: PERMS.UPLOAD_DATA, detail: 'Registro manual y consulta de soportes.' }
  ],
  metrics: [
    { label: 'Empleados activos', tone: 'green', load: async (deps) => countStream(deps.streamEmployees, isActive) },
    { label: 'Supervisores', tone: 'blue', load: async (deps) => countStream(deps.streamSupervisors, isActive) },
    { label: 'Incapacidades hoy', tone: 'red', load: async (deps) => countIncapacitiesToday(deps) }
  ]
});
