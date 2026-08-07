import { PERMS } from '../../permissions.js';
import { dailyMetric, renderModuleDashboard } from './ModuleDashboardUtils.js';

export const OperacionDashboard = (mount, deps = {}) => renderModuleDashboard(mount, deps, {
  title: 'Operacion',
  lead: 'Registro diario, QR, supernumerarios e historial operacional.',
  sectionTitle: 'Pulso operativo',
  className: 'module-dashboard--operacion',
  insights: [
    'Mira planeacion, registros y faltantes del dia en un solo lugar.',
    'Entra al registro diario para gestionar novedades pendientes.',
    'Usa supernumerarios y QR como apoyo directo al control diario.'
  ],
  actions: [
    { label: 'Registro Diario', route: '/registros-vivo', perm: PERMS.IMPORT_DATA, detail: 'Seguimiento y gestion de novedades.' },
    { label: 'Registro QR', route: '/registro-qr', perm: PERMS.VIEW_QR_DAILY_REGISTRY, detail: 'Marcaciones desde tablets.' },
    { label: 'Supernumerarios', route: '/supernumerarios', perm: PERMS.VIEW_SUPERNUMERARIOS, detail: 'Disponibilidad para reemplazos.' },
    { label: 'Registro Sede', route: '/registro-sede', perm: PERMS.IMPORT_DATA, detail: 'Resumen por sede y dependencia.' },
    { label: 'Historial', route: '/import-history', perm: PERMS.VIEW_IMPORT_HISTORY, detail: 'Importaciones y eventos previos.' }
  ],
  metrics: [
    { label: 'Planeados hoy', tone: 'blue', load: async (deps) => dailyMetric(deps, 'planned') },
    { label: 'Registrados hoy', tone: 'green', load: async (deps) => dailyMetric(deps, 'attendanceCount') },
    { label: 'Faltantes hoy', tone: 'red', load: async (deps) => dailyMetric(deps, 'missing') }
  ]
});
