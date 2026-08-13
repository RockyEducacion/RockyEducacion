import { PERMS } from '../../permissions.js';
import { countStream, isActive, renderModuleDashboard } from './ModuleDashboardUtils.js';

export const AdministracionDashboard = (mount, deps = {}) => renderModuleDashboard(mount, deps, {
  title: 'Administracion',
  lead: 'Catalogos maestros que sostienen la operacion diaria.',
  sectionTitle: 'Catalogos disponibles',
  className: 'module-dashboard--administracion',
  insights: [
    'Mantiene alineadas sedes, zonas, dependencias, cargos y novedades.',
    'Prioriza ajustes de catalogo antes de cargues o cierres operativos.',
    'Revisa que las novedades activas coincidan con las reglas del negocio.'
  ],
  actions: [
    { label: 'Zonas', route: '/zones', perm: PERMS.VIEW_ZONES, detail: 'Gestion territorial.' },
    { label: 'Dependencias', route: '/dependencies', perm: PERMS.VIEW_DEPENDENCIES, detail: 'Estructura organizacional.' },
    { label: 'Sedes', route: '/sedes', perm: PERMS.VIEW_SEDES, detail: 'Puntos de operacion y QR.' },
    { label: 'Cargos', route: '/cargos', perm: PERMS.VIEW_CARGOS, detail: 'Cargos y salarios.' },
    { label: 'Novedades', route: '/novedades', perm: PERMS.VIEW_NOVEDADES, detail: 'Tipos de novedades y reglas.' },
    { label: 'Tablets QR', route: '/tablets-qr', perm: PERMS.VIEW_QR_DEVICES, detail: 'Dispositivos registrados.' }
  ],
  metrics: [
    { label: 'Sedes activas', tone: 'green', load: async (deps) => countStream(deps.streamSedes, isActive) },
    { label: 'Cargos activos', tone: 'blue', load: async (deps) => countStream(deps.streamCargos, isActive) },
    { label: 'Novedades activas', tone: 'indigo', load: async (deps) => countStream(deps.streamNovedades, isActive) }
  ]
});
