import { PERMS } from '../../permissions.js';
import { countStream, isActive, renderModuleDashboard } from './ModuleDashboardUtils.js';

export const GobiernoDashboard = (mount, deps = {}) => renderModuleDashboard(mount, deps, {
  title: 'Gobierno',
  lead: 'Permisos, usuarios y control de acceso.',
  sectionTitle: 'Panel de acceso',
  className: 'module-dashboard--gobierno',
  insights: [
    'Supervisa usuarios activos, inactivos y accesos administrativos.',
    'Centraliza la revision de roles antes de tocar modulos operativos.',
    'Usa este panel como punto de entrada para auditorias de permisos.'
  ],
  actions: [
    { label: 'Centro de Permisos', route: '/permissions', perm: 'superadmin', detail: 'Matriz de permisos y excepciones por usuario.' },
    { label: 'Usuarios', route: '/users', perm: PERMS.VIEW_USERS, detail: 'Roles, estado y sincronizacion de accesos.' }
  ],
  metrics: [
    { label: 'Usuarios', tone: 'blue', load: async (deps) => countStream(deps.streamUsers) },
    { label: 'Activos', tone: 'green', load: async (deps) => countStream(deps.streamUsers, isActive) },
    { label: 'Inactivos', tone: 'red', load: async (deps) => countStream(deps.streamUsers, (row) => !isActive(row)) }
  ]
});
