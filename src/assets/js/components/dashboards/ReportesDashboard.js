import { PERMS } from '../../permissions.js';
import { countCurrentMonthMetrics, daysElapsedInMonth, renderModuleDashboard } from './ModuleDashboardUtils.js';

export const ReportesDashboard = (mount, deps = {}) => renderModuleDashboard(mount, deps, {
  title: 'Reportes',
  lead: 'Consultas historicas y reportes consolidados.',
  sectionTitle: 'Centro de analisis',
  className: 'module-dashboard--reportes',
  insights: [
    'Accede rapido a historicos, ausentismo y consolidados.',
    'Valida tendencias del mes antes de exportar informacion.',
    'Separa consultas diarias de reportes institucionales.'
  ],
  actions: [
    { label: 'Historico Registro Diario', route: '/reports-daily-history', perm: PERMS.VIEW_REPORTS_CLIENT, detail: 'Consulta por fecha y sede.' },
    { label: 'Ausentismo', route: '/absenteeism', perm: PERMS.MANAGE_ABSENTEEISM, detail: 'Analisis de ausencias.' },
    { label: 'Reportes consolidados', route: '/reports-consolidated', perm: PERMS.VIEW_REPORTS_COMPANY, detail: 'Exportables institucionales.' }
  ],
  metrics: [
    { label: 'Dias del mes', tone: 'blue', load: async () => daysElapsedInMonth() },
    { label: 'Registros metricos', tone: 'green', load: async (deps) => countCurrentMonthMetrics(deps) },
    { label: 'Reportes disponibles', tone: 'indigo', load: async (_, actions) => actions.length }
  ]
});
