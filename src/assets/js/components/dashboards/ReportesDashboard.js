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
    { label: 'Empleados', route: '/reports-employees', perm: PERMS.VIEW_REPORTS_EMPLOYEES, detail: 'Vigentes por cargo, tipo, zona, dependencia y sede.' },
    { label: 'Contratacion por Sedes', route: '/reports-hiring', perm: PERMS.VIEW_REPORTS_HIRING, detail: 'Planeados, contratados y diferencia por sede.' },
    { label: 'Historico Registro Diario', route: '/reports-daily-history', perm: PERMS.VIEW_REPORTS_CLIENT, detail: 'Consulta por fecha y sede.' },
    { label: 'Historico Registro QR', route: '/reports-qr-history', perm: PERMS.VIEW_REPORTS_QR_HISTORY, detail: 'Consulta ingresos, salidas, pendientes y alertas QR.' },
    { label: 'Ausentismo', route: '/absenteeism', perm: PERMS.VIEW_REPORTS_ABSENTEEISM, detail: 'Analisis de ausencias.' },
    { label: 'Consolidado Novedades', route: '/reports-novelties-consolidated', perm: PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED, detail: 'Novedades por periodo y cobertura.' },
    { label: 'Consolidado Servicios', route: '/reports-services-consolidated', perm: PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED, detail: 'Servicios planeados, cedulas atendidas y ausentismos confirmados.' }
  ],
  metrics: [
    { label: 'Dias del mes', tone: 'blue', load: async () => daysElapsedInMonth() },
    { label: 'Registros metricos', tone: 'green', load: async (deps) => countCurrentMonthMetrics(deps) },
    { label: 'Reportes disponibles', tone: 'indigo', load: async (_, actions) => actions.length }
  ]
});
