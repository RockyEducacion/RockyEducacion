export const ROLES = {
  SUPERADMIN: 'superadmin',
  ADMIN: 'admin',
  EDITOR: 'editor',
  CONSULTOR: 'consultor',
  SUPERVISOR: 'supervisor',
  TABLET_QR: 'tablet_qr',
  EMPLEADO: 'empleado'
};

export const ROLE_LABELS = {
  [ROLES.SUPERADMIN]: 'SuperAdmin',
  [ROLES.ADMIN]: 'Administrativo',
  [ROLES.EDITOR]: 'Coordinador Operativo',
  [ROLES.CONSULTOR]: 'Supervisor Contrato',
  [ROLES.SUPERVISOR]: 'Supervisor Zona',
  [ROLES.TABLET_QR]: 'Tablet QR',
  [ROLES.EMPLEADO]: 'Empleado'
};

export const ALL_ROLES = [ROLES.SUPERADMIN, ROLES.ADMIN, ROLES.EDITOR, ROLES.CONSULTOR, ROLES.SUPERVISOR, ROLES.TABLET_QR, ROLES.EMPLEADO];

export const PERMS = {
  VIEW_PERMISSIONS: 'viewPermissions',
  MANAGE_PERMISSIONS: 'managePermissions',
  VIEW_AUDIT: 'viewAudit',
  MANAGE_AUDIT: 'manageAudit',
  VIEW_USERS: 'viewUsers',
  EDIT_USERS: 'editUsers',
  VIEW_ZONES: 'viewZones',
  EDIT_ZONES: 'editZones',
  VIEW_DEPENDENCIES: 'viewDependencies',
  EDIT_DEPENDENCIES: 'editDependencies',
  VIEW_SEDES: 'viewSedes',
  EDIT_SEDES: 'editSedes',
  VIEW_EMPLOYEES: 'viewEmployees',
  EDIT_EMPLOYEES: 'editEmployees',
  VIEW_EMPLOYEE_NOVELTIES: 'viewEmployeeNovelties',
  MANAGE_EMPLOYEE_SCHEDULES: 'manageEmployeeSchedules',
  VIEW_SUPERVISORS: 'viewSupervisors',
  EDIT_SUPERVISORS: 'editSupervisors',
  VIEW_SUPERNUMERARIOS: 'viewSupernumerarios',
  EDIT_SUPERNUMERARIOS: 'editSupernumerarios',
  VIEW_CARGOS: 'viewCargos',
  EDIT_CARGOS: 'editCargos',
  VIEW_NOVEDADES: 'viewNovedades',
  EDIT_NOVEDADES: 'editNovedades',
  VIEW_OPERATION_REGISTRY: 'viewOperationRegistry',
  MANAGE_OPERATION_REGISTRY: 'manageOperationRegistry',
  IMPORT_DATA: 'importData',
  VIEW_QR_SCANNER: 'viewQrScanner',
  USE_QR_SCANNER: 'useQrScanner',
  VIEW_QR_DAILY_REGISTRY: 'viewQrDailyRegistry',
  MANAGE_QR_DAILY_REGISTRY: 'manageQrDailyRegistry',
  VIEW_QR_DEVICES: 'viewQrDevices',
  MANAGE_QR_DEVICES: 'manageQrDevices',
  VIEW_IMPORT_HISTORY: 'viewImportHistory',
  MANAGE_IMPORT_HISTORY: 'manageImportHistory',
  MANAGE_ABSENTEEISM: 'manageAbsenteeism',
  VIEW_REPORTS: 'viewReports',
  VIEW_REPORTS_CLIENT: 'viewReportsClient',
  EXPORT_REPORTS_CLIENT: 'exportReportsClient',
  VIEW_REPORTS_QR_HISTORY: 'viewReportsQrHistory',
  EXPORT_REPORTS_QR_HISTORY: 'exportReportsQrHistory',
  VIEW_REPORTS_ABSENTEEISM: 'viewReportsAbsenteeism',
  EXPORT_REPORTS_ABSENTEEISM: 'exportReportsAbsenteeism',
  VIEW_REPORTS_COMPANY: 'viewReportsCompany',
  EXPORT_REPORTS_COMPANY: 'exportReportsCompany',
  VIEW_REPORTS_EMPLOYEES: 'viewReportsEmployees',
  EXPORT_REPORTS_EMPLOYEES: 'exportReportsEmployees',
  VIEW_REPORTS_HIRING: 'viewReportsHiring',
  EXPORT_REPORTS_HIRING: 'exportReportsHiring',
  VIEW_REPORTS_NOVELTIES_CONSOLIDATED: 'viewReportsNoveltiesConsolidated',
  EXPORT_REPORTS_NOVELTIES_CONSOLIDATED: 'exportReportsNoveltiesConsolidated',
  VIEW_REPORTS_SERVICES_CONSOLIDATED: 'viewReportsServicesConsolidated',
  EXPORT_REPORTS_SERVICES_CONSOLIDATED: 'exportReportsServicesConsolidated',
  VIEW_BULK_UPLOAD_SEDES: 'viewBulkUploadSedes',
  BULK_UPLOAD_SEDES: 'bulkUploadSedes',
  VIEW_BULK_UPLOAD_EMPLOYEES: 'viewBulkUploadEmployees',
  BULK_UPLOAD_EMPLOYEES: 'bulkUploadEmployees',
  VIEW_INCAPACITIES: 'viewIncapacities',
  MANAGE_INCAPACITIES: 'manageIncapacities',
  UPLOAD_DATA: 'uploadData'
};

function fullFalsePerms() {
  return Object.fromEntries(Object.values(PERMS).map((k) => [k, false]));
}

export function permsForRole(role) {
  const none = fullFalsePerms();
  switch (role) {
    case ROLES.SUPERADMIN:
      return Object.fromEntries(Object.values(PERMS).map((k) => [k, true]));
    case ROLES.ADMIN:
      return {
        ...none,
        [PERMS.VIEW_USERS]: true,
        [PERMS.EDIT_USERS]: true,
        [PERMS.VIEW_ZONES]: true,
        [PERMS.EDIT_ZONES]: true,
        [PERMS.VIEW_DEPENDENCIES]: true,
        [PERMS.EDIT_DEPENDENCIES]: true,
        [PERMS.VIEW_SEDES]: true,
        [PERMS.EDIT_SEDES]: true,
        [PERMS.VIEW_EMPLOYEES]: true,
        [PERMS.EDIT_EMPLOYEES]: true,
        [PERMS.VIEW_EMPLOYEE_NOVELTIES]: true,
        [PERMS.MANAGE_EMPLOYEE_SCHEDULES]: true,
        [PERMS.VIEW_SUPERVISORS]: true,
        [PERMS.EDIT_SUPERVISORS]: true,
        [PERMS.VIEW_SUPERNUMERARIOS]: true,
        [PERMS.EDIT_SUPERNUMERARIOS]: true,
        [PERMS.VIEW_CARGOS]: true,
        [PERMS.EDIT_CARGOS]: true,
        [PERMS.VIEW_NOVEDADES]: true,
        [PERMS.EDIT_NOVEDADES]: true,
        [PERMS.VIEW_QR_SCANNER]: true,
        [PERMS.USE_QR_SCANNER]: true,
        [PERMS.VIEW_QR_DAILY_REGISTRY]: true,
        [PERMS.MANAGE_QR_DAILY_REGISTRY]: true,
        [PERMS.VIEW_QR_DEVICES]: true,
        [PERMS.MANAGE_QR_DEVICES]: true,
        [PERMS.VIEW_REPORTS_QR_HISTORY]: true,
        [PERMS.EXPORT_REPORTS_QR_HISTORY]: true,
        [PERMS.VIEW_BULK_UPLOAD_SEDES]: true,
        [PERMS.BULK_UPLOAD_SEDES]: true,
        [PERMS.VIEW_BULK_UPLOAD_EMPLOYEES]: true,
        [PERMS.BULK_UPLOAD_EMPLOYEES]: true
      };
    case ROLES.EDITOR:
      return {
        ...none,
        [PERMS.VIEW_OPERATION_REGISTRY]: true,
        [PERMS.MANAGE_OPERATION_REGISTRY]: true,
        [PERMS.IMPORT_DATA]: true,
        [PERMS.VIEW_QR_DAILY_REGISTRY]: true,
        [PERMS.MANAGE_QR_DAILY_REGISTRY]: true,
        [PERMS.VIEW_IMPORT_HISTORY]: true,
        [PERMS.MANAGE_ABSENTEEISM]: true,
        [PERMS.VIEW_REPORTS_QR_HISTORY]: true,
        [PERMS.EXPORT_REPORTS_QR_HISTORY]: true,
        [PERMS.VIEW_REPORTS_ABSENTEEISM]: true,
        [PERMS.EXPORT_REPORTS_ABSENTEEISM]: true
      };
    case ROLES.CONSULTOR:
      return {
        ...none,
        [PERMS.VIEW_REPORTS]: true,
        [PERMS.VIEW_REPORTS_CLIENT]: true,
        [PERMS.EXPORT_REPORTS_CLIENT]: true,
        [PERMS.VIEW_REPORTS_COMPANY]: true,
        [PERMS.EXPORT_REPORTS_COMPANY]: true,
        [PERMS.VIEW_REPORTS_EMPLOYEES]: true,
        [PERMS.EXPORT_REPORTS_EMPLOYEES]: true,
        [PERMS.VIEW_REPORTS_HIRING]: true,
        [PERMS.EXPORT_REPORTS_HIRING]: true,
        [PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED]: true,
        [PERMS.EXPORT_REPORTS_NOVELTIES_CONSOLIDATED]: true,
        [PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED]: true,
        [PERMS.EXPORT_REPORTS_SERVICES_CONSOLIDATED]: true
      };
    case ROLES.SUPERVISOR:
      return {
        ...none,
        [PERMS.VIEW_SEDES]: true,
        [PERMS.EDIT_SEDES]: true,
        [PERMS.VIEW_EMPLOYEES]: true,
        [PERMS.EDIT_EMPLOYEES]: true,
        [PERMS.VIEW_EMPLOYEE_NOVELTIES]: true,
        [PERMS.MANAGE_EMPLOYEE_SCHEDULES]: true,
        [PERMS.VIEW_SUPERVISORS]: true,
        [PERMS.EDIT_SUPERVISORS]: true,
        [PERMS.VIEW_QR_SCANNER]: true,
        [PERMS.USE_QR_SCANNER]: true,
        [PERMS.VIEW_QR_DAILY_REGISTRY]: true,
        [PERMS.MANAGE_QR_DAILY_REGISTRY]: true,
        [PERMS.VIEW_QR_DEVICES]: true,
        [PERMS.MANAGE_QR_DEVICES]: true,
        [PERMS.VIEW_REPORTS_QR_HISTORY]: true,
        [PERMS.EXPORT_REPORTS_QR_HISTORY]: true,
        [PERMS.VIEW_BULK_UPLOAD_SEDES]: true,
        [PERMS.BULK_UPLOAD_SEDES]: true,
        [PERMS.VIEW_BULK_UPLOAD_EMPLOYEES]: true,
        [PERMS.BULK_UPLOAD_EMPLOYEES]: true,
        [PERMS.VIEW_INCAPACITIES]: true,
        [PERMS.MANAGE_INCAPACITIES]: true,
        [PERMS.UPLOAD_DATA]: true
      };
    case ROLES.TABLET_QR:
      return {
        ...none,
        [PERMS.VIEW_QR_SCANNER]: true,
        [PERMS.USE_QR_SCANNER]: true
      };
    case ROLES.EMPLEADO:
      return {
        ...none,
        [PERMS.VIEW_INCAPACITIES]: true,
        [PERMS.MANAGE_INCAPACITIES]: true,
        [PERMS.UPLOAD_DATA]: true
      };
    default:
      return none;
  }
}
