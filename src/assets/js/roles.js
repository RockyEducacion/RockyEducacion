export const ROLES = {
  SUPERADMIN: 'superadmin',
  ADMIN: 'admin',
  EDITOR: 'editor',
  CONSULTOR: 'consultor',
  SUPERVISOR: 'supervisor',
  EMPLEADO: 'empleado'
};

export const ALL_ROLES = [ROLES.SUPERADMIN, ROLES.ADMIN, ROLES.EDITOR, ROLES.CONSULTOR, ROLES.SUPERVISOR, ROLES.EMPLEADO];

export const PERMS = {
  MANAGE_PERMISSIONS: 'managePermissions',
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
  VIEW_SUPERVISORS: 'viewSupervisors',
  EDIT_SUPERVISORS: 'editSupervisors',
  VIEW_SUPERNUMERARIOS: 'viewSupernumerarios',
  EDIT_SUPERNUMERARIOS: 'editSupernumerarios',
  VIEW_CARGOS: 'viewCargos',
  EDIT_CARGOS: 'editCargos',
  VIEW_NOVEDADES: 'viewNovedades',
  EDIT_NOVEDADES: 'editNovedades',
  IMPORT_DATA: 'importData',
  VIEW_IMPORT_HISTORY: 'viewImportHistory',
  RUN_PAYROLL: 'runPayroll',
  MANAGE_ABSENTEEISM: 'manageAbsenteeism',
  VIEW_REPORTS: 'viewReports',
  VIEW_REPORTS_CLIENT: 'viewReportsClient',
  VIEW_REPORTS_COMPANY: 'viewReportsCompany',
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
        [PERMS.VIEW_SUPERVISORS]: true,
        [PERMS.EDIT_SUPERVISORS]: true,
        [PERMS.VIEW_SUPERNUMERARIOS]: true,
        [PERMS.EDIT_SUPERNUMERARIOS]: true,
        [PERMS.VIEW_CARGOS]: true,
        [PERMS.EDIT_CARGOS]: true,
        [PERMS.VIEW_NOVEDADES]: true,
        [PERMS.EDIT_NOVEDADES]: true
      };
    case ROLES.EDITOR:
      return {
        ...none,
        [PERMS.IMPORT_DATA]: true,
        [PERMS.VIEW_IMPORT_HISTORY]: true,
        [PERMS.RUN_PAYROLL]: true,
        [PERMS.MANAGE_ABSENTEEISM]: true
      };
    case ROLES.CONSULTOR:
      return {
        ...none,
        [PERMS.VIEW_REPORTS]: true,
        [PERMS.VIEW_REPORTS_CLIENT]: true,
        [PERMS.VIEW_REPORTS_COMPANY]: true
      };
    case ROLES.SUPERVISOR:
      return {
        ...none,
        [PERMS.VIEW_SEDES]: true,
        [PERMS.EDIT_SEDES]: true,
        [PERMS.VIEW_EMPLOYEES]: true,
        [PERMS.EDIT_EMPLOYEES]: true,
        [PERMS.VIEW_SUPERVISORS]: true,
        [PERMS.EDIT_SUPERVISORS]: true,
        [PERMS.UPLOAD_DATA]: true
      };
    case ROLES.EMPLEADO:
      return {
        ...none,
        [PERMS.UPLOAD_DATA]: true
      };
    default:
      return none;
  }
}
