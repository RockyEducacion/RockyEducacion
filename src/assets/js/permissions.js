import { getState } from './state.js';
import { ROLES, PERMS, permsForRole } from './roles.js';

const LEGACY_FALLBACK_BY_NEW = {
  [PERMS.VIEW_PERMISSIONS]: PERMS.MANAGE_PERMISSIONS,
  [PERMS.VIEW_AUDIT]: PERMS.MANAGE_PERMISSIONS,
  [PERMS.MANAGE_AUDIT]: PERMS.MANAGE_PERMISSIONS,
  [PERMS.VIEW_USERS]: 'manageUsers',
  [PERMS.EDIT_USERS]: 'manageUsers',
  [PERMS.VIEW_ZONES]: 'manageZones',
  [PERMS.EDIT_ZONES]: 'manageZones',
  [PERMS.VIEW_DEPENDENCIES]: 'manageDependencies',
  [PERMS.EDIT_DEPENDENCIES]: 'manageDependencies',
  [PERMS.VIEW_SEDES]: 'manageSedes',
  [PERMS.EDIT_SEDES]: 'manageSedes',
  [PERMS.VIEW_EMPLOYEES]: 'manageEmployees',
  [PERMS.EDIT_EMPLOYEES]: 'manageEmployees',
  [PERMS.VIEW_EMPLOYEE_NOVELTIES]: PERMS.VIEW_EMPLOYEES,
  [PERMS.MANAGE_EMPLOYEE_SCHEDULES]: 'manageEmployees',
  [PERMS.VIEW_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.VIEW_CARGOS]: 'manageEmployees',
  [PERMS.EDIT_CARGOS]: 'manageEmployees',
  [PERMS.VIEW_NOVEDADES]: 'manageEmployees',
  [PERMS.EDIT_NOVEDADES]: 'manageEmployees',
  [PERMS.VIEW_SUPERVISORS]: 'manageSupervisors',
  [PERMS.EDIT_SUPERVISORS]: 'manageSupervisors',
  [PERMS.VIEW_OPERATION_REGISTRY]: PERMS.IMPORT_DATA,
  [PERMS.MANAGE_OPERATION_REGISTRY]: PERMS.IMPORT_DATA,
  [PERMS.VIEW_QR_SCANNER]: PERMS.IMPORT_DATA,
  [PERMS.USE_QR_SCANNER]: PERMS.VIEW_QR_SCANNER,
  [PERMS.VIEW_QR_DAILY_REGISTRY]: PERMS.IMPORT_DATA,
  [PERMS.MANAGE_QR_DAILY_REGISTRY]: PERMS.VIEW_QR_DAILY_REGISTRY,
  [PERMS.VIEW_QR_DEVICES]: PERMS.MANAGE_QR_DEVICES,
  [PERMS.MANAGE_QR_DEVICES]: PERMS.EDIT_SEDES,
  [PERMS.MANAGE_IMPORT_HISTORY]: PERMS.VIEW_IMPORT_HISTORY,
  [PERMS.VIEW_REPORTS_QR_HISTORY]: PERMS.VIEW_QR_DAILY_REGISTRY,
  [PERMS.VIEW_REPORTS_ABSENTEEISM]: PERMS.MANAGE_ABSENTEEISM,
  [PERMS.VIEW_REPORTS_CLIENT]: PERMS.VIEW_REPORTS,
  [PERMS.EXPORT_REPORTS_CLIENT]: PERMS.VIEW_REPORTS_CLIENT,
  [PERMS.EXPORT_REPORTS_QR_HISTORY]: PERMS.VIEW_REPORTS_QR_HISTORY,
  [PERMS.EXPORT_REPORTS_ABSENTEEISM]: PERMS.VIEW_REPORTS_ABSENTEEISM,
  [PERMS.VIEW_REPORTS_COMPANY]: PERMS.VIEW_REPORTS,
  [PERMS.EXPORT_REPORTS_COMPANY]: PERMS.VIEW_REPORTS_COMPANY,
  [PERMS.VIEW_REPORTS_EMPLOYEES]: PERMS.VIEW_REPORTS_COMPANY,
  [PERMS.EXPORT_REPORTS_EMPLOYEES]: PERMS.EXPORT_REPORTS_COMPANY,
  [PERMS.VIEW_REPORTS_HIRING]: PERMS.VIEW_REPORTS_COMPANY,
  [PERMS.EXPORT_REPORTS_HIRING]: PERMS.EXPORT_REPORTS_COMPANY,
  [PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED]: PERMS.VIEW_REPORTS_COMPANY,
  [PERMS.EXPORT_REPORTS_NOVELTIES_CONSOLIDATED]: PERMS.EXPORT_REPORTS_COMPANY,
  [PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED]: PERMS.VIEW_REPORTS_COMPANY,
  [PERMS.EXPORT_REPORTS_SERVICES_CONSOLIDATED]: PERMS.EXPORT_REPORTS_COMPANY,
  [PERMS.VIEW_BULK_UPLOAD_SEDES]: PERMS.EDIT_SEDES,
  [PERMS.BULK_UPLOAD_SEDES]: PERMS.EDIT_SEDES,
  [PERMS.VIEW_BULK_UPLOAD_EMPLOYEES]: PERMS.EDIT_EMPLOYEES,
  [PERMS.BULK_UPLOAD_EMPLOYEES]: PERMS.EDIT_EMPLOYEES,
  [PERMS.VIEW_INCAPACITIES]: PERMS.UPLOAD_DATA,
  [PERMS.MANAGE_INCAPACITIES]: PERMS.UPLOAD_DATA
};

export function getRole() {
  return getState().userProfile?.role ?? null;
}

export function isSuperAdmin() {
  return getRole() === ROLES.SUPERADMIN;
}

export function getEffectivePermissions() {
  const s = getState();
  if (!s.user || !s.userProfile?.role) return {};
  if (isSuperAdmin()) return Object.fromEntries(Object.values(PERMS).map((k) => [k, true]));
  const role = s.userProfile.role;
  if (role === ROLES.SUPERVISOR && s.userProfile?.supervisorEligible !== true) return {};
  const matrix = s.roleMatrix?.[role];
  const base = { ...permsForRole(role), ...(matrix || {}) };
  if (matrix && typeof matrix === 'object') {
    Object.entries(LEGACY_FALLBACK_BY_NEW).forEach(([newKey, legacyKey]) => {
      if (Object.prototype.hasOwnProperty.call(matrix, newKey)) return;
      if (Object.prototype.hasOwnProperty.call(matrix, legacyKey)) base[newKey] = matrix[legacyKey] === true;
      else if (base[legacyKey] === true) base[newKey] = true;
    });
  }
  const overrides = s.userOverrides || {};
  const merged = { ...base, ...overrides };
  Object.entries(LEGACY_FALLBACK_BY_NEW).forEach(([newKey, legacyKey]) => {
    if (Object.prototype.hasOwnProperty.call(overrides, newKey)) return;
    if (Object.prototype.hasOwnProperty.call(overrides, legacyKey)) merged[newKey] = overrides[legacyKey] === true;
    else if (merged[legacyKey] === true) merged[newKey] = true;
  });
  return merged;
}

export function can(key) {
  const perms = getEffectivePermissions() || {};
  if (Object.prototype.hasOwnProperty.call(perms, key)) return Boolean(perms[key]);
  const legacyKey = LEGACY_FALLBACK_BY_NEW[key];
  if (legacyKey && Object.prototype.hasOwnProperty.call(perms, legacyKey)) return Boolean(perms[legacyKey]);
  return false;
}

export { PERMS };
