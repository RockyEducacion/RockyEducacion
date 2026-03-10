import { getState } from './state.js';
import { ROLES, PERMS, permsForRole } from './roles.js';

const LEGACY_FALLBACK_BY_NEW = {
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
  [PERMS.VIEW_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.VIEW_CARGOS]: 'manageEmployees',
  [PERMS.EDIT_CARGOS]: 'manageEmployees',
  [PERMS.VIEW_NOVEDADES]: 'manageEmployees',
  [PERMS.EDIT_NOVEDADES]: 'manageEmployees',
  [PERMS.VIEW_SUPERVISORS]: 'manageSupervisors',
  [PERMS.EDIT_SUPERVISORS]: 'manageSupervisors'
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
  const overrides = s.userOverrides || {};
  return { ...base, ...overrides };
}

export function can(key) {
  const perms = getEffectivePermissions() || {};
  if (Object.prototype.hasOwnProperty.call(perms, key)) return Boolean(perms[key]);
  const legacyKey = LEGACY_FALLBACK_BY_NEW[key];
  if (legacyKey && Object.prototype.hasOwnProperty.call(perms, legacyKey)) return Boolean(perms[legacyKey]);
  return false;
}

export { PERMS };
