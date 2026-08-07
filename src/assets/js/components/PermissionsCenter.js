import { el } from '../utils/dom.js';
import { isSuperAdmin } from '../permissions.js';
import { ALL_ROLES, ROLE_LABELS, ROLES, PERMS, permsForRole } from '../roles.js';
import { getState } from '../state.js';

const PERM_KEYS = [
  PERMS.MANAGE_PERMISSIONS,
  PERMS.VIEW_USERS,
  PERMS.EDIT_USERS,
  PERMS.VIEW_ZONES,
  PERMS.EDIT_ZONES,
  PERMS.VIEW_DEPENDENCIES,
  PERMS.EDIT_DEPENDENCIES,
  PERMS.VIEW_SEDES,
  PERMS.EDIT_SEDES,
  PERMS.VIEW_EMPLOYEES,
  PERMS.EDIT_EMPLOYEES,
  PERMS.MANAGE_EMPLOYEE_SCHEDULES,
  PERMS.VIEW_SUPERNUMERARIOS,
  PERMS.EDIT_SUPERNUMERARIOS,
  PERMS.VIEW_SUPERVISORS,
  PERMS.EDIT_SUPERVISORS,
  PERMS.VIEW_CARGOS,
  PERMS.EDIT_CARGOS,
  PERMS.VIEW_NOVEDADES,
  PERMS.EDIT_NOVEDADES,
  PERMS.IMPORT_DATA,
  PERMS.VIEW_QR_SCANNER,
  PERMS.VIEW_QR_DAILY_REGISTRY,
  PERMS.MANAGE_QR_DEVICES,
  PERMS.VIEW_IMPORT_HISTORY,
  PERMS.MANAGE_ABSENTEEISM,
  PERMS.VIEW_REPORTS_CLIENT,
  PERMS.VIEW_REPORTS_COMPANY,
  PERMS.UPLOAD_DATA
];

const PERM_LABELS = {
  [PERMS.MANAGE_PERMISSIONS]: 'Gestionar permisos',
  [PERMS.VIEW_USERS]: 'Usuarios - Consulta',
  [PERMS.EDIT_USERS]: 'Usuarios - Edicion',
  [PERMS.VIEW_ZONES]: 'Zonas - Consulta',
  [PERMS.EDIT_ZONES]: 'Zonas - Edicion',
  [PERMS.VIEW_DEPENDENCIES]: 'Dependencias - Consulta',
  [PERMS.EDIT_DEPENDENCIES]: 'Dependencias - Edicion',
  [PERMS.VIEW_SEDES]: 'Sedes - Consulta',
  [PERMS.EDIT_SEDES]: 'Sedes - Edicion',
  [PERMS.VIEW_EMPLOYEES]: 'Empleados - Consulta',
  [PERMS.EDIT_EMPLOYEES]: 'Empleados - Edicion',
  [PERMS.MANAGE_EMPLOYEE_SCHEDULES]: 'Empleados - Programaciones',
  [PERMS.VIEW_SUPERNUMERARIOS]: 'Supernumerarios - Consulta',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'Supernumerarios - Edicion',
  [PERMS.VIEW_SUPERVISORS]: 'Supervisores - Consulta',
  [PERMS.EDIT_SUPERVISORS]: 'Supervisores - Edicion',
  [PERMS.VIEW_CARGOS]: 'Cargos - Consulta',
  [PERMS.EDIT_CARGOS]: 'Cargos - Edicion',
  [PERMS.VIEW_NOVEDADES]: 'Novedades - Consulta',
  [PERMS.EDIT_NOVEDADES]: 'Novedades - Edicion',
  [PERMS.IMPORT_DATA]: 'Operacion - Registro',
  [PERMS.VIEW_QR_SCANNER]: 'QR - Lector',
  [PERMS.VIEW_QR_DAILY_REGISTRY]: 'QR - Registro diario',
  [PERMS.MANAGE_QR_DEVICES]: 'QR - Administrar tablets',
  [PERMS.VIEW_IMPORT_HISTORY]: 'Operacion - Historial',
  [PERMS.MANAGE_ABSENTEEISM]: 'Operacion - Ausentismo',
  [PERMS.VIEW_REPORTS_CLIENT]: 'Reportes - Diarios',
  [PERMS.VIEW_REPORTS_COMPANY]: 'Reportes - Consolidados',
  [PERMS.UPLOAD_DATA]: 'Incapacidades'
};
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
  [PERMS.MANAGE_EMPLOYEE_SCHEDULES]: 'manageEmployees',
  [PERMS.VIEW_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.VIEW_CARGOS]: 'manageEmployees',
  [PERMS.EDIT_CARGOS]: 'manageEmployees',
  [PERMS.VIEW_NOVEDADES]: 'manageEmployees',
  [PERMS.EDIT_NOVEDADES]: 'manageEmployees',
  [PERMS.VIEW_SUPERVISORS]: 'manageSupervisors',
  [PERMS.EDIT_SUPERVISORS]: 'manageSupervisors',
  [PERMS.VIEW_QR_SCANNER]: PERMS.IMPORT_DATA,
  [PERMS.VIEW_QR_DAILY_REGISTRY]: PERMS.IMPORT_DATA,
  [PERMS.MANAGE_QR_DEVICES]: PERMS.EDIT_SEDES,
  [PERMS.VIEW_REPORTS_CLIENT]: PERMS.VIEW_REPORTS,
  [PERMS.VIEW_REPORTS_COMPANY]: PERMS.VIEW_REPORTS
};
function normalizePermissionRecord(raw = {}, role = null) {
  const hasAny = raw && Object.keys(raw).length > 0;
  const base = hasAny ? Object.fromEntries(PERM_KEYS.map((k) => [k, false])) : permsForRole(role || ROLES.ADMIN);
  const out = { ...base };
  PERM_KEYS.forEach((k) => {
    if (Object.prototype.hasOwnProperty.call(raw, k)) out[k] = raw[k] === true;
  });
  Object.entries(LEGACY_FALLBACK_BY_NEW).forEach(([newKey, legacyKey]) => {
    if (Object.prototype.hasOwnProperty.call(raw, newKey)) return;
    if (Object.prototype.hasOwnProperty.call(raw, legacyKey)) out[newKey] = raw[legacyKey] === true;
  });
  return out;
}

export const PermissionsCenter = (mount, deps = {}) => {
  if (!isSuperAdmin()) {
    mount.replaceChildren(
      el('section', { className: 'main-card' }, [
        el('h2', {}, ['Centro de Permisos']),
        el('p', {}, ['Solo SuperAdmin puede administrar permisos.'])
      ])
    );
    return;
  }

  let selectedRole = ROLES.ADMIN;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Centro de Permisos']),
    renderRolesTab()
  ]);

  function renderRolesTab() {
    const panel = el('div', {}, []);
    const roleSel = el(
      'select',
      { className: 'select', style: 'max-width:260px' },
      ALL_ROLES.map((r) => el('option', { value: r, selected: r === selectedRole }, [ROLE_LABELS[r] || r]))
    );
    roleSel.addEventListener('change', () => {
      selectedRole = roleSel.value;
      panel.replaceWith(renderRolesTab());
    });

    const s = getState();
    const matrix = s.roleMatrix || {};
    const computedBase = normalizePermissionRecord(matrix[selectedRole] || {}, selectedRole);
    const original = JSON.parse(JSON.stringify(computedBase));
    const base = JSON.parse(JSON.stringify(computedBase));
    const editingSuperAdmin = selectedRole === ROLES.SUPERADMIN;

    const grid = el(
      'div',
      { className: 'perms-grid mt-2' },
      PERM_KEYS.map((k) => permCheckbox(k, base[k] === true, (ch) => (base[k] = ch), editingSuperAdmin))
    );

    const warnSA = editingSuperAdmin
      ? el('p', { className: 'warn mt-1' }, ['Edicion de SuperAdmin bloqueada (solo lectura).'])
      : null;

    const actions = el('div', { className: 'mt-2' }, [
      el(
        'button',
        {
          className: 'btn btn--primary',
          disabled: editingSuperAdmin,
          onclick: async () => {
            if (editingSuperAdmin) return;
            if (!window.confirm(`Guardar cambios de permisos para el rol "${selectedRole}"?`)) return;
            try {
              await deps.setRolePermissions?.(selectedRole, base);
              await deps.addAuditLog?.({
                targetType: 'role',
                targetId: selectedRole,
                action: 'update_role_matrix',
                before: original,
                after: base
              });
              alert('Permisos del rol actualizados.');
            } catch (e) {
              alert('Error al guardar: ' + (e?.message || e));
            }
          }
        },
        ['Guardar cambios del rol']
      )
    ]);

    panel.replaceChildren(el('label', { className: 'label' }, ['Selecciona un rol']), roleSel, ...[warnSA, grid, actions].filter(Boolean));
    return panel;
  }

  function permCheckbox(key, val, onChange, disabled) {
    const id = `perm_${key}_${Math.random().toString(36).slice(2, 6)}`;
    const label = PERM_LABELS[key] || key;
    const w = el('label', { className: 'perm-item', title: disabled ? 'Solo lectura para SuperAdmin' : '' }, [
      el('input', { type: 'checkbox', id, checked: !!val, disabled: !!disabled }),
      el('span', {}, [label])
    ]);
    if (!disabled) {
      w.querySelector('input').addEventListener('change', (e) => onChange(e.target.checked));
    }
    return w;
  }

  mount.replaceChildren(ui);
  return () => {};
};
