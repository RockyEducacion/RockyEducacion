import { el } from '../utils/dom.js';
import { showActionModal } from '../utils/actionModal.js';
import { showInfoModal } from '../utils/infoModal.js';
import { can, isSuperAdmin } from '../permissions.js';
import { ALL_ROLES, ROLE_LABELS, ROLES, PERMS, permsForRole } from '../roles.js';
import { getState } from '../state.js';

const PERM_KEYS = [
  PERMS.VIEW_PERMISSIONS,
  PERMS.MANAGE_PERMISSIONS,
  PERMS.VIEW_AUDIT,
  PERMS.MANAGE_AUDIT,
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
  PERMS.VIEW_EMPLOYEE_NOVELTIES,
  PERMS.MANAGE_EMPLOYEE_SCHEDULES,
  PERMS.VIEW_SUPERNUMERARIOS,
  PERMS.EDIT_SUPERNUMERARIOS,
  PERMS.VIEW_SUPERVISORS,
  PERMS.EDIT_SUPERVISORS,
  PERMS.VIEW_CARGOS,
  PERMS.EDIT_CARGOS,
  PERMS.VIEW_NOVEDADES,
  PERMS.EDIT_NOVEDADES,
  PERMS.VIEW_OPERATION_REGISTRY,
  PERMS.MANAGE_OPERATION_REGISTRY,
  PERMS.IMPORT_DATA,
  PERMS.VIEW_QR_SCANNER,
  PERMS.USE_QR_SCANNER,
  PERMS.VIEW_QR_DAILY_REGISTRY,
  PERMS.MANAGE_QR_DAILY_REGISTRY,
  PERMS.VIEW_QR_DEVICES,
  PERMS.MANAGE_QR_DEVICES,
  PERMS.VIEW_IMPORT_HISTORY,
  PERMS.MANAGE_IMPORT_HISTORY,
  PERMS.VIEW_REPORTS_CLIENT,
  PERMS.EXPORT_REPORTS_CLIENT,
  PERMS.VIEW_REPORTS_QR_HISTORY,
  PERMS.EXPORT_REPORTS_QR_HISTORY,
  PERMS.VIEW_REPORTS_ABSENTEEISM,
  PERMS.EXPORT_REPORTS_ABSENTEEISM,
  PERMS.VIEW_REPORTS_COMPANY,
  PERMS.EXPORT_REPORTS_COMPANY,
  PERMS.VIEW_REPORTS_EMPLOYEES,
  PERMS.EXPORT_REPORTS_EMPLOYEES,
  PERMS.VIEW_REPORTS_HIRING,
  PERMS.EXPORT_REPORTS_HIRING,
  PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED,
  PERMS.EXPORT_REPORTS_NOVELTIES_CONSOLIDATED,
  PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED,
  PERMS.EXPORT_REPORTS_SERVICES_CONSOLIDATED,
  PERMS.VIEW_BULK_UPLOAD_SEDES,
  PERMS.BULK_UPLOAD_SEDES,
  PERMS.VIEW_BULK_UPLOAD_EMPLOYEES,
  PERMS.BULK_UPLOAD_EMPLOYEES,
  PERMS.VIEW_INCAPACITIES,
  PERMS.MANAGE_INCAPACITIES,
  PERMS.UPLOAD_DATA
];

const PERM_LABELS = {
  [PERMS.VIEW_PERMISSIONS]: 'Permisos - Consulta',
  [PERMS.MANAGE_PERMISSIONS]: 'Gestionar permisos',
  [PERMS.VIEW_AUDIT]: 'Auditoria - Consulta',
  [PERMS.MANAGE_AUDIT]: 'Auditoria - Acciones',
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
  [PERMS.VIEW_EMPLOYEE_NOVELTIES]: 'Novedades empleados - Consulta',
  [PERMS.MANAGE_EMPLOYEE_SCHEDULES]: 'Novedades empleados - Acciones',
  [PERMS.VIEW_SUPERNUMERARIOS]: 'Supernumerarios - Consulta',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'Supernumerarios - Edicion',
  [PERMS.VIEW_SUPERVISORS]: 'Supervisores - Consulta',
  [PERMS.EDIT_SUPERVISORS]: 'Supervisores - Edicion',
  [PERMS.VIEW_CARGOS]: 'Cargos - Consulta',
  [PERMS.EDIT_CARGOS]: 'Cargos - Edicion',
  [PERMS.VIEW_NOVEDADES]: 'Novedades - Consulta',
  [PERMS.EDIT_NOVEDADES]: 'Novedades - Edicion',
  [PERMS.VIEW_OPERATION_REGISTRY]: 'Registro diario/sede - Consulta',
  [PERMS.MANAGE_OPERATION_REGISTRY]: 'Registro diario/sede - Acciones',
  [PERMS.IMPORT_DATA]: 'Operacion - Registro',
  [PERMS.VIEW_QR_SCANNER]: 'Lector QR - Consulta',
  [PERMS.USE_QR_SCANNER]: 'Lector QR - Acciones',
  [PERMS.VIEW_QR_DAILY_REGISTRY]: 'QR - Registro diario',
  [PERMS.MANAGE_QR_DAILY_REGISTRY]: 'QR - Registro diario - Acciones',
  [PERMS.VIEW_QR_DEVICES]: 'Tablets QR - Consulta',
  [PERMS.MANAGE_QR_DEVICES]: 'QR - Administrar tablets',
  [PERMS.VIEW_IMPORT_HISTORY]: 'Operacion - Historial',
  [PERMS.MANAGE_IMPORT_HISTORY]: 'Operacion - Historial - Acciones',
  [PERMS.MANAGE_ABSENTEEISM]: 'Operacion - Ausentismo',
  [PERMS.VIEW_REPORTS_CLIENT]: 'Reportes - Diarios',
  [PERMS.EXPORT_REPORTS_CLIENT]: 'Reportes diarios - Exportar',
  [PERMS.VIEW_REPORTS_QR_HISTORY]: 'Reportes - Historico QR',
  [PERMS.EXPORT_REPORTS_QR_HISTORY]: 'Historico QR - Exportar',
  [PERMS.VIEW_REPORTS_ABSENTEEISM]: 'Reportes - Ausentismo',
  [PERMS.EXPORT_REPORTS_ABSENTEEISM]: 'Ausentismo - Exportar',
  [PERMS.VIEW_REPORTS_COMPANY]: 'Reportes - Consolidados',
  [PERMS.EXPORT_REPORTS_COMPANY]: 'Reportes consolidados - Exportar',
  [PERMS.VIEW_REPORTS_EMPLOYEES]: 'Reporte empleados - Consulta',
  [PERMS.EXPORT_REPORTS_EMPLOYEES]: 'Reporte empleados - Exportar',
  [PERMS.VIEW_REPORTS_HIRING]: 'Contratacion por sedes - Consulta',
  [PERMS.EXPORT_REPORTS_HIRING]: 'Contratacion por sedes - Exportar',
  [PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED]: 'Consolidado novedades - Consulta',
  [PERMS.EXPORT_REPORTS_NOVELTIES_CONSOLIDATED]: 'Consolidado novedades - Exportar',
  [PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED]: 'Consolidado servicios - Consulta',
  [PERMS.EXPORT_REPORTS_SERVICES_CONSOLIDATED]: 'Consolidado servicios - Exportar',
  [PERMS.VIEW_BULK_UPLOAD_SEDES]: 'Cargue masivo sedes - Consulta',
  [PERMS.BULK_UPLOAD_SEDES]: 'Cargue masivo - Sedes',
  [PERMS.VIEW_BULK_UPLOAD_EMPLOYEES]: 'Cargue masivo empleados - Consulta',
  [PERMS.BULK_UPLOAD_EMPLOYEES]: 'Cargue masivo - Empleados',
  [PERMS.VIEW_INCAPACITIES]: 'Incapacidades - Consulta',
  [PERMS.MANAGE_INCAPACITIES]: 'Incapacidades - Acciones',
  [PERMS.UPLOAD_DATA]: 'Incapacidades'
};

const PERMISSION_SECTIONS = [
  {
    title: 'Administracion',
    description: 'Catalogos base, estructura operativa y configuracion QR.',
    items: [
      { label: 'Zonas', view: PERMS.VIEW_ZONES, action: PERMS.EDIT_ZONES, actionLabel: 'Edicion' },
      { label: 'Dependencias', view: PERMS.VIEW_DEPENDENCIES, action: PERMS.EDIT_DEPENDENCIES, actionLabel: 'Edicion' },
      { label: 'Sedes', view: PERMS.VIEW_SEDES, action: PERMS.EDIT_SEDES, actionLabel: 'Edicion' },
      { label: 'Lector QR', view: PERMS.VIEW_QR_SCANNER, action: PERMS.USE_QR_SCANNER, actionLabel: 'Usar lector' },
      { label: 'Tablets QR', view: PERMS.VIEW_QR_DEVICES, action: PERMS.MANAGE_QR_DEVICES, actionLabel: 'Administrar' },
      { label: 'Cargos', view: PERMS.VIEW_CARGOS, action: PERMS.EDIT_CARGOS, actionLabel: 'Edicion' },
      { label: 'Novedades', view: PERMS.VIEW_NOVEDADES, action: PERMS.EDIT_NOVEDADES, actionLabel: 'Edicion' }
    ]
  },
  {
    title: 'Empleados',
    description: 'Base de personal, historial laboral, supervisores e incapacidades.',
    items: [
      { label: 'Empleados', view: PERMS.VIEW_EMPLOYEES, action: PERMS.EDIT_EMPLOYEES, actionLabel: 'Edicion' },
      { label: 'Novedades empleados', view: PERMS.VIEW_EMPLOYEE_NOVELTIES, action: PERMS.MANAGE_EMPLOYEE_SCHEDULES, actionLabel: 'Programar' },
      { label: 'Supervisores', view: PERMS.VIEW_SUPERVISORS, action: PERMS.EDIT_SUPERVISORS, actionLabel: 'Edicion' },
      { label: 'Incapacidades', view: PERMS.VIEW_INCAPACITIES, action: PERMS.MANAGE_INCAPACITIES, actionLabel: 'Gestionar' }
    ]
  },
  {
    title: 'Operacion',
    description: 'Registro diario, QR operativo, supernumerarios, historial y ausentismo.',
    items: [
      { label: 'Registro diario y sede', view: PERMS.VIEW_OPERATION_REGISTRY, action: PERMS.MANAGE_OPERATION_REGISTRY, actionLabel: 'Gestionar' },
      { label: 'Registro QR', view: PERMS.VIEW_QR_DAILY_REGISTRY, action: PERMS.MANAGE_QR_DAILY_REGISTRY, actionLabel: 'Gestionar' },
      { label: 'Supernumerarios', view: PERMS.VIEW_SUPERNUMERARIOS, action: PERMS.EDIT_SUPERNUMERARIOS, actionLabel: 'Edicion' },
      { label: 'Historial', view: PERMS.VIEW_IMPORT_HISTORY, action: PERMS.MANAGE_IMPORT_HISTORY, actionLabel: 'Acciones' }
    ]
  },
  {
    title: 'Reportes',
    description: 'Historicos diarios, QR, empleados, contratacion y consolidados.',
    items: [
      { label: 'Historico Registro Diario', view: PERMS.VIEW_REPORTS_CLIENT, action: PERMS.EXPORT_REPORTS_CLIENT, actionLabel: 'Exportar' },
      { label: 'Historico Registro QR', view: PERMS.VIEW_REPORTS_QR_HISTORY, action: PERMS.EXPORT_REPORTS_QR_HISTORY, actionLabel: 'Exportar' },
      { label: 'Ausentismo', view: PERMS.VIEW_REPORTS_ABSENTEEISM, action: PERMS.EXPORT_REPORTS_ABSENTEEISM, actionLabel: 'Exportar' },
      { label: 'Empleados', view: PERMS.VIEW_REPORTS_EMPLOYEES, action: PERMS.EXPORT_REPORTS_EMPLOYEES, actionLabel: 'Exportar' },
      { label: 'Contratacion por Sedes', view: PERMS.VIEW_REPORTS_HIRING, action: PERMS.EXPORT_REPORTS_HIRING, actionLabel: 'Exportar' },
      { label: 'Consolidado Novedades', view: PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED, action: PERMS.EXPORT_REPORTS_NOVELTIES_CONSOLIDATED, actionLabel: 'Exportar' },
      { label: 'Consolidado Servicios', view: PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED, action: PERMS.EXPORT_REPORTS_SERVICES_CONSOLIDATED, actionLabel: 'Exportar' }
    ]
  },
  {
    title: 'Cargue masivo',
    description: 'Plantillas y cargues masivos de sedes y empleados.',
    items: [
      { label: 'Cargue sedes', view: PERMS.VIEW_BULK_UPLOAD_SEDES, action: PERMS.BULK_UPLOAD_SEDES, actionLabel: 'Importar' },
      { label: 'Cargue empleados', view: PERMS.VIEW_BULK_UPLOAD_EMPLOYEES, action: PERMS.BULK_UPLOAD_EMPLOYEES, actionLabel: 'Importar' }
    ]
  },
  {
    title: 'Gobierno',
    description: 'Usuarios, auditoria y administracion de permisos.',
    items: [
      { label: 'Centro de permisos', view: PERMS.VIEW_PERMISSIONS, action: PERMS.MANAGE_PERMISSIONS, actionLabel: 'Editar' },
      { label: 'Auditoria', view: PERMS.VIEW_AUDIT, action: PERMS.MANAGE_AUDIT, actionLabel: 'Acciones' },
      { label: 'Usuarios', view: PERMS.VIEW_USERS, action: PERMS.EDIT_USERS, actionLabel: 'Edicion' }
    ]
  }
];
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
    else if (out[legacyKey] === true) out[newKey] = true;
  });
  return out;
}

export const PermissionsCenter = (mount, deps = {}) => {
  const canViewPermissions = isSuperAdmin() || can(PERMS.VIEW_PERMISSIONS);
  const canManagePermissions = isSuperAdmin() || can(PERMS.MANAGE_PERMISSIONS);
  if (!canViewPermissions) {
    mount.replaceChildren(
      el('section', { className: 'main-card' }, [
        el('h2', {}, ['Centro de Permisos']),
        el('p', {}, ['No tienes permiso para consultar permisos.'])
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
    const readOnly = editingSuperAdmin || !canManagePermissions;

    const groups = el(
      'div',
      { className: 'permissions-center__modules' },
      PERMISSION_SECTIONS.map((sectionDef, index) => permissionSection(sectionDef, index, base, readOnly))
    );

    const warnSA = editingSuperAdmin
      ? el('p', { className: 'warn mt-1' }, ['Edicion de SuperAdmin bloqueada (solo lectura).'])
      : !canManagePermissions
        ? el('p', { className: 'warn mt-1' }, ['Modo consulta: no tienes permiso para guardar cambios.'])
      : null;

    const actions = el('div', { className: 'mt-2' }, [
      el(
        'button',
        {
          className: 'btn btn--primary',
          disabled: readOnly,
          onclick: async () => {
            if (readOnly) return;
            const roleLabel = ROLE_LABELS[selectedRole] || selectedRole;
            const modal = await showActionModal({
              title: 'Guardar permisos del rol',
              message: `Vas a guardar los cambios de permisos para el rol "${roleLabel}".`,
              confirmText: 'Guardar cambios',
              cancelText: 'Cancelar'
            });
            if (!modal?.confirmed) return;
            try {
              await deps.setRolePermissions?.(selectedRole, base);
              await deps.addAuditLog?.({
                targetType: 'role',
                targetId: selectedRole,
                action: 'update_role_matrix',
                before: original,
                after: base
              });
              showInfoModal('Permisos actualizados', [
                `Los permisos del rol "${roleLabel}" se guardaron correctamente.`
              ]);
            } catch (e) {
              showInfoModal('No fue posible guardar', [
                String(e?.message || e || 'Error desconocido.')
              ]);
            }
          }
        },
        ['Guardar cambios del rol']
      )
    ]);

    panel.replaceChildren(el('label', { className: 'label' }, ['Selecciona un rol']), roleSel, ...[warnSA, groups, actions].filter(Boolean));
    return panel;
  }

  function permissionSection(sectionDef, index, base, disabled) {
    const items = (sectionDef.items || []).map(normalizePermissionPair);
    const activeCount = items.reduce((acc, item) => acc + (base[item.view] === true ? 1 : 0) + (base[item.action] === true ? 1 : 0), 0);
    const totalCount = items.length * 2;
    return el('section', { className: 'permissions-center__module', 'data-permission-section-index': String(index) }, [
      el('div', { className: 'permissions-center__module-head' }, [
        el('div', {}, [
          el('h3', { className: 'permissions-center__module-title' }, [sectionDef.title]),
          el('p', { className: 'permissions-center__module-description' }, [sectionDef.description || ''])
        ]),
        el('span', { className: 'badge permissions-center__module-count' }, [`${activeCount}/${totalCount}`])
      ]),
      el(
        'div',
        { className: 'permission-pairs' },
        items.map((item) => permissionPair(item, base, (key, ch) => {
          base[key] = ch;
          syncPermissionCheckboxes(key, ch);
          if (key === item.action && ch && base[item.view] !== true) {
            base[item.view] = true;
            syncPermissionCheckboxes(item.view, true);
          }
          if (key === item.view && !ch && base[item.action] === true) {
            base[item.action] = false;
            syncPermissionCheckboxes(item.action, false);
          }
          syncPermissionSectionCounts(base);
        }, disabled))
      )
    ]);
  }

  function normalizePermissionPair(item) {
    return {
      label: item.label || 'Permiso',
      view: item.view,
      action: item.action,
      viewLabel: item.viewLabel || 'Consulta',
      actionLabel: item.actionLabel || 'Accion'
    };
  }

  function syncPermissionCheckboxes(key, checked) {
    ui.querySelectorAll(`input[data-perm-key="${key}"]`).forEach((input) => {
      input.checked = checked;
    });
  }

  function syncPermissionSectionCounts(base) {
    ui.querySelectorAll('[data-permission-section-index]').forEach((node) => {
      const index = Number(node.getAttribute('data-permission-section-index'));
      const sectionDef = PERMISSION_SECTIONS[index];
      if (!sectionDef) return;
      const items = (sectionDef.items || []).map(normalizePermissionPair);
      const activeCount = items.reduce((acc, item) => acc + (base[item.view] === true ? 1 : 0) + (base[item.action] === true ? 1 : 0), 0);
      const count = node.querySelector('.permissions-center__module-count');
      if (count) count.textContent = `${activeCount}/${items.length * 2}`;
    });
  }

  function permissionPair(item, base, onChange, disabled) {
    return el('article', { className: 'permission-pair' }, [
      el('div', { className: 'permission-pair__title' }, [item.label]),
      el('div', { className: 'permission-pair__checks' }, [
        permCheckbox({ key: item.view, label: item.viewLabel }, base[item.view] === true, (checked) => onChange(item.view, checked), disabled),
        permCheckbox({ key: item.action, label: item.actionLabel }, base[item.action] === true, (checked) => onChange(item.action, checked), disabled)
      ])
    ]);
  }

  function permCheckbox(item, val, onChange, disabled) {
    const key = item.key;
    const id = `perm_${key}_${Math.random().toString(36).slice(2, 6)}`;
    const label = item.label || PERM_LABELS[key] || key;
    const w = el('label', { className: 'perm-item', title: disabled ? 'Solo lectura' : '' }, [
      el('input', { type: 'checkbox', id, checked: !!val, disabled: !!disabled, 'data-perm-key': key }),
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
