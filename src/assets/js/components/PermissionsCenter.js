import { el, qs } from '../utils/dom.js';
import { isSuperAdmin } from '../permissions.js';
import { ALL_ROLES, ROLES, PERMS, permsForRole } from '../roles.js';
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
  PERMS.VIEW_SUPERNUMERARIOS,
  PERMS.EDIT_SUPERNUMERARIOS,
  PERMS.VIEW_SUPERVISORS,
  PERMS.EDIT_SUPERVISORS,
  PERMS.VIEW_CARGOS,
  PERMS.EDIT_CARGOS,
  PERMS.VIEW_NOVEDADES,
  PERMS.EDIT_NOVEDADES,
  PERMS.IMPORT_DATA,
  PERMS.VIEW_IMPORT_HISTORY,
  PERMS.RUN_PAYROLL,
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
  [PERMS.VIEW_SUPERNUMERARIOS]: 'Supernumerarios - Consulta',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'Supernumerarios - Edicion',
  [PERMS.VIEW_SUPERVISORS]: 'Supervisores - Consulta',
  [PERMS.EDIT_SUPERVISORS]: 'Supervisores - Edicion',
  [PERMS.VIEW_CARGOS]: 'Cargos - Consulta',
  [PERMS.EDIT_CARGOS]: 'Cargos - Edicion',
  [PERMS.VIEW_NOVEDADES]: 'Novedades - Consulta',
  [PERMS.EDIT_NOVEDADES]: 'Novedades - Edicion',
  [PERMS.IMPORT_DATA]: 'Operacion - Registro',
  [PERMS.VIEW_IMPORT_HISTORY]: 'Operacion - Historial',
  [PERMS.RUN_PAYROLL]: 'Operacion - Nomina',
  [PERMS.MANAGE_ABSENTEEISM]: 'Operacion - Ausentismo',
  [PERMS.VIEW_REPORTS_CLIENT]: 'Reportes - Cliente',
  [PERMS.VIEW_REPORTS_COMPANY]: 'Reportes - Empresa',
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
  [PERMS.VIEW_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.EDIT_SUPERNUMERARIOS]: 'manageEmployees',
  [PERMS.VIEW_CARGOS]: 'manageEmployees',
  [PERMS.EDIT_CARGOS]: 'manageEmployees',
  [PERMS.VIEW_NOVEDADES]: 'manageEmployees',
  [PERMS.EDIT_NOVEDADES]: 'manageEmployees',
  [PERMS.VIEW_SUPERVISORS]: 'manageSupervisors',
  [PERMS.EDIT_SUPERVISORS]: 'manageSupervisors',
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

  let unAudit = null;
  let currentTab = 'roles';
  let selectedRole = ROLES.ADMIN;
  let userTarget = null;
  let userOverrides = {};
  let originalOverrides = {};

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Centro de Permisos']),
    el('div', { className: 'tabs' }, [tabBtn('Por rol', 'roles'), tabBtn('Por usuario', 'users'), tabBtn('Auditoria', 'audit')]),
    el('div', { id: 'tabContent', className: 'mt-2' }, [])
  ]);

  function tabBtn(text, key) {
    const b = el('button', { className: 'tab' + (currentTab === key ? ' is-active' : '') }, [text]);
    b.addEventListener('click', () => {
      currentTab = key;
      renderTab();
    });
    return b;
  }

  function clearAuditStream() {
    if (typeof unAudit === 'function') {
      try {
        unAudit();
      } catch {}
      unAudit = null;
    }
  }

  function updateTabUi() {
    const idx = { roles: 0, users: 1, audit: 2 }[currentTab];
    ui.querySelectorAll('.tab').forEach((b) => b.classList.remove('is-active'));
    ui.querySelectorAll('.tab')[idx]?.classList.add('is-active');
  }

  function renderTab() {
    if (currentTab !== 'audit') clearAuditStream();
    const c = qs('#tabContent', ui);
    if (currentTab === 'roles') c.replaceChildren(renderRolesTab());
    else if (currentTab === 'users') c.replaceChildren(renderUsersTab());
    else c.replaceChildren(renderAuditTab());
    updateTabUi();
  }

  function renderRolesTab() {
    const roleSel = el(
      'select',
      { className: 'select', style: 'max-width:260px' },
      ALL_ROLES.map((r) => el('option', { value: r, selected: r === selectedRole }, [r]))
    );
    roleSel.addEventListener('change', () => {
      selectedRole = roleSel.value;
      renderTab();
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

    return el('div', {}, [el('label', { className: 'label' }, ['Selecciona un rol']), roleSel, warnSA, grid, actions].filter(Boolean));
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

  function renderUsersTab() {
    const emailInput = el('input', { className: 'input', placeholder: 'Correo del usuario' });
    const btn = el('button', { className: 'btn mt-1' }, ['Cargar usuario']);
    const box = el('div', { className: 'mt-2' }, []);
    btn.addEventListener('click', async () => {
      try {
        const res = await deps.findUserByEmail?.(emailInput.value.trim());
        if (!res) {
          box.replaceChildren(el('p', { className: 'error' }, ['Usuario no encontrado']));
          return;
        }
        userTarget = res;
        const ov = (await deps.getUserOverrides?.(userTarget.uid)) || {};
        userOverrides = normalizePermissionRecord(ov, null);
        originalOverrides = JSON.parse(JSON.stringify(userOverrides));
        box.replaceChildren(renderUserOverrides());
      } catch (e) {
        box.replaceChildren(el('p', { className: 'error' }, ['Error: ', e?.message || e]));
      }
    });
    return el('div', {}, [el('label', { className: 'label' }, ['Buscar usuario por correo']), emailInput, btn, box]);
  }

  function renderUserOverrides() {
    const current = JSON.parse(JSON.stringify(userOverrides));
    const grid = el(
      'div',
      { className: 'perms-grid mt-2' },
      PERM_KEYS.map((k) => permCheckbox(k, current[k] === true, (ch) => (current[k] = ch), false))
    );
    const info = el('p', { className: 'text-muted mt-1' }, [`Usuario: ${userTarget.email} (${userTarget.displayName || '-'})`]);
    const actions = el('div', { className: 'mt-2' }, [
      el(
        'button',
        {
          className: 'btn btn--primary',
          onclick: async () => {
            if (!window.confirm(`Guardar overrides para ${userTarget.email}?`)) return;
            try {
              const before = originalOverrides;
              const after = current;
              await deps.setUserOverrides?.(userTarget.uid, after);
              await deps.addAuditLog?.({
                targetType: 'user',
                targetId: userTarget.uid,
                action: 'update_user_overrides',
                before,
                after
              });
              originalOverrides = JSON.parse(JSON.stringify(after));
              userOverrides = JSON.parse(JSON.stringify(after));
              alert('Overrides guardados.');
            } catch (e) {
              alert('Error: ' + (e?.message || e));
            }
          }
        },
        ['Guardar overrides']
      ),
      el(
        'button',
        {
          className: 'btn btn--danger',
          style: 'margin-left:.5rem',
          onclick: async () => {
            if (!window.confirm(`Quitar TODOS los overrides de ${userTarget.email}?`)) return;
            try {
              const before = originalOverrides;
              await deps.clearUserOverrides?.(userTarget.uid);
              await deps.addAuditLog?.({
                targetType: 'user',
                targetId: userTarget.uid,
                action: 'clear_user_overrides',
                before
              });
              originalOverrides = {};
              userOverrides = {};
              alert('Overrides eliminados.');
            } catch (e) {
              alert('Error: ' + (e?.message || e));
            }
          }
        },
        ['Quitar overrides']
      )
    ]);
    return el('div', {}, [info, grid, actions]);
  }

  function renderAuditTab() {
    const box = el('div', { className: 'mt-1' }, [el('p', { className: 'text-muted' }, ['Ultimos cambios de permisos'])]);
    const list = el('div', { id: 'auditList', className: 'mt-1' }, []);
    box.append(list);
    clearAuditStream();
    unAudit =
      deps.streamAuditLogs?.((items) => {
        list.replaceChildren(...items.map((it) => renderAuditItem(it)));
      }) || null;
    return box;
  }

  function renderAuditItem(it) {
    const date = it.ts?.toDate ? it.ts.toDate() : it.ts || new Date();
    return el('div', { className: 'card', style: 'margin-top:.5rem' }, [
      el('div', {}, [el('strong', {}, [it.action || 'accion']), ' - ', new Date(date).toLocaleString()]),
      el('div', { className: 'mt-1 text-muted' }, [`Actor: ${it.actorEmail || it.actorUid || '-'}`]),
      el('div', { className: 'mt-1' }, [`Target: ${it.targetType}/${it.targetId}`])
    ]);
  }

  renderTab();
  mount.replaceChildren(ui);
  return () => clearAuditStream();
};
