import { el, qs, infoIcon, moreIcon } from '../utils/dom.js';
import { PERMS, can } from '../permissions.js';
import { getState } from '../state.js';
import { ALL_ROLES, ROLE_LABELS } from '../roles.js';
import { showActionModal } from '../utils/actionModal.js';
import { showInfoModal } from '../utils/infoModal.js';
import { createTablePagination } from '../utils/pagination.js';

const STATUS = ['activo', 'inactivo', 'eliminado'];

export const UsersAdmin = (mount, deps = {}) => {
  if (!can(PERMS.VIEW_USERS)) {
    return mount.replaceChildren(
      el('section', { className: 'main-card' }, [
        el('h2', {}, ['Usuarios']),
        el('p', {}, ['No tienes permiso para consultar usuarios.'])
      ])
    );
  }
  const canEditUsers = can(PERMS.EDIT_USERS);

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Gestion de usuarios']),
    el('div', { className: 'form-row' }, [
      el('div', {}, [el('label', { className: 'label' }, ['Buscar']), el('input', { id: 'search', className: 'input', placeholder: 'Correo, nombre o documento...' })]),
      el('div', {}, [el('label', { className: 'label' }, ['Rol']), el('select', { id: 'roleFilter', className: 'select' }, [])]),
      el('div', {}, [el('label', { className: 'label' }, ['Estado']), el('select', { id: 'statusFilter', className: 'select' }, [])]),
      el('span', { className: 'right text-muted' }, [canEditUsers ? 'Administra rol y estado de acceso.' : 'Modo consulta: sin permisos de edicion.'])
    ]),
    el('div', { className: 'users-results mt-2' }, [
      el('div', { className: 'table-wrap users-table-view' }, [
        el('table', { className: 'table', id: 'tbl' }, [
          el('thead', {}, [el('tr', {}, [el('th', {}, ['Usuario']), el('th', {}, ['Correo']), el('th', {}, ['Rol']), el('th', {}, ['Estado']), el('th', {}, ['Acciones'])])]),
          el('tbody', {})
        ])
      ]),
      el('div', { id: 'userCards', className: 'users-card-list' }, [])
    ]),
    el('p', { id: 'msg', className: 'mt-2 text-muted' }, [' '])
  ]);

  const roleFilter = qs('#roleFilter', ui);
  roleFilter.append(el('option', { value: '' }, ['Todos']), ...ALL_ROLES.map((r) => el('option', { value: r }, [roleLabel(r)])));
  const statusFilter = qs('#statusFilter', ui);
  statusFilter.append(el('option', { value: '' }, ['Todos']), ...STATUS.map((s) => el('option', { value: s }, [s])));

  const msg = qs('#msg', ui);
  const tbody = qs('tbody', ui);
  const cards = qs('#userCards', ui);
  const paginator = createTablePagination(ui, { id: 'users', after: '.users-results', onChange: renderRows });
  let data = [];

  function statusOf(u) {
    const raw = String(u?.estado || 'activo').trim().toLowerCase();
    if (raw === 'inactivo' || raw === 'eliminado') return raw;
    return 'activo';
  }

  function statusBadge(st) {
    const cls = st === 'activo' ? 'badge--ok' : 'badge--off';
    return el('span', { className: `badge ${cls}` }, [st]);
  }

  function formatDate(ts) {
    try {
      const d = ts?.toDate ? ts.toDate() : ts ? new Date(ts) : null;
      return d ? new Date(d).toLocaleString() : '-';
    } catch {
      return '-';
    }
  }

  function infoData(u) {
    return [
      `UID: ${u.uid || '-'}`,
      `Documento: ${u.documento || '-'}`,
      `Estado: ${statusOf(u)}`,
      `Supervisor elegible: ${u.supervisorEligible === true ? 'Si' : 'No'}`,
      `Zona perfil: ${u.zonaCodigo || '-'}`,
      `Zonas permitidas: ${(u.zonasPermitidas || []).join(', ') || '-'}`,
      `Creado por: ${u.createdByEmail || u.createdByUid || '-'}`,
      `Creado el: ${formatDate(u.createdAt)}`,
      `Ultimo cambio por: ${u.lastModifiedByEmail || u.lastModifiedByUid || '-'}`,
      `Ultimo cambio: ${formatDate(u.lastModifiedAt)}`,
      `Eliminado por: ${u.deletedByEmail || u.deletedByUid || '-'}`,
      `Eliminado el: ${formatDate(u.deletedAt)}`
    ];
  }

  function setMsg(text) {
    msg.textContent = text || ' ';
  }

  function roleLabel(role) {
    return ROLE_LABELS[role] || role || '-';
  }

  async function handleRoleChange(user, sel) {
    const prev = user.role || 'empleado';
    const next = String(sel.value || '').trim();
    if (!next || next === prev) return;
    try {
      await updateUserRole(user, prev, next);
    } catch {
      sel.value = prev;
    }
  }

  async function changeRole(user) {
    const prev = user.role || 'empleado';
    const modal = await showActionModal({
      title: 'Cambiar rol',
      message: `Usuario: ${user.email || user.uid}`,
      confirmText: 'Guardar rol',
      fields: [{
        id: 'role',
        label: 'Rol',
        type: 'select',
        required: true,
        value: prev,
        options: ALL_ROLES.map((role) => ({ value: role, label: roleLabel(role) }))
      }]
    });
    if (!modal?.confirmed) return;
    const next = String(modal.values.role || '').trim();
    if (!next || next === prev) return;
    try {
      await updateUserRole(user, prev, next);
    } catch {}
  }

  async function updateUserRole(user, prev, next) {
    try {
      await deps.setUserRole?.(user.uid, next);
      await deps.addAuditLog?.({
        targetType: 'user',
        targetId: user.uid,
        action: 'update_user_role',
        before: { role: prev },
        after: { role: next }
      });
      setMsg(`Rol actualizado para ${user.email || user.uid}: ${next}`);
    } catch (e) {
      setMsg(`Error al actualizar rol: ${e?.message || e}`);
      throw e;
    }
  }

  async function changeStatus(user, target) {
    const currentStatus = statusOf(user);
    const title = target === 'inactivo' ? 'Desactivar usuario' : 'Activar usuario';
    const modal = await showActionModal({
      title,
      message: `Usuario: ${user.email || user.uid}`,
      confirmText: target === 'inactivo' ? 'Desactivar' : 'Activar',
      fields: [{ id: 'detail', label: 'Detalle', type: 'textarea', required: true, placeholder: 'Escribe el motivo de esta accion' }]
    });
    if (!modal.confirmed) return;
    await deps.setUserStatus?.(user.uid, target);
    await deps.addAuditLog?.({
      targetType: 'user',
      targetId: user.uid,
      action: target === 'inactivo' ? 'deactivate_user' : 'activate_user',
      before: { estado: currentStatus },
      after: { estado: target },
      note: modal.values.detail || null
    });
    setMsg(`Usuario ${target === 'inactivo' ? 'desactivado' : 'activado'}: ${user.email || user.uid}`);
  }

  function actionsCell(u) {
    const box = el('div', { className: 'row-actions' }, []);
    const btnMore = el('button', { className: 'btn btn--icon', type: 'button', title: 'Mas opciones', 'aria-label': 'Mas opciones' }, [moreIcon()]);
    btnMore.addEventListener('click', () => openMoreOptionsModal(u));
    box.append(btnMore);

    const btnInfo = el('button', { className: 'btn btn--icon', type: 'button', title: 'Ver informacion', 'aria-label': 'Ver informacion' }, [infoIcon()]);
    btnInfo.addEventListener('click', () => showInfoModal('Informacion del usuario', infoData(u)));
    box.append(btnInfo);

    return box;
  }

  async function openMoreOptionsModal(u) {
    const currentUid = getState()?.user?.uid || '';
    const isSelf = String(currentUid || '') === String(u.uid || '');
    const st = statusOf(u);
    const isSupervisor = String(u.role || '').trim().toLowerCase() === 'supervisor';
    const options = [{ value: '', label: 'Seleccione...' }];
    if (canEditUsers) {
      options.push({ value: 'change_role', label: 'Cambiar rol' });
    }
    if (st !== 'eliminado' && canEditUsers && isSupervisor) {
      options.push({ value: 'sync_supervisor', label: 'Sincronizar acceso supervisor' });
    }
    if (st !== 'eliminado' && canEditUsers && !isSelf) {
      options.push({ value: 'toggle_status', label: st === 'activo' ? 'Desactivar usuario' : 'Activar usuario' });
    } else if (st === 'eliminado' && canEditUsers && !isSelf) {
      options.push({ value: 'toggle_status', label: 'Activar usuario' });
    }
    if (options.length === 1) {
      showInfoModal('Mas opciones', ['No hay acciones disponibles para este usuario.']);
      return;
    }

    const modal = await showActionModal({
      title: 'Mas opciones',
      message: `Usuario: ${u.email || u.uid || '-'}`,
      confirmText: 'Continuar',
      fields: [{
        id: 'action',
        label: 'Accion',
        type: 'select',
        required: true,
        options
      }]
    });
    if (!modal?.confirmed) return;
    if (modal.values.action === 'change_role') return changeRole(u);
    if (modal.values.action === 'sync_supervisor') return syncSupervisorAccess(u);
    if (modal.values.action === 'toggle_status') return toggleUserStatus(u, st);
  }

  async function syncSupervisorAccess(u) {
    try {
      await deps.syncSupervisorAccessForUser?.(u.uid);
      await deps.addAuditLog?.({
        targetType: 'user',
        targetId: u.uid,
        action: 'sync_supervisor_access',
        before: {
          supervisorEligible: u.supervisorEligible === true,
          zonaCodigo: u.zonaCodigo || null,
          zonasPermitidas: u.zonasPermitidas || []
        }
      });
      setMsg(`Acceso supervisor sincronizado: ${u.email || u.uid}`);
    } catch (e) {
      setMsg(`Error al sincronizar supervisor: ${e?.message || e}`);
    }
  }

  async function toggleUserStatus(u, st) {
    try {
      await changeStatus(u, st === 'activo' ? 'inactivo' : 'activo');
    } catch (e) {
      setMsg(`Error al actualizar estado: ${e?.message || e}`);
    }
  }

  function roleSelect(u) {
    const currentRole = u.role || 'empleado';
    const sel = el(
      'select',
      { className: 'select', disabled: !canEditUsers },
      ALL_ROLES.map((r) => el('option', { value: r, selected: currentRole === r }, [roleLabel(r)]))
    );
    if (canEditUsers) sel.addEventListener('change', () => handleRoleChange(u, sel));
    return sel;
  }

  function renderRow(u) {
    const st = statusOf(u);
    const tr = el('tr', { 'data-id': u.uid || '' });
    tr.append(
      el('td', {}, [u.displayName || '-']),
      el('td', {}, [u.email || '-']),
      el('td', {}, [roleSelect(u)]),
      el('td', {}, [statusBadge(st)]),
      el('td', {}, [actionsCell(u)])
    );
    return tr;
  }

  function renderUserCard(u) {
    const st = statusOf(u);
    const role = u.role || 'empleado';
    const lastChange = formatDate(u.lastModifiedAt || u.createdAt);
    return el('article', { className: 'user-card' }, [
      el('div', { className: 'user-card__header' }, [
        el('div', { className: 'user-card__identity' }, [
          el('strong', { className: 'user-card__name' }, [u.displayName || '-']),
          el('span', { className: 'user-card__email' }, [u.email || '-'])
        ]),
        statusBadge(st)
      ]),
      el('dl', { className: 'user-card__meta' }, [
        metaItem('Rol', roleLabel(role)),
        metaItem('Documento', u.documento || '-'),
        metaItem('Ultimo cambio', lastChange)
      ]),
      el('div', { className: 'user-card__actions' }, [actionsCell(u)])
    ]);
  }

  function metaItem(label, value) {
    return el('div', { className: 'user-card__meta-item' }, [
      el('dt', {}, [label]),
      el('dd', {}, [value || '-'])
    ]);
  }

  function renderRows() {
    const term = String(qs('#search', ui).value || '').trim().toLowerCase();
    const rf = String(qs('#roleFilter', ui).value || '').trim();
    const sf = String(qs('#statusFilter', ui).value || '').trim();
    const rows = (data || [])
      .filter((u) => {
        const text = `${u.email || ''} ${u.displayName || ''} ${u.documento || ''}`.toLowerCase();
        return (!term || text.includes(term)) && (!rf || (u.role || 'empleado') === rf) && (!sf || statusOf(u) === sf);
      })
      .sort((a, b) => String(a.email || '').localeCompare(String(b.email || '')));
    const pageRows = paginator.slice(rows);

    tbody.replaceChildren(...pageRows.map((u) => renderRow(u)));
    cards.replaceChildren(...(pageRows.length
      ? pageRows.map((u) => renderUserCard(u))
      : [el('p', { className: 'text-muted user-card__empty' }, ['Sin usuarios para mostrar.'])]));
    setMsg(`Total registros filtrados: ${rows.length}`);
  }

  qs('#search', ui).addEventListener('input', () => { paginator.reset(); renderRows(); });
  qs('#roleFilter', ui).addEventListener('change', () => { paginator.reset(); renderRows(); });
  qs('#statusFilter', ui).addEventListener('change', () => { paginator.reset(); renderRows(); });

  const un = deps.streamUsers?.((users) => {
    data = users || [];
    renderRows();
  });

  mount.replaceChildren(ui);
  return () => un?.();
};
