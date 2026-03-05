import { el, qs } from '../utils/dom.js';
import { PERMS, can } from '../permissions.js';
import { getState } from '../state.js';
import { showActionModal } from '../utils/actionModal.js';
import { showInfoModal } from '../utils/infoModal.js';

const ROLES = ['superadmin', 'admin', 'editor', 'consultor', 'supervisor', 'empleado'];
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
    el('div', { className: 'mt-2 table-wrap' }, [
      el('table', { className: 'table', id: 'tbl' }, [
        el('thead', {}, [el('tr', {}, [el('th', {}, ['Usuario']), el('th', {}, ['Correo']), el('th', {}, ['Rol']), el('th', {}, ['Estado']), el('th', {}, ['Acciones'])])]),
        el('tbody', {})
      ])
    ]),
    el('p', { id: 'msg', className: 'mt-2 text-muted' }, [' '])
  ]);

  const roleFilter = qs('#roleFilter', ui);
  roleFilter.append(el('option', { value: '' }, ['Todos']), ...ROLES.map((r) => el('option', { value: r }, [r])));
  const statusFilter = qs('#statusFilter', ui);
  statusFilter.append(el('option', { value: '' }, ['Todos']), ...STATUS.map((s) => el('option', { value: s }, [s])));

  const msg = qs('#msg', ui);
  const tbody = qs('tbody', ui);
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

  async function handleRoleChange(user, sel) {
    const prev = user.role || 'empleado';
    const next = String(sel.value || '').trim();
    if (!next || next === prev) return;
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
      sel.value = prev;
      setMsg(`Error al actualizar rol: ${e?.message || e}`);
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

  async function deleteUser(user) {
    const modal = await showActionModal({
      title: 'Eliminar usuario',
      message: `Esta accion marca el usuario como eliminado y bloquea su acceso. Usuario: ${user.email || user.uid}`,
      confirmText: 'Eliminar',
      fields: [{ id: 'detail', label: 'Motivo', type: 'textarea', required: true, placeholder: 'Explica por que se elimina este usuario' }]
    });
    if (!modal.confirmed) return;
    await deps.softDeleteUser?.(user.uid);
    await deps.addAuditLog?.({
      targetType: 'user',
      targetId: user.uid,
      action: 'soft_delete_user',
      before: { estado: statusOf(user), role: user.role || 'empleado' },
      after: { estado: 'eliminado', role: 'empleado' },
      note: modal.values.detail || null
    });
    setMsg(`Usuario eliminado: ${user.email || user.uid}`);
  }

  function actionsCell(u) {
    const box = el('div', { className: 'row-actions' }, []);
    const currentUid = getState()?.user?.uid || '';
    const isSelf = String(currentUid || '') === String(u.uid || '');
    const st = statusOf(u);

    if (st !== 'eliminado' && canEditUsers) {
      const btnToggle = el(
        'button',
        { className: `btn btn--icon ${st === 'activo' ? 'btn--danger' : ''}`, title: st === 'activo' ? 'Desactivar' : 'Activar', 'aria-label': st === 'activo' ? 'Desactivar' : 'Activar' },
        [st === 'activo' ? '\u23FB' : '\u21BA']
      );
      btnToggle.disabled = isSelf;
      btnToggle.addEventListener('click', async () => {
        try {
          await changeStatus(u, st === 'activo' ? 'inactivo' : 'activo');
        } catch (e) {
          setMsg(`Error al actualizar estado: ${e?.message || e}`);
        }
      });
      box.append(btnToggle);
    }

    const btnDelete = el('button', { className: 'btn btn--icon btn--danger', title: 'Eliminar', 'aria-label': 'Eliminar' }, ['\u2716']);
    btnDelete.disabled = isSelf || st === 'eliminado' || !canEditUsers;
    if (canEditUsers) {
      btnDelete.addEventListener('click', async () => {
        try {
          await deleteUser(u);
        } catch (e) {
          setMsg(`Error al eliminar usuario: ${e?.message || e}`);
        }
      });
    }
    box.append(btnDelete);

    const btnInfo = el('button', { className: 'btn btn--icon', title: 'Ver informacion', 'aria-label': 'Ver informacion' }, ['\u24D8']);
    btnInfo.addEventListener('click', () => showInfoModal('Informacion del usuario', infoData(u)));
    box.append(btnInfo);

    return box;
  }

  function roleSelect(u) {
    const st = statusOf(u);
    const currentRole = u.role || 'empleado';
    const sel = el(
      'select',
      { className: 'select', disabled: st === 'eliminado' || !canEditUsers },
      ROLES.map((r) => el('option', { value: r, selected: currentRole === r }, [r]))
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

  function renderRows() {
    const term = String(qs('#search', ui).value || '').trim().toLowerCase();
    const rf = String(qs('#roleFilter', ui).value || '').trim();
    const sf = String(qs('#statusFilter', ui).value || '').trim();
    const rows = (data || [])
      .filter((u) => {
        const text = `${u.email || ''} ${u.displayName || ''} ${u.documento || ''}`.toLowerCase();
        return (!term || text.includes(term)) && (!rf || (u.role || 'empleado') === rf) && (!sf || statusOf(u) === sf);
      })
      .sort((a, b) => String(a.email || '').localeCompare(String(b.email || '')))
      .map((u) => renderRow(u));

    tbody.replaceChildren(...rows);
    setMsg(`Total registros filtrados: ${rows.length}`);
  }

  qs('#search', ui).addEventListener('input', renderRows);
  qs('#roleFilter', ui).addEventListener('change', renderRows);
  qs('#statusFilter', ui).addEventListener('change', renderRows);

  const un = deps.streamUsers?.((users) => {
    data = users || [];
    renderRows();
  });

  mount.replaceChildren(ui);
  return () => un?.();
};
