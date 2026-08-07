import { el, qs } from '../utils/dom.js';
import { isSuperAdmin } from '../permissions.js';
import { createTablePagination } from '../utils/pagination.js';

export const PermissionsAudit = (mount, deps = {}) => {
  if (!isSuperAdmin()) {
    mount.replaceChildren(
      el('section', { className: 'main-card' }, [
        el('h2', {}, ['Auditoria de permisos']),
        el('p', {}, ['Solo SuperAdmin puede consultar la auditoria de permisos.'])
      ])
    );
    return;
  }

  let unAudit = null;
  let auditItems = [];
  let auditPaginator = null;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Auditoria de permisos']),
    el('p', { className: 'text-muted' }, ['Consulta los ultimos cambios registrados sobre permisos y roles.']),
    el('div', { id: 'auditList', className: 'mt-2' }, [])
  ]);

  function clearAuditStream() {
    if (typeof unAudit === 'function') {
      try {
        unAudit();
      } catch {}
      unAudit = null;
    }
  }

  function renderAuditPage() {
    const list = qs('#auditList', ui);
    if (!list) return;
    if (!auditPaginator || !ui.contains(auditPaginator.controls)) {
      auditPaginator = createTablePagination(ui, { id: 'permissionsAudit', after: '#auditList', onChange: renderAuditPage });
    }
    const pageItems = auditPaginator.slice(auditItems);
    list.replaceChildren(...pageItems.map((it) => renderAuditItem(it)));
  }

  function renderAuditItem(it) {
    const date = it.ts?.toDate ? it.ts.toDate() : it.ts || new Date();
    const note = String(it.note || '').trim();
    const beforeText = formatAuditValue(it.before);
    const afterText = formatAuditValue(it.after);
    return el('div', { className: 'card', style: 'margin-top:.5rem' }, [
      el('div', {}, [el('strong', {}, [it.action || 'accion']), ' - ', new Date(date).toLocaleString()]),
      el('div', { className: 'mt-1 text-muted' }, [`Actor: ${it.actorEmail || it.actorUid || '-'}`]),
      el('div', { className: 'mt-1' }, [`Target: ${it.targetType || '-'}/${it.targetId || '-'}`]),
      el('div', { className: 'mt-1' }, [`Observacion: ${note || '-'}`]),
      el('details', { className: 'mt-1' }, [
        el('summary', {}, ['Ver cambios (Antes / Despues)']),
        el('div', { className: 'mt-1' }, [
          el('div', {}, [el('strong', {}, ['Antes'])]),
          el(
            'pre',
            { className: 'mt-1', style: 'white-space:pre-wrap;word-break:break-word;background:#f7f7f7;padding:.5rem;border-radius:.375rem' },
            [beforeText]
          ),
          el('div', { className: 'mt-1' }, [el('strong', {}, ['Despues'])]),
          el(
            'pre',
            { className: 'mt-1', style: 'white-space:pre-wrap;word-break:break-word;background:#f7f7f7;padding:.5rem;border-radius:.375rem' },
            [afterText]
          )
        ])
      ])
    ]);
  }

  function formatAuditValue(value) {
    if (value == null) return '-';
    if (typeof value === 'string') return value.trim() || '-';
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  mount.replaceChildren(ui);
  unAudit =
    deps.streamAuditLogs?.((items) => {
      auditItems = items || [];
      renderAuditPage();
    }) || null;

  return () => clearAuditStream();
};
