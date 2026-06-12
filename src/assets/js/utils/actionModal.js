import { el } from './dom.js';

let activeOverlay = null;

export async function showActionModal({
  title = 'Confirmar accion',
  message = '',
  confirmText = 'Confirmar',
  cancelText = 'Cancelar',
  fields = []
} = {}) {
  closeActionModal();

  return new Promise((resolve) => {
    const overlay = el('div', { className: 'action-modal__overlay' }, []);
    const dialog = el('div', { className: 'action-modal', role: 'dialog', 'aria-modal': 'true' }, []);
    const header = el('div', { className: 'action-modal__header' }, [
      el('h3', { className: 'action-modal__title' }, [title]),
      el('button', { className: 'btn action-modal__close', type: 'button', 'aria-label': 'Cerrar' }, ['x'])
    ]);
    const body = el('div', { className: 'action-modal__body' }, []);
    if (message) body.append(el('p', { className: 'action-modal__message' }, [message]));

    const fieldNodes = [];
    for (const f of fields) {
      const row = el('div', { className: 'action-modal__field' }, [
        el('label', { className: 'label' }, [f.label || f.id || 'Campo'])
      ]);
      let inputNode;
      if (f.type === 'select') {
        inputNode = el(
          'select',
          { className: 'select', id: f.id || '' },
          (f.options || []).map((opt) =>
            el('option', { value: String(opt.value ?? ''), selected: String(opt.value ?? '') === String(f.value ?? '') }, [opt.label || String(opt.value ?? '')])
          )
        );
      } else if (f.type === 'datalist') {
        const listId = `${f.id || 'field'}_list_${Math.random().toString(36).slice(2, 8)}`;
        inputNode = el('input', {
          className: 'input',
          id: f.id || '',
          type: 'text',
          list: listId,
          placeholder: f.placeholder || '',
          value: f.value || ''
        });
        const listNode = el(
          'datalist',
          { id: listId },
          (f.options || []).map((opt) => {
            const value = typeof opt === 'string' ? opt : String(opt.value ?? '');
            const label = typeof opt === 'string' ? opt : String(opt.label ?? value);
            return el('option', { value }, [label]);
          })
        );
        row.append(inputNode, listNode);
        fieldNodes.push({ def: f, node: inputNode });
        body.append(row);
        continue;
      } else if (f.type === 'textarea') {
        inputNode = el('textarea', {
          className: 'input',
          id: f.id || '',
          rows: f.rows || 4,
          placeholder: f.placeholder || '',
          style: 'max-width:100%;width:100%;resize:vertical;'
        });
        inputNode.value = String(f.value || '');
      } else {
        inputNode = el('input', {
          className: 'input',
          id: f.id || '',
          type: f.type || 'text',
          placeholder: f.placeholder || '',
          value: f.value || ''
        });
        if (f.min !== undefined) inputNode.min = String(f.min || '');
        if (f.max !== undefined) inputNode.max = String(f.max || '');
        if (f.step !== undefined) inputNode.step = String(f.step || '');
        if (f.readonly) inputNode.readOnly = true;
        if (f.disabled) inputNode.disabled = true;
      }
      row.append(inputNode);
      fieldNodes.push({ def: f, node: inputNode });
      body.append(row);
    }

    const footer = el('div', { className: 'action-modal__footer' }, [
      el('button', { className: 'btn', type: 'button' }, [cancelText]),
      el('button', { className: 'btn btn--primary', type: 'button' }, [confirmText])
    ]);

    const btnCancel = footer.querySelectorAll('button')[0];
    const btnConfirm = footer.querySelectorAll('button')[1];
    const btnClose = header.querySelector('.action-modal__close');

    const done = (out) => {
      cleanup();
      resolve(out);
    };

    const onCancel = () => done({ confirmed: false, values: {} });

    const onConfirm = () => {
      const values = {};
      for (const { def, node } of fieldNodes) {
        const id = def.id || '';
        if (!id) continue;
        const value = node.type === 'file'
          ? (node.files?.[0] || null)
          : String(node.value || '').trim();
        if (def.required && !value) {
          alert(`Completa el campo: ${def.label || id}`);
          node.focus();
          return;
        }
        values[id] = value;
      }
      done({ confirmed: true, values });
    };

    const onBackdrop = (ev) => {
      if (ev.target === overlay) onCancel();
    };
    const onEsc = (ev) => {
      if (ev.key === 'Escape') onCancel();
    };

    const cleanup = () => {
      document.removeEventListener('keydown', onEsc);
      overlay.removeEventListener('click', onBackdrop);
      btnCancel.removeEventListener('click', onCancel);
      btnConfirm.removeEventListener('click', onConfirm);
      btnClose.removeEventListener('click', onCancel);
      overlay.remove();
      activeOverlay = null;
    };

    btnCancel.addEventListener('click', onCancel);
    btnConfirm.addEventListener('click', onConfirm);
    btnClose.addEventListener('click', onCancel);
    overlay.addEventListener('click', onBackdrop);
    document.addEventListener('keydown', onEsc);

    dialog.append(header, body, footer);
    overlay.append(dialog);
    document.body.append(overlay);
    activeOverlay = overlay;
  });
}

export function closeActionModal() {
  if (!activeOverlay) return;
  activeOverlay.remove();
  activeOverlay = null;
}
