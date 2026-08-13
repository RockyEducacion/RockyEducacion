import { el } from './dom.js';

let activeModal = null;

export function showInfoModal(title, lines = []) {
  closeInfoModal();

  const overlay = el('div', { className: 'info-modal__overlay' }, []);
  const dialog = el('div', { className: 'info-modal', role: 'dialog', 'aria-modal': 'true' }, []);
  const header = el('div', { className: 'info-modal__header' }, [
    el('h3', { className: 'info-modal__title' }, [title || 'Informacion']),
    el('button', { className: 'btn info-modal__close', type: 'button', 'aria-label': 'Cerrar' }, ['x'])
  ]);

  const items = Array.isArray(lines) ? lines : [lines];
  const body = el(
    'div',
    { className: 'info-modal__body' },
    items.map((line) => line instanceof Node
      ? line
      : el('p', { className: 'info-modal__line' }, [String(line || '-')])
    )
  );

  dialog.append(header, body);
  overlay.append(dialog);
  document.body.append(overlay);
  activeModal = overlay;

  const closeBtn = header.querySelector('.info-modal__close');
  const onEsc = (ev) => {
    if (ev.key === 'Escape') closeInfoModal();
  };
  const onBackdrop = (ev) => {
    if (ev.target === overlay) closeInfoModal();
  };

  closeBtn.addEventListener('click', closeInfoModal);
  overlay.addEventListener('click', onBackdrop);
  document.addEventListener('keydown', onEsc);

  overlay._cleanup = () => {
    document.removeEventListener('keydown', onEsc);
    overlay.removeEventListener('click', onBackdrop);
  };
}

export function closeInfoModal() {
  if (!activeModal) return;
  activeModal._cleanup?.();
  activeModal.remove();
  activeModal = null;
}
