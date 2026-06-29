let container = null;
let installed = false;

const MAX_VISIBLE_NOTICES = 4;

export function installBrowserAlertReplacement() {
  if (installed || typeof window === 'undefined' || typeof document === 'undefined') return;
  installed = true;
  const nativeAlert = window.alert?.bind(window);
  window.appNotify = showNotification;
  window.alert = (message) => {
    try {
      showNotification(String(message ?? ''), { type: inferNoticeType(message) });
    } catch (error) {
      nativeAlert?.(message);
    }
  };
}

export function showNotification(message, options = {}) {
  const text = String(message ?? '').trim();
  if (!text) return null;
  const type = normalizeType(options.type || inferNoticeType(text));
  const timeout = Number(options.timeoutMs ?? defaultTimeout(type));
  const root = ensureContainer();
  const notice = document.createElement('div');
  notice.className = `app-notice app-notice--${type}`;
  notice.setAttribute('role', type === 'error' ? 'alert' : 'status');

  const marker = document.createElement('span');
  marker.className = 'app-notice__marker';
  marker.setAttribute('aria-hidden', 'true');

  const body = document.createElement('div');
  body.className = 'app-notice__body';
  const title = document.createElement('strong');
  title.className = 'app-notice__title';
  title.textContent = titleForType(type);
  const msg = document.createElement('span');
  msg.className = 'app-notice__message';
  msg.textContent = text;
  body.append(title, msg);

  const close = document.createElement('button');
  close.className = 'app-notice__close';
  close.type = 'button';
  close.setAttribute('aria-label', 'Cerrar aviso');
  close.textContent = 'x';
  close.addEventListener('click', () => dismissNotice(notice));

  notice.append(marker, body, close);
  root.prepend(notice);

  while (root.children.length > MAX_VISIBLE_NOTICES) {
    dismissNotice(root.lastElementChild, 0);
  }

  if (timeout > 0) {
    setTimeout(() => dismissNotice(notice), timeout);
  }
  return notice;
}

function ensureContainer() {
  if (container && document.body.contains(container)) return container;
  container = document.createElement('div');
  container.className = 'app-notices';
  container.setAttribute('aria-live', 'polite');
  container.setAttribute('aria-relevant', 'additions');
  document.body.append(container);
  return container;
}

function dismissNotice(notice, delay = 160) {
  if (!notice || !notice.parentNode) return;
  notice.classList.add('is-leaving');
  setTimeout(() => notice.remove(), delay);
}

function normalizeType(type) {
  const value = String(type || '').trim().toLowerCase();
  if (['success', 'error', 'warning', 'info'].includes(value)) return value;
  return 'info';
}

function inferNoticeType(value) {
  const text = String(value ?? '').trim().toLowerCase();
  if (!text) return 'info';
  if (text.startsWith('error') || text.includes('no se pudo') || text.includes('no tienes permiso')) return 'error';
  if (text.includes('invalida') || text.includes('invalido') || text.includes('selecciona') || text.includes('completa') || text.includes('debe')) return 'warning';
  if (text.includes(' ok') || text.includes('actualizad') || text.includes('guardad') || text.includes('cread') || text.includes('cancelad')) return 'success';
  return 'info';
}

function titleForType(type) {
  if (type === 'success') return 'Listo';
  if (type === 'error') return 'Error';
  if (type === 'warning') return 'Revisa';
  return 'Aviso';
}

function defaultTimeout(type) {
  if (type === 'error') return 7000;
  if (type === 'warning') return 5600;
  return 4200;
}
