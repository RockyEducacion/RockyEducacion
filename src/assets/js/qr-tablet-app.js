import { QrTabletScanner } from './components/QrTabletScanner.js';
import { can, PERMS } from './permissions.js';
import { setState } from './state.js';
import { el, qs } from './utils/dom.js';

const root = document.getElementById('qr-tablet-root');

let deps = {};
let currentUser = null;
let currentProfile = null;
let unsubscribeAuth = null;
let unsubscribeRoleMatrix = null;
let unsubscribeUserOverrides = null;
let scannerCleanup = null;
let roleMatrixReady = false;
let userOverridesReady = false;
let authSyncToken = 0;
let currentView = '';

function cleanupScanner() {
  try { scannerCleanup?.(); } catch {}
  scannerCleanup = null;
}

function resetSubscriptions() {
  try { unsubscribeRoleMatrix?.(); } catch {}
  try { unsubscribeUserOverrides?.(); } catch {}
  unsubscribeRoleMatrix = null;
  unsubscribeUserOverrides = null;
  roleMatrixReady = false;
  userOverridesReady = false;
}

function setMessage(node, text, state = '') {
  node.textContent = text || ' ';
  if (state) node.dataset.state = state;
  else delete node.dataset.state;
}

function topbar(profile = {}) {
  return el('header', { className: 'qr-tablet-topbar' }, [
    el('div', { className: 'qr-tablet-brand' }, [
      el('img', { src: 'src/assets/img/rocky-logo.png', alt: 'Rocky' }),
      el('div', {}, [
        el('h1', {}, ['Lector QR Tablet']),
        el('p', {}, [profile?.email || 'Acceso seguro'])
      ])
    ]),
    el('div', { className: 'qr-tablet-actions' }, [
      el('a', { className: 'btn', href: 'access.html' }, ['Centro de accesos']),
      el('button', { id: 'btnQrTabletLogout', className: 'btn btn--primary', type: 'button' }, ['Cerrar sesion'])
    ])
  ]);
}

function renderLoading(text = 'Validando acceso...') {
  cleanupScanner();
  currentView = 'loading';
  root.replaceChildren(el('section', { className: 'qr-tablet-card' }, [
    el('h2', {}, ['Lector QR Tablet']),
    el('p', { className: 'text-muted' }, [text])
  ]));
}

function renderLogin(message = '') {
  cleanupScanner();
  currentView = 'login';
  const msg = el('p', { className: 'qr-tablet-message text-muted' }, [message || ' ']);
  const ui = el('section', { className: 'qr-tablet-card' }, [
    el('h2', {}, ['Lector QR Tablet']),
    el('p', { className: 'text-muted mt-1' }, ['Ingresa con el mismo usuario y contrasena del panel Rocky. Esta pantalla solo habilita el lector QR.']),
    el('form', { className: 'qr-tablet-login-form' }, [
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'qrTabletEmail' }, ['Correo']),
        el('input', { id: 'qrTabletEmail', className: 'input', type: 'email', autocomplete: 'username', placeholder: 'correo@dominio.com' })
      ]),
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'qrTabletPassword' }, ['Contrasena']),
        el('input', { id: 'qrTabletPassword', className: 'input', type: 'password', autocomplete: 'current-password', placeholder: '********' })
      ]),
      el('button', { className: 'btn btn--primary', type: 'submit' }, ['Iniciar sesion']),
      msg
    ])
  ]);

  qs('form', ui)?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const email = String(qs('#qrTabletEmail', ui)?.value || '').trim();
    const pass = String(qs('#qrTabletPassword', ui)?.value || '');
    setMessage(msg, 'Validando acceso...', 'ok');
    try {
      await deps.login?.(email, pass);
    } catch (error) {
      setMessage(msg, `Error al iniciar sesion: ${error?.message || error}`, 'error');
    }
  });

  root.replaceChildren(ui);
}

function renderDenied(text = 'No tienes permiso para usar el lector QR.') {
  cleanupScanner();
  currentView = 'denied';
  const ui = el('div', {}, [
    topbar(currentProfile),
    el('section', { className: 'qr-tablet-card', style: 'margin-top:1rem;' }, [
      el('h2', {}, ['Acceso restringido']),
      el('p', { className: 'text-muted mt-1' }, [text]),
      el('p', { className: 'text-muted mt-1' }, ['Solicita al administrador el permiso QR - Lector.'])
    ])
  ]);
  root.replaceChildren(ui);
  qs('#btnQrTabletLogout', ui)?.addEventListener('click', logout);
}

function renderScanner() {
  if (currentView === 'scanner' && scannerCleanup) return;
  cleanupScanner();
  currentView = 'scanner';
  const scannerMount = el('section', { className: 'qr-tablet-scanner-host' });
  const ui = el('div', { className: 'qr-tablet-scanner-host' }, [
    topbar(currentProfile),
    scannerMount
  ]);
  root.replaceChildren(ui);
  qs('#btnQrTabletLogout', ui)?.addEventListener('click', logout);
  scannerCleanup = QrTabletScanner(scannerMount, {
    scanAttendanceQr: deps.scanAttendanceQr
  });
}

function renderProtected() {
  if (!currentUser) {
    renderLogin();
    return;
  }
  if (!currentProfile) {
    renderLoading('Cargando perfil...');
    return;
  }
  if (String(currentProfile.estado || 'activo').trim().toLowerCase() !== 'activo') {
    renderDenied('Tu usuario no esta activo.');
    return;
  }
  if (!roleMatrixReady || !userOverridesReady) {
    renderLoading('Cargando permisos...');
    return;
  }
  if (!can(PERMS.VIEW_QR_SCANNER)) {
    renderDenied();
    return;
  }
  renderScanner();
}

async function logout() {
  cleanupScanner();
  resetSubscriptions();
  try { await deps.logout?.(); } catch {}
  currentUser = null;
  currentProfile = null;
  setState({ user: null, userProfile: null, roleMatrix: {}, userOverrides: {} });
  renderLogin();
}

function subscribePermissionState(userId) {
  resetSubscriptions();
  unsubscribeRoleMatrix = deps.streamRoleMatrix?.((map = {}) => {
    roleMatrixReady = true;
    setState({ roleMatrix: map || {} });
    renderProtected();
  }) || null;
  unsubscribeUserOverrides = deps.streamUserOverrides?.(userId, (overrides = {}) => {
    userOverridesReady = true;
    setState({ userOverrides: overrides || {} });
    renderProtected();
  }) || null;
  if (!unsubscribeRoleMatrix) roleMatrixReady = true;
  if (!unsubscribeUserOverrides) userOverridesReady = true;
}

async function handleAuthUser(user) {
  const syncToken = ++authSyncToken;
  cleanupScanner();

  if (!user) {
    resetSubscriptions();
    currentUser = null;
    currentProfile = null;
    setState({ user: null, userProfile: null, roleMatrix: {}, userOverrides: {} });
    renderLogin();
    return;
  }

  renderLoading('Validando usuario...');
  currentUser = user;

  try {
    await deps.ensureUserProfile?.(user);
    const profile = await deps.loadUserProfile?.(user.uid);
    if (syncToken !== authSyncToken) return;
    currentProfile = profile || null;
    setState({ user, userProfile: currentProfile, userOverrides: {} });
    subscribePermissionState(user.uid);
    renderProtected();
  } catch (error) {
    console.error('No se pudo validar el acceso QR tablet:', error);
    if (syncToken !== authSyncToken) return;
    currentUser = null;
    currentProfile = null;
    setState({ user: null, userProfile: null, roleMatrix: {}, userOverrides: {} });
    renderLogin('No fue posible validar tu acceso. Intenta nuevamente.');
  }
}

async function init() {
  renderLoading();
  try {
    const fb = await import('./supabase.js');
    deps = {
      login: fb.login,
      logout: fb.logout,
      authState: fb.authState,
      ensureUserProfile: fb.ensureUserProfile,
      loadUserProfile: fb.loadUserProfile,
      streamRoleMatrix: fb.streamRoleMatrix,
      streamUserOverrides: fb.streamUserOverrides,
      scanAttendanceQr: fb.scanAttendanceQr
    };
    unsubscribeAuth = fb.authState(handleAuthUser);
  } catch (error) {
    console.error('No se pudo iniciar el lector QR tablet:', error);
    renderLogin('No fue posible conectar con el proveedor de autenticacion.');
  }
}

window.addEventListener('pagehide', () => {
  cleanupScanner();
  try { unsubscribeAuth?.(); } catch {}
  resetSubscriptions();
});

init();
