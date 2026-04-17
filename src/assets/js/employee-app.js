import { CargarDatos } from './components/CargarDatos.js';
import { EMPLOYEE_PORTAL_API_BASE } from './config.js';
import { el, qs } from './utils/dom.js';

const root = document.getElementById('employee-root');
const SESSION_STORAGE_KEY = 'employee_portal_token';

function apiUrl(path) {
  const base = String(EMPLOYEE_PORTAL_API_BASE || '').trim().replace(/\/+$/, '');
  return base ? `${base}${path}` : path;
}

function getSessionToken() {
  try {
    return String(sessionStorage.getItem(SESSION_STORAGE_KEY) || '').trim();
  } catch {
    return '';
  }
}

function setSessionToken(token) {
  try {
    if (token) sessionStorage.setItem(SESSION_STORAGE_KEY, token);
    else sessionStorage.removeItem(SESSION_STORAGE_KEY);
  } catch {}
}

async function request(path, options = {}) {
  const token = getSessionToken();
  const response = await fetch(apiUrl(path), {
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(options.headers || {})
    },
    ...options
  });

  let data = {};
  try {
    data = await response.json();
  } catch {
    data = {};
  }

  if (!response.ok || data?.ok === false) {
    if (response.status === 401 || response.status === 403) setSessionToken('');
    const error = new Error(data?.error || 'No fue posible completar la solicitud.');
    error.redirectMain = data?.redirectMain === true;
    throw error;
  }

  return data;
}

function setMessage(node, text, state = '') {
  node.textContent = text || ' ';
  if (state) node.dataset.state = state;
  else delete node.dataset.state;
}

function employeeCard(children = []) {
  return el('section', { className: 'main-card employee-card' }, children);
}

function renderLoading(text = 'Validando acceso...') {
  root.replaceChildren(employeeCard([
    el('h2', {}, ['Portal de empleados']),
    el('p', { className: 'text-muted' }, [text])
  ]));
}

function renderLogin() {
  const msg = el('p', { className: 'employee-message text-muted' }, [' ']);
  const ui = employeeCard([
    el('h2', {}, ['Ingreso rapido']),
    el('p', { className: 'text-muted' }, ['No necesitas crear cuenta. Usa el documento y los ultimos 4 digitos del celular registrado en empleados.']),
    el('form', { className: 'employee-form' }, [
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'employeeDoc' }, ['Documento']),
        el('input', { id: 'employeeDoc', className: 'input', inputMode: 'numeric', autocomplete: 'username', placeholder: 'Numero de documento' })
      ]),
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'employeeLast4' }, ['Ultimos 4 del celular']),
        el('input', { id: 'employeeLast4', className: 'input', inputMode: 'numeric', autocomplete: 'one-time-code', placeholder: '1234', maxLength: 4 })
      ]),
      el('div', { className: 'employee-form__actions' }, [
        el('button', { className: 'btn btn--primary', type: 'submit' }, ['Ingresar']),
        el('a', { className: 'link', href: 'access.html' }, ['Ver todos los accesos'])
      ]),
      msg
    ])
  ]);

  ui.querySelector('form')?.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const documento = String(qs('#employeeDoc', ui)?.value || '').trim();
    const last4 = String(qs('#employeeLast4', ui)?.value || '').trim();

    setMessage(msg, 'Validando tu acceso...', 'ok');

    try {
      const data = await request('/api/employee-login', {
        method: 'POST',
        body: JSON.stringify({ documento, last4 })
      });
      setSessionToken(String(data?.token || '').trim());
      renderDashboard();
    } catch (error) {
      setMessage(msg, error.message || 'No fue posible iniciar sesion.', 'error');
      if (error.redirectMain) {
        window.setTimeout(() => {
          window.location.href = 'access.html';
        }, 1400);
      }
    }
  });

  root.replaceChildren(ui);
}

async function logout() {
  try {
    await request('/api/employee-logout', { method: 'POST', body: JSON.stringify({}) });
  } catch {}
  setSessionToken('');
  renderLogin();
}

function statCard(label, value) {
  return el('article', { className: 'employee-stat' }, [
    el('p', { className: 'employee-stat__label' }, [label]),
    el('p', { className: 'employee-stat__value' }, [value])
  ]);
}

function formatDate(value) {
  try {
    const date = value ? new Date(value) : null;
    if (!date || Number.isNaN(date.getTime())) return '-';
    return date.toLocaleString('es-CO');
  } catch {
    return '-';
  }
}

function renderDashboardCard(session) {
  const host = el('div');
  const uploadMount = el('div');
  const info = employeeCard([
    el('div', { className: 'form-row' }, [
      el('div', {}, [
        el('h2', { style: 'margin:0;' }, [`Hola, ${session?.nombre || 'Empleado'}`]),
        el('p', { className: 'text-muted mt-1' }, ['Tu acceso esta limitado a la seccion de carga de datos del portal de empleados.'])
      ]),
      el('button', { className: 'btn right', type: 'button' }, ['Cerrar sesion'])
    ]),
    el('div', { className: 'employee-grid mt-2' }, [
      statCard('Documento', session?.documento || '-'),
      statCard('Sesion vence', formatDate(session?.expiresAt))
    ]),
    el('div', { className: 'divider' }, []),
    uploadMount
  ]);

  info.querySelector('button')?.addEventListener('click', logout);
  host.append(info);
  CargarDatos(uploadMount);
  return host;
}

async function renderDashboard() {
  renderLoading('Cargando sesion del empleado...');

  try {
    const data = await request('/api/employee-me', { method: 'GET' });
    root.replaceChildren(renderDashboardCard(data.session || {}));
  } catch (error) {
    if (error.redirectMain) {
      root.replaceChildren(employeeCard([
        el('h2', {}, ['Acceso redirigido']),
        el('p', { className: 'text-muted' }, ['Este empleado tiene un perfil con acceso ampliado. Continua por el acceso administrativo desde el centro de accesos.']),
        el('div', { className: 'mt-2' }, [el('a', { className: 'btn btn--primary', href: 'access.html' }, ['Ir al centro de accesos'])])
      ]));
      return;
    }

    renderLogin();
  }
}

renderLoading();
renderDashboard();
