import { el, qs } from '../utils/dom.js';

export const Login = (mount, deps = {}) => {
  let currentTab = 'login';
  const root = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Acceso']),
    el('div', { className: 'tabs' }, [tabBtn('Acceso', 'login'), tabBtn('Crear cuenta', 'register')]),
    el('div', { id: 'tabContent', className: 'mt-2' }, [])
  ]);

  function tabBtn(text, key) {
    const b = el('button', { className: `tab${currentTab === key ? ' is-active' : ''}` }, [text]);
    b.addEventListener('click', () => {
      currentTab = key;
      renderTab();
    });
    return b;
  }

  function consumeBlockedMessage() {
    try {
      const txt = sessionStorage.getItem('auth_block_msg');
      if (txt) sessionStorage.removeItem('auth_block_msg');
      return txt || '';
    } catch {
      return '';
    }
  }

  function renderTab() {
    const c = qs('#tabContent', root);
    if (currentTab === 'login') c.replaceChildren(loginForm());
    else c.replaceChildren(registerForm());
    const idx = { login: 0, register: 1 }[currentTab];
    root.querySelectorAll('.tab').forEach((b) => b.classList.remove('is-active'));
    root.querySelectorAll('.tab')[idx].classList.add('is-active');
  }

  function loginForm() {
    const ui = el('div', {}, [
      el('label', { className: 'label mt-2' }, ['Correo']),
      el('input', { id: 'email', type: 'email', placeholder: 'correo@dominio.com', className: 'input' }),
      el('label', { className: 'label mt-2' }, ['Contrasena']),
      el('input', { id: 'pass', type: 'password', placeholder: '********', className: 'input' }),
      el('div', { className: 'mt-2' }, [el('button', { id: 'btnLogin', className: 'btn btn--primary' }, ['Iniciar sesion'])]),
      el('p', { className: 'mt-2' }, [el('span', { className: 'text-muted' }, ['No tienes cuenta? ']), el('span', { className: 'link', onclick: () => { currentTab = 'register'; renderTab(); } }, ['Crear cuenta'])]),
      el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
    ]);

    const blocked = consumeBlockedMessage();
    if (blocked) qs('#msg', ui).textContent = blocked;

    if (!deps.login) {
      qs('#msg', ui).textContent = 'El proveedor de autenticacion no esta disponible.';
    } else {
      ui.querySelector('#btnLogin').addEventListener('click', async () => {
        try {
          const email = ui.querySelector('#email').value.trim();
          const pass = ui.querySelector('#pass').value;
          await deps.login(email, pass);
          qs('#msg', ui).textContent = 'Sesion iniciada.';
        } catch (e) {
          qs('#msg', ui).textContent = `Error al iniciar sesion: ${e?.message || e}`;
        }
      });
    }

    return ui;
  }

  function registerForm() {
    const ui = el('div', {}, [
      el('label', { className: 'label mt-2' }, ['Documento']),
      el('input', { id: 'doc', placeholder: 'Numero de documento', className: 'input' }),
      el('label', { className: 'label mt-2' }, ['Nombre completo']),
      el('input', { id: 'name', placeholder: 'Tu nombre y apellidos', className: 'input' }),
      el('label', { className: 'label mt-2' }, ['Correo']),
      el('input', { id: 'email', type: 'email', placeholder: 'correo@dominio.com', className: 'input' }),
      el('label', { className: 'label mt-2' }, ['Contrasena']),
      el('input', { id: 'pass', type: 'password', placeholder: '********', className: 'input' }),
      el('div', { className: 'mt-2' }, [el('button', { id: 'btnReg', className: 'btn btn--primary' }, ['Crear cuenta'])]),
      el('p', { className: 'mt-2' }, [el('span', { className: 'link', onclick: () => { currentTab = 'login'; renderTab(); } }, ['<- Volver a Acceso'])]),
      el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
    ]);

    if (!deps.register) {
      qs('#msg', ui).textContent = 'El proveedor de autenticacion no esta disponible.';
    } else {
      ui.querySelector('#btnReg').addEventListener('click', async () => {
        try {
          const doc = ui.querySelector('#doc').value.trim();
          const name = ui.querySelector('#name').value.trim();
          const email = ui.querySelector('#email').value.trim();
          const pass = ui.querySelector('#pass').value;
          if (!doc || !name || !email || !pass) throw new Error('Completa documento, nombre, correo y contrasena.');
          const cred = await deps.register(email, pass, { nombre: name, documento: doc });
          if (cred?.session && cred?.user?.uid && deps.createUserProfile) {
            await deps.createUserProfile(cred.user.uid, { email, nombre: name, documento: doc });
            qs('#msg', ui).textContent = 'Cuenta creada. Comunicate con el administrador para que te asigne los permisos correspondientes.';
            currentTab = 'login';
            renderTab();
            return;
          }
          qs('#msg', ui).textContent = 'Cuenta creada. Revisa tu correo si aplica y luego inicia sesion. Comunicate con el administrador para que te asigne los permisos correspondientes.';
          currentTab = 'login';
          renderTab();
        } catch (e) {
          qs('#msg', ui).textContent = `Error al registrar: ${e?.message || e}`;
        }
      });
    }

    return ui;
  }

  renderTab();
  mount.replaceChildren(root);
};
