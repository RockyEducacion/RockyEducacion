import { el, qs } from '../utils/dom.js';
import { showActionModal } from '../utils/actionModal.js';

export const Login = (mount, deps = {}) => {
  const root = el('section', { className: 'main-card login-card' }, [
    el('h2', {}, ['Acceso']),
    el('div', { id: 'loginContent', className: 'mt-2' }, [])
  ]);

  function consumeBlockedMessage() {
    try {
      const txt = sessionStorage.getItem('auth_block_msg');
      if (txt) sessionStorage.removeItem('auth_block_msg');
      return txt || '';
    } catch {
      return '';
    }
  }

  function setMessage(text) {
    const msg = qs('#msg', root);
    if (msg) msg.textContent = text || ' ';
  }

  function loginForm() {
    const ui = el('div', { className: 'login-form' }, [
      el('label', { className: 'label mt-2' }, ['Correo']),
      el('input', { id: 'email', type: 'email', placeholder: 'correo@dominio.com', className: 'input' }),
      el('label', { className: 'label mt-2' }, ['Contrasena']),
      el('input', { id: 'pass', type: 'password', placeholder: '********', className: 'input' }),
      el('div', { className: 'form-row login-actions mt-2' }, [
        el('button', { id: 'btnLogin', className: 'btn btn--primary', type: 'button' }, ['Iniciar sesion']),
        el('button', { id: 'btnOpenCreate', className: 'btn btn--primary', type: 'button' }, ['Crear cuenta'])
      ]),
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
          setMessage('Sesion iniciada.');
        } catch (e) {
          setMessage(`Error al iniciar sesion: ${e?.message || e}`);
        }
      });
    }

    ui.querySelector('#btnOpenCreate').addEventListener('click', openRegisterModal);
    return ui;
  }

  async function openRegisterModal() {
    if (!deps.register) {
      setMessage('El proveedor de autenticacion no esta disponible.');
      return;
    }

    const modal = await showActionModal({
      title: 'Crear cuenta',
      message: 'Completa la informacion para crear tu cuenta.',
      confirmText: 'Crear cuenta',
      fields: [
        { id: 'doc', label: 'Documento', type: 'text', required: true, placeholder: 'Numero de documento' },
        { id: 'name', label: 'Nombre completo', type: 'text', required: true, placeholder: 'Tu nombre y apellidos' },
        { id: 'email', label: 'Correo', type: 'email', required: true, placeholder: 'correo@dominio.com' },
        { id: 'pass', label: 'Contrasena', type: 'password', required: true, placeholder: '********' }
      ]
    });
    if (!modal.confirmed) return;

    try {
      const doc = String(modal.values.doc || '').trim();
      const name = String(modal.values.name || '').trim();
      const email = String(modal.values.email || '').trim();
      const pass = String(modal.values.pass || '');
      if (!doc || !name || !email || !pass) throw new Error('Completa documento, nombre, correo y contrasena.');
      const cred = await deps.register(email, pass, { nombre: name, documento: doc });
      if (cred?.session && cred?.user?.uid && deps.createUserProfile) {
        await deps.createUserProfile(cred.user.uid, { email, nombre: name, documento: doc });
        setMessage('Cuenta creada. Comunicate con el administrador para que te asigne los permisos correspondientes.');
        return;
      }
      setMessage('Cuenta creada. Inicia sesion y comunicate con el administrador para que te asigne los permisos correspondientes.');
    } catch (e) {
      setMessage(`Error al registrar: ${e?.message || e}`);
    }
  }

  qs('#loginContent', root).replaceChildren(loginForm());
  mount.replaceChildren(root);
};
