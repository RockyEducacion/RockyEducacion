import { el, qs } from '../utils/dom.js';
import { navigate } from '../router.js';
import { showActionModal } from '../utils/actionModal.js';

function clearRecoveryUrl(hashPath = '/login') {
  try {
    window.history.replaceState(null, '', `${window.location.pathname}#${hashPath}`);
  } catch {}
}

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
    const ui = el('form', { className: 'login-form' }, [
      el('label', { className: 'label mt-2' }, ['Correo']),
      el('input', { id: 'email', type: 'email', placeholder: 'correo@dominio.com', className: 'input', autocomplete: 'email' }),
      el('label', { className: 'label mt-2' }, ['Contrasena']),
      el('input', { id: 'pass', type: 'password', placeholder: '********', className: 'input', autocomplete: 'current-password' }),
      el('button', { id: 'btnForgotPassword', className: 'login-link', type: 'button' }, ['Olvide mi contrasena']),
      el('div', { className: 'form-row login-actions mt-2' }, [
        el('button', { id: 'btnLogin', className: 'btn btn--primary', type: 'submit' }, ['Iniciar sesion']),
        el('button', { id: 'btnOpenCreate', className: 'btn btn--primary', type: 'button' }, ['Crear cuenta'])
      ]),
      el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
    ]);

    const blocked = consumeBlockedMessage();
    if (blocked) qs('#msg', ui).textContent = blocked;

    if (!deps.login) {
      qs('#msg', ui).textContent = 'El proveedor de autenticacion no esta disponible.';
    } else {
      ui.addEventListener('submit', async (ev) => {
        ev.preventDefault();
        try {
          const email = ui.querySelector('#email').value.trim();
          const pass = ui.querySelector('#pass').value;
          if (!email || !pass) throw new Error('Ingresa correo y contrasena.');
          ui.querySelector('#btnLogin').disabled = true;
          await deps.login(email, pass);
          setMessage('Sesion iniciada.');
        } catch (e) {
          setMessage(`Error al iniciar sesion: ${e?.message || e}`);
        } finally {
          ui.querySelector('#btnLogin').disabled = false;
        }
      });
    }

    ui.querySelector('#btnForgotPassword').addEventListener('click', () => navigate('/forgot-password'));
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

export const ForgotPassword = (mount, deps = {}) => {
  const root = el('section', { className: 'main-card login-card' }, [
    el('h2', {}, ['Recuperar contrasena']),
    el('p', { className: 'auth-copy' }, ['Ingresa tu correo y te enviaremos un enlace para crear una nueva contrasena.']),
    el('form', { className: 'login-form' }, [
      el('label', { className: 'label mt-2' }, ['Correo']),
      el('input', { id: 'resetEmail', type: 'email', placeholder: 'correo@dominio.com', className: 'input', autocomplete: 'email' }),
      el('div', { className: 'form-row login-actions mt-2' }, [
        el('button', { id: 'btnSendReset', className: 'btn btn--primary', type: 'submit' }, ['Enviar enlace']),
        el('button', { id: 'btnBackLogin', className: 'btn', type: 'button' }, ['Volver'])
      ]),
      el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
    ])
  ]);

  const form = root.querySelector('form');
  const sendButton = qs('#btnSendReset', root);

  function showSuccess(email) {
    qs('#loginContent', root)?.remove();
    form.replaceChildren(
      el('div', { className: 'auth-state auth-state--success' }, [
        el('strong', {}, ['Revisa tu correo']),
        el('p', {}, [`Si ${email} esta registrado, recibira un enlace para continuar el proceso.`])
      ]),
      el('div', { className: 'form-row login-actions mt-2' }, [
        el('button', { id: 'btnBackLoginDone', className: 'btn btn--primary', type: 'button' }, ['Volver al inicio'])
      ])
    );
    qs('#btnBackLoginDone', root).addEventListener('click', () => navigate('/login'));
  }

  if (!deps.requestPasswordReset) {
    qs('#msg', root).textContent = 'El proveedor de autenticacion no esta disponible.';
  } else {
    form.addEventListener('submit', async (ev) => {
      ev.preventDefault();
      const email = qs('#resetEmail', root).value.trim();
      try {
        if (!email) throw new Error('Ingresa tu correo.');
        sendButton.disabled = true;
        await deps.requestPasswordReset(email);
        showSuccess(email);
      } catch (e) {
        qs('#msg', root).textContent = `No pudimos enviar el enlace: ${e?.message || e}`;
      } finally {
        sendButton.disabled = false;
      }
    });
  }

  qs('#btnBackLogin', root).addEventListener('click', () => navigate('/login'));
  mount.replaceChildren(root);
};

export const ResetPassword = (mount, deps = {}) => {
  const root = el('section', { className: 'main-card login-card' }, [
    el('h2', {}, ['Nueva contrasena']),
    el('p', { className: 'auth-copy' }, ['Crea una contrasena nueva para recuperar el acceso a tu cuenta.']),
    el('form', { className: 'login-form' }, [
      el('label', { className: 'label mt-2' }, ['Nueva contrasena']),
      el('input', { id: 'newPass', type: 'password', placeholder: 'Minimo 8 caracteres', className: 'input', autocomplete: 'new-password' }),
      el('label', { className: 'label mt-2' }, ['Confirmar contrasena']),
      el('input', { id: 'confirmPass', type: 'password', placeholder: 'Repite la contrasena', className: 'input', autocomplete: 'new-password' }),
      el('div', { className: 'auth-hint' }, ['Usa una contrasena que no hayas utilizado antes.']),
      el('div', { className: 'form-row login-actions mt-2' }, [
        el('button', { id: 'btnUpdatePassword', className: 'btn btn--primary', type: 'submit' }, ['Guardar contrasena']),
        el('button', { id: 'btnBackLogin', className: 'btn', type: 'button' }, ['Volver'])
      ]),
      el('p', { id: 'msg', className: 'text-muted mt-2' }, [' '])
    ])
  ]);

  const form = root.querySelector('form');
  const updateButton = qs('#btnUpdatePassword', root);

  function showSuccess() {
    form.replaceChildren(
      el('div', { className: 'auth-state auth-state--success' }, [
        el('strong', {}, ['Contrasena actualizada']),
        el('p', {}, ['Ya puedes iniciar sesion con tu nueva contrasena.'])
      ]),
      el('div', { className: 'form-row login-actions mt-2' }, [
        el('button', { id: 'btnBackLoginDone', className: 'btn btn--primary', type: 'button' }, ['Ir al login'])
      ])
    );
    qs('#btnBackLoginDone', root).addEventListener('click', async () => {
      clearRecoveryUrl('/login');
      try { await deps.logout?.(); } catch {}
      navigate('/login');
    });
  }

  if (!deps.updatePassword) {
    qs('#msg', root).textContent = 'El proveedor de autenticacion no esta disponible.';
  } else {
    form.addEventListener('submit', async (ev) => {
      ev.preventDefault();
      const pass = qs('#newPass', root).value;
      const confirm = qs('#confirmPass', root).value;
      try {
        if (pass.length < 8) throw new Error('La contrasena debe tener al menos 8 caracteres.');
        if (pass !== confirm) throw new Error('Las contrasenas no coinciden.');
        updateButton.disabled = true;
        await deps.updatePassword(pass);
        showSuccess();
      } catch (e) {
        qs('#msg', root).textContent = `No pudimos actualizar la contrasena: ${e?.message || e}`;
      } finally {
        updateButton.disabled = false;
      }
    });
  }

  qs('#btnBackLogin', root).addEventListener('click', () => {
    clearRecoveryUrl('/login');
    navigate('/login');
  });
  mount.replaceChildren(root);
};
