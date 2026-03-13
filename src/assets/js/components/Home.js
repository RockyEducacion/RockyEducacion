import { el } from '../utils/dom.js';
import { getState } from '../state.js';

export const Home = async (mount) => {
  const profile = getState()?.userProfile || {};
  const displayName = String(profile?.displayName || profile?.email || 'usuario').trim();
  const role = String(profile?.role || 'sin rol').trim();

  const ui = el('section', { className: 'main-card' }, [
    el('div', {
      className: 'section-block',
      style: [
        'background:linear-gradient(135deg,#f8fafc 0%,#e0f2fe 52%,#ecfccb 100%)',
        'border:1px solid #dbeafe',
        'padding:1.5rem'
      ].join(';')
    }, [
      el('p', {
        className: 'text-muted',
        style: 'margin:0 0 .35rem 0; letter-spacing:.08em; text-transform:uppercase;'
      }, ['RockyEducacion']),
      el('h2', { style: 'margin:0; font-size:2rem; line-height:1.05;' }, ['Bienvenido']),
      el('p', { style: 'margin:.75rem 0 0 0; font-size:1rem;' }, [`Hola, ${displayName}.`]),
      el('p', { className: 'text-muted', style: 'margin:.35rem 0 0 0;' }, [`Rol actual: ${role}.`]),
      el('p', { className: 'mt-2', style: 'max-width:760px; margin-bottom:0;' }, [
        'El dashboard operativo se encuentra en ajuste para asegurar estabilidad y consistencia de las metricas. ',
        'Por ahora puedes continuar usando los modulos operativos y administrativos del sistema.'
      ])
    ])
  ]);

  mount.replaceChildren(ui);
  return () => {};
};
