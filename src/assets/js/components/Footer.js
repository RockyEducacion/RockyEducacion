import { el } from '../utils/dom.js';
import { getRole } from '../permissions.js';

export const Footer = () => {
  const role = getRole();
  return (
  el('div', { className: 'container footer-shell' }, [
    el('div', { className: 'footer-brand' }, [
      el('img', {
        className: 'footer-brand__logo',
        src: 'src/assets/img/capcol-logo.png',
        alt: 'Capcol SAS',
        loading: 'lazy'
      }),
      el('div', { className: 'footer-brand__copy' }, [
        el('span', { className: 'footer-brand__eyebrow' }, ['Consultores en Administración Pública Colombiana S.A.S.']),
        el('span', { className: 'footer-brand__name' }, ['Plataforma Rocky'])
      ])
    ]),
    el('div', { className: 'footer-meta' }, [
      el('span', { className: 'role-badge footer-role-badge', title: 'Rol actual' }, ['Rol: ', role || '-']),
      el('p', { className: 'text-muted footer-copy' }, [
        '© 2026 CAPCOL S.A.S.'
      ])
    ])
  ]));
};
