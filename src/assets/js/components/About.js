import { el } from '../utils/dom.js';

export const About = (mount) => {
  mount.replaceChildren(
    el('section', { className: 'main-card' }, [
      el('h2', {}, ['Acerca de RockyEDU']),
      el('p', { className: 'text-muted mt-1' }, [
        'Plataforma de gestion operativa y administrativa para el seguimiento de servicios, personal y novedades.'
      ]),
      el('div', { className: 'contact-grid mt-2' }, [
        el('article', { className: 'contact-card' }, [
          el('h3', { className: 'contact-card__title' }, ['Informacion corporativa']),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['Version: ']), 'RockyEDU v1.2.3']),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['Titularidad: ']), 'CAPCOL S.A.S.']),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['NIT: ']), '900.939.656-7']),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['Derechos: ']), 'Todos los derechos reservados'])
        ]),
        el('article', { className: 'contact-card' }, [
          el('h3', { className: 'contact-card__title' }, ['Canales de contacto']),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['Direccion: ']), 'Calle 20 # 18-62, Caramanta, Antioquia, Colombia']),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['Telefono: ']), '3502624742']),
          el('p', { className: 'contact-card__row' }, [
            el('strong', {}, ['Correo: ']),
            el('a', { href: 'mailto:capcol@capcol.com.co' }, ['capcol@capcol.com.co'])
          ]),
          el('p', { className: 'contact-card__row mt-2' }, [el('strong', {}, ['Soporte:'])]),
          el('p', { className: 'contact-card__row' }, [el('strong', {}, ['Telefono soporte: ']), '3502624743']),
          el('p', { className: 'contact-card__row' }, [
            el('strong', {}, ['Correo soporte: ']),
            el('a', { href: 'mailto:soporte@capcol.com.co' }, ['soporte@capcol.com.co'])
          ])
        ])
      ])
    ])
  );
};
