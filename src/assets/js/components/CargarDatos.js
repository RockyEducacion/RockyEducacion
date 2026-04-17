import { el } from '../utils/dom.js';

export const CargarDatos = (mount) => {
  mount.replaceChildren(el('section', { className: 'section-block' }, [
    el('h3', { className: 'section-title' }, ['Carga de datos']),
    el('p', { className: 'text-muted' }, ['Este modulo quedo reservado como la unica seccion operativa del portal de empleados.']),
    el('div', { className: 'employee-panel' }, [
      el('article', { className: 'employee-stat' }, [
        el('p', { className: 'employee-stat__label' }, ['Estado']),
        el('p', { className: 'employee-stat__value' }, ['Modulo en proxima iteracion'])
      ]),
      el('article', { className: 'employee-stat' }, [
        el('p', { className: 'employee-stat__label' }, ['Alcance previsto']),
        el('p', { className: 'employee-stat__value' }, ['Registro y actualizacion de datos operativos del empleado autenticado'])
      ])
    ])
  ]));
};
