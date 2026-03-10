import { el, qs } from '../utils/dom.js';
import { navigate } from '../router.js';
import { getState, subscribe } from '../state.js';
import { can, isSuperAdmin, PERMS } from '../permissions.js';

export const Sidebar = () => {
  const container = el('div', {});
  const brandText = el('span', { className: 'sidebar__brand-text' }, ['Rocky']);
  const brandImg = el('img', {
    className: 'sidebar__logo',
    src: 'src/assets/img/rocky-logo.png',
    alt: 'Logo Rocky',
    loading: 'lazy'
  });
  brandImg.addEventListener('error', () => {
    brandImg.classList.add('hidden');
    brandText.textContent = 'RockyEDU';
  });
  const top = el('div', { className: 'sidebar__top' }, [
    el('div', { className: 'sidebar__brand' }, [brandImg, brandText]),
    el('button', { className: 'btn sidebar__collapse-btn', id: 'btnCollapse', type: 'button', 'aria-label': 'Contraer sidebar' }, ['<<'])
  ]);

  const sections = [];
  const { user, userProfile } = getState();

  if (user && userProfile) {
    const role = String(userProfile?.role || '').trim().toLowerCase();
    const isSupervisor = role === 'supervisor';
    const govLinks = [];
    if (isSuperAdmin()) govLinks.push(navLink('Centro de Permisos', '/permissions'));
    if (can(PERMS.VIEW_USERS)) govLinks.push(navLink('Usuarios', '/users'));
    if (govLinks.length) sections.push(section('Gobierno', govLinks, 'gobierno'));

    const adminLinks = [];
    if (can(PERMS.VIEW_ZONES)) adminLinks.push(navLink('Zonas', '/zones'));
    if (can(PERMS.VIEW_DEPENDENCIES)) adminLinks.push(navLink('Dependencias', '/dependencies'));
    if (can(PERMS.VIEW_SEDES)) adminLinks.push(navLink('Sedes', '/sedes'));
    if (can(PERMS.VIEW_CARGOS)) adminLinks.push(navLink('Cargos', '/cargos'));
    if (can(PERMS.VIEW_NOVEDADES)) adminLinks.push(navLink('Novedades', '/novedades'));
    if (can(PERMS.VIEW_EMPLOYEES)) adminLinks.push(navLink('Empleados', '/employees'));
    if (can(PERMS.VIEW_SUPERVISORS)) adminLinks.push(navLink('Supervisores', '/supervisors'));
    if (can(PERMS.VIEW_SUPERNUMERARIOS)) adminLinks.push(navLink('Supernumerarios', '/supernumerarios'));
    if (adminLinks.length) sections.push(section('Administracion', adminLinks, 'administracion'));

    const bulkLinks = [];
    if (can(PERMS.EDIT_SEDES)) bulkLinks.push(navLink('Cargue sedes', '/bulk-upload-sedes'));
    if (can(PERMS.EDIT_EMPLOYEES)) bulkLinks.push(navLink('Cargue empleados', '/bulk-upload'));
    if (can(PERMS.EDIT_SUPERNUMERARIOS)) bulkLinks.push(navLink('Cargue supernumerarios', '/bulk-upload-supernumerarios'));
    if (bulkLinks.length) sections.push(section('Cargue masivo', bulkLinks, 'cargue_masivo'));

    const opLinks = [];
    if (can(PERMS.IMPORT_DATA) || isSupervisor) opLinks.push(navLink(isSupervisor ? 'Registro Diario Supervisor' : 'Registro Diario', '/registros-vivo'));
    if (can(PERMS.IMPORT_DATA)) opLinks.push(navLink('Registro Sede', '/registro-sede'));
    if (can(PERMS.VIEW_IMPORT_HISTORY)) opLinks.push(navLink('Historial', '/import-history'));
    if (can(PERMS.RUN_PAYROLL)) opLinks.push(navLink('Nomina', '/payroll'));
    if (can(PERMS.MANAGE_ABSENTEEISM)) opLinks.push(navLink('Ausentismo', '/absenteeism'));
    if (opLinks.length) sections.push(section('Operacion', opLinks, 'operacion'));

    if (can(PERMS.VIEW_REPORTS)) {
      sections.push(section('Reportes', [navLink('Reportes', '/reports')], 'reportes'));
    }

    if (can(PERMS.UPLOAD_DATA)) {
      sections.push(section('Carga de informacion', [navLink('Carga de datos', '/upload')], 'carga_informacion'));
    }
  }

  container.replaceChildren(top, ...sections);

  const btn = qs('#btnCollapse', container);
  const initialCollapsed = getSidebarCollapsedPref();
  applySidebarCollapsed(initialCollapsed);

  const syncCollapseBtn = () => {
    const aside = document.getElementById('app-sidebar');
    const collapsed = aside?.getAttribute('data-collapsed') === 'true';
    btn.textContent = collapsed ? '>>' : '<<';
    btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  };
  syncCollapseBtn();
  btn.addEventListener('click', () => {
    const aside = document.getElementById('app-sidebar');
    const collapsed = aside.getAttribute('data-collapsed') === 'true';
    const nextCollapsed = !collapsed;
    applySidebarCollapsed(nextCollapsed);
    setSidebarCollapsedPref(nextCollapsed);
    syncCollapseBtn();
  });

  const applyTheme = (t) => document.documentElement.setAttribute('data-theme', t);
  applyTheme(getState().theme);
  const unsub = subscribe('theme', applyTheme);
  container._cleanup = () => unsub?.();

  return container;
};

function section(title, links, key) {
  const pref = getSectionPref(key);
  const sec = el('div', { className: `sidebar__section${pref ? ' is-collapsed' : ''}` }, []);
  const titleBtn = el('button', {
    className: 'sidebar__section-title sidebar__section-toggle',
    type: 'button',
    'aria-expanded': pref ? 'false' : 'true'
  }, [title]);
  const nav = el('nav', { className: 'sidebar__nav' }, links);
  titleBtn.addEventListener('click', () => {
    const collapsed = sec.classList.toggle('is-collapsed');
    titleBtn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    setSectionPref(key, collapsed);
  });
  sec.append(titleBtn, nav);
  return sec;
}

function navLink(text, to) {
  const iconLabel = getNavIconLabel(to);
  const a = el('a', { href: `#${to}`, className: 'sidebar__nav-link' }, [
    el('span', { className: 'sidebar__item-icon', 'aria-hidden': 'true' }, [iconLabel]),
    el('span', { className: 'sidebar__item-text' }, [text])
  ]);
  a.title = text;
  a.setAttribute('aria-label', text);
  a.addEventListener('click', (e) => {
    e.preventDefault();
    navigate(to);
    document.querySelectorAll('.sidebar__nav-link').forEach((n) => n.classList.remove('is-active'));
    a.classList.add('is-active');
  });
  return a;
}

function getSectionPref(key) {
  try {
    return localStorage.getItem(`sidebar_sec_${key}`) === '1';
  } catch (_) {
    return false;
  }
}

function setSectionPref(key, collapsed) {
  try {
    localStorage.setItem(`sidebar_sec_${key}`, collapsed ? '1' : '0');
  } catch (_) {}
}

function getNavIconLabel(route) {
  const map = {
    '/permissions': 'CP',
    '/users': 'US',
    '/zones': 'ZN',
    '/dependencies': 'DP',
    '/sedes': 'SD',
    '/cargos': 'CG',
    '/novedades': 'NV',
    '/employees': 'EM',
    '/supervisors': 'SP',
    '/supernumerarios': 'SN',
    '/bulk-upload-sedes': 'BS',
    '/bulk-upload': 'BE',
    '/bulk-upload-supernumerarios': 'BN',
    '/imports': 'WA',
    '/whatsapp-live': 'WA',
    '/registros-vivo': 'WA',
    '/registro-sede': 'RS',
    '/import-history': 'HI',
    '/payroll': 'NO',
    '/absenteeism': 'AU',
    '/reports': 'RP',
    '/upload': 'CD'
  };
  return map[route] || '>>';
}

function getSidebarCollapsedPref() {
  try {
    return localStorage.getItem('sidebar_collapsed') === '1';
  } catch (_) {
    return false;
  }
}

function setSidebarCollapsedPref(collapsed) {
  try {
    localStorage.setItem('sidebar_collapsed', collapsed ? '1' : '0');
  } catch (_) {}
}

function applySidebarCollapsed(collapsed) {
  const aside = document.getElementById('app-sidebar');
  const layout = document.querySelector('.app-layout');
  if (aside) aside.setAttribute('data-collapsed', collapsed ? 'true' : 'false');
  if (layout) layout.setAttribute('data-sidebar-collapsed', collapsed ? 'true' : 'false');
}
