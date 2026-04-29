import { el, qs } from '../utils/dom.js';
import { navigate } from '../router.js';
import { getState, subscribe } from '../state.js';
import { can, isSuperAdmin, PERMS } from '../permissions.js';

const MOBILE_BREAKPOINT = '(max-width: 900px)';

export const Sidebar = () => {
  const container = el('div', {});
  const brandTitle = el('strong', { className: 'sidebar__brand-name' }, ['ROCKY']);
  const brandSubtitle = el('span', { className: 'sidebar__brand-subtitle' }, ['Gestion operativa']);
  const brandText = el('div', { className: 'sidebar__brand-copy' }, [brandTitle, brandSubtitle]);
  const brandImg = el('img', {
    className: 'sidebar__logo',
    src: 'src/assets/img/rocky-logo.png',
    alt: 'Logo ROCKY',
    loading: 'lazy'
  });
  brandImg.addEventListener('error', () => {
    brandImg.classList.add('hidden');
    brandTitle.textContent = 'RockyEDU';
    brandSubtitle.textContent = 'Gestion operativa';
  });
  const top = el('div', { className: 'sidebar__top' }, [
    el('div', { className: 'sidebar__brand' }, [brandImg, brandText]),
    el('button', { className: 'btn sidebar__collapse-btn', id: 'btnCollapse', type: 'button', 'aria-label': 'Contraer sidebar' }, [
      el('span', { className: 'sidebar__collapse-btn-glyph', 'aria-hidden': 'true' }, [])
    ])
  ]);

  const sections = [];
  const { user, userProfile } = getState();

  if (user && userProfile) {
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
    if (bulkLinks.length) sections.push(section('Cargue masivo', bulkLinks, 'cargue_masivo'));

    const opLinks = [];
    if (can(PERMS.IMPORT_DATA)) opLinks.push(navLink('Registro Diario', '/registros-vivo'));
    if (can(PERMS.IMPORT_DATA)) opLinks.push(navLink('Registro Sede', '/registro-sede'));
    if (can(PERMS.VIEW_IMPORT_HISTORY)) opLinks.push(navLink('Historial', '/import-history'));
    if (can(PERMS.RUN_PAYROLL)) opLinks.push(navLink('Nomina', '/payroll'));
    if (can(PERMS.MANAGE_ABSENTEEISM)) opLinks.push(navLink('Ausentismo', '/absenteeism'));
    if (opLinks.length) sections.push(section('Operacion', opLinks, 'operacion'));

    const reportLinks = [];
    if (can(PERMS.VIEW_REPORTS_CLIENT)) reportLinks.push(navLink('Reportes Cliente', '/reports-client'));
    if (can(PERMS.VIEW_REPORTS_COMPANY)) reportLinks.push(navLink('Reportes Empresa', '/reports-company'));
    if (reportLinks.length) {
      sections.push(section('Reportes', reportLinks, 'reportes'));
    }

    if (can(PERMS.UPLOAD_DATA)) {
      sections.push(section('Incapacidades', [navLink('Incapacidades', '/upload')], 'carga_informacion'));
    }
  }

  container.replaceChildren(top, ...sections);
  bindSidebarBackdrop();
  ensureMobileSidebarState();

  const btn = qs('#btnCollapse', container);
  const initialCollapsed = getSidebarCollapsedPref();
  applySidebarCollapsed(initialCollapsed);

  const syncCollapseBtn = () => {
    const aside = document.getElementById('app-sidebar');
    const collapsed = aside?.getAttribute('data-collapsed') === 'true';
    btn.setAttribute('data-collapsed', collapsed ? 'true' : 'false');
    btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    btn.title = collapsed ? 'Expandir sidebar' : 'Contraer sidebar';
    btn.setAttribute('aria-label', btn.title);
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
    closeMobileSidebar();
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
    '/imports': 'WA',
    '/whatsapp-live': 'WA',
    '/registros-vivo': 'WA',
    '/registro-sede': 'RS',
    '/import-history': 'HI',
    '/payroll': 'NO',
    '/absenteeism': 'AU',
    '/reports': 'RP',
    '/reports-client': 'RC',
    '/reports-company': 'RE',
    '/upload': 'IN'
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

export function isMobileSidebarOpen() {
  const aside = document.getElementById('app-sidebar');
  return aside?.getAttribute('data-mobile-open') === 'true';
}

export function toggleMobileSidebar() {
  setMobileSidebarOpen(!isMobileSidebarOpen());
}

export function closeMobileSidebar() {
  setMobileSidebarOpen(false);
}

function setMobileSidebarOpen(open) {
  const aside = document.getElementById('app-sidebar');
  const layout = document.querySelector('.app-layout');
  const backdrop = document.getElementById('app-sidebar-backdrop');
  const mobileToggle = document.querySelector('.header-mobile-toggle');
  const next = open ? 'true' : 'false';
  if (aside) aside.setAttribute('data-mobile-open', next);
  if (layout) layout.setAttribute('data-sidebar-mobile-open', next);
  if (backdrop) {
    backdrop.hidden = !open;
    backdrop.setAttribute('aria-hidden', open ? 'false' : 'true');
  }
  if (mobileToggle) {
    mobileToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    mobileToggle.textContent = open ? '✕' : '☰';
    mobileToggle.title = open ? 'Cerrar menu' : 'Abrir menu';
    mobileToggle.setAttribute('aria-label', mobileToggle.title);
  }
  document.body.classList.toggle('sidebar-mobile-open', open);
  document.dispatchEvent(new CustomEvent('sidebar-mobile-statechange', { detail: { open } }));
}

function ensureMobileSidebarState() {
  if (!isMobileViewport()) {
    closeMobileSidebar();
    return;
  }
  setMobileSidebarOpen(false);
}

function bindSidebarBackdrop() {
  const backdrop = document.getElementById('app-sidebar-backdrop');
  if (!backdrop || backdrop.dataset.bound === '1') return;
  backdrop.dataset.bound = '1';
  backdrop.addEventListener('click', () => closeMobileSidebar());
  window.addEventListener('resize', () => {
    if (!isMobileViewport()) closeMobileSidebar();
  });
  window.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeMobileSidebar();
  });
}

function isMobileViewport() {
  try {
    return window.matchMedia(MOBILE_BREAKPOINT).matches;
  } catch (_) {
    return window.innerWidth <= 900;
  }
}
