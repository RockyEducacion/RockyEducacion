import { el, qs } from '../utils/dom.js';
import { navigate } from '../router.js';
import { getState, subscribe } from '../state.js';
import { can, isSuperAdmin, PERMS } from '../permissions.js';

const MOBILE_BREAKPOINT = '(max-width: 900px)';

export const Sidebar = (deps = {}) => {
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
    brandTitle.textContent = 'RockyDEMO';
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
    if (can(PERMS.VIEW_QR_SCANNER)) adminLinks.push(navLink('Lector QR', '/lector-qr'));
    if (can(PERMS.MANAGE_QR_DEVICES)) adminLinks.push(navLink('Tablets QR', '/tablets-qr'));
    if (can(PERMS.VIEW_CARGOS)) adminLinks.push(navLink('Cargos', '/cargos'));
    if (can(PERMS.VIEW_NOVEDADES)) adminLinks.push(navLink('Novedades', '/novedades'));
    if (adminLinks.length) sections.push(section('Administracion', adminLinks, 'administracion'));

    const employeeLinks = [];
    if (can(PERMS.VIEW_EMPLOYEES)) employeeLinks.push(navLink('Empleados', '/employees'));
    if (can(PERMS.VIEW_EMPLOYEES)) employeeLinks.push(navLink('Novedades empleados', '/employee-novelties'));
    if (can(PERMS.VIEW_SUPERVISORS)) employeeLinks.push(navLink('Supervisores', '/supervisors'));
    if (can(PERMS.UPLOAD_DATA)) employeeLinks.push(navLink('Incapacidades', '/upload'));
    if (employeeLinks.length) sections.push(section('Empleados', employeeLinks, 'empleados'));

    const opLinks = [];
    if (can(PERMS.IMPORT_DATA)) opLinks.push(navLink('Registro Diario', '/registros-vivo', { badgeId: 'sidebarRegistroDiarioBadge' }));
    if (can(PERMS.VIEW_QR_DAILY_REGISTRY)) opLinks.push(navLink('Registro QR', '/registro-qr'));
    if (can(PERMS.VIEW_SUPERNUMERARIOS)) opLinks.push(navLink('Supernumerarios', '/supernumerarios', { badgeId: 'sidebarSupernumerariosFreeBadge', badgeAlwaysVisible: true, badgeAriaLabel: '0 supernumerarios libres hoy' }));
    if (can(PERMS.IMPORT_DATA)) opLinks.push(navLink('Registro Sede', '/registro-sede'));
    if (can(PERMS.VIEW_IMPORT_HISTORY)) opLinks.push(navLink('Historial', '/import-history'));
    if (opLinks.length) sections.push(section('Operacion', opLinks, 'operacion'));

    const reportLinks = [];
    const dailyReportLinks = [];
    if (can(PERMS.VIEW_REPORTS_CLIENT)) dailyReportLinks.push(navLink('Historico Registro Diario', '/reports-daily-history'));
    if (can(PERMS.MANAGE_ABSENTEEISM)) dailyReportLinks.push(navLink('Ausentismo', '/absenteeism'));
    if (dailyReportLinks.length) reportLinks.push(subSection('Reportes diarios', dailyReportLinks, 'reportes_diarios'));
    if (can(PERMS.VIEW_REPORTS_COMPANY)) reportLinks.push(navLink('Reportes consolidados', '/reports-consolidated'));
    if (reportLinks.length) {
      sections.push(section('Reportes', reportLinks, 'reportes'));
    }

    const bulkLinks = [];
    if (can(PERMS.EDIT_SEDES)) bulkLinks.push(navLink('Cargue sedes', '/bulk-upload-sedes'));
    if (can(PERMS.EDIT_EMPLOYEES)) bulkLinks.push(navLink('Cargue empleados', '/bulk-upload'));
    if (bulkLinks.length) sections.push(section('Cargue masivo', bulkLinks, 'cargue_masivo'));

  }

  container.replaceChildren(top, ...sections);
  scheduleLucideIcons();
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
  const unPendingBadge = bindPendingNoveltyBadge(container, deps);
  const unFreeSupernumerariosBadge = bindFreeSupernumerariosBadge(container, deps);
  container._cleanup = () => {
    unsub?.();
    unPendingBadge?.();
    unFreeSupernumerariosBadge?.();
  };

  return container;
};

function section(title, links, key) {
  const pref = getSectionPref(key);
  const meta = getSectionIconMeta(key);
  const sec = el('div', { className: `sidebar__section${pref ? ' is-collapsed' : ''}` }, []);
  const titleBtn = el('button', {
    className: 'sidebar__section-title sidebar__section-toggle',
    type: 'button',
    'aria-expanded': pref ? 'false' : 'true'
  }, [
    el('span', { className: 'sidebar__section-title-content' }, [
      lucideIcon(meta.icon, meta.fallback, 'sidebar__section-icon'),
      el('span', {}, [title])
    ])
  ]);
  const nav = el('nav', { className: 'sidebar__nav' }, links);
  titleBtn.addEventListener('click', () => {
    const collapsed = sec.classList.toggle('is-collapsed');
    titleBtn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    setSectionPref(key, collapsed);
  });
  sec.append(titleBtn, nav);
  return sec;
}

function subSection(title, links, key) {
  const pref = getSectionPref(`sub_${key}`);
  const meta = getSubsectionIconMeta(key);
  const sec = el('div', { className: `sidebar__subsection${pref ? ' is-collapsed' : ''}` }, []);
  const titleBtn = el('button', {
    className: 'sidebar__subsection-title sidebar__subsection-toggle',
    type: 'button',
    'aria-expanded': pref ? 'false' : 'true'
  }, [
    el('span', { className: 'sidebar__subsection-title-content' }, [
      lucideIcon(meta.icon, meta.fallback, 'sidebar__subsection-icon'),
      el('span', {}, [title])
    ])
  ]);
  const nav = el('nav', { className: 'sidebar__subnav' }, links);
  titleBtn.addEventListener('click', () => {
    const collapsed = sec.classList.toggle('is-collapsed');
    titleBtn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    setSectionPref(`sub_${key}`, collapsed);
  });
  sec.append(titleBtn, nav);
  return sec;
}

function navLink(text, to, options = {}) {
  const iconMeta = getNavIconMeta(to);
  const textNode = el('span', { className: 'sidebar__item-text' }, [text]);
  const content = options.badgeId
    ? el('span', { className: 'sidebar__item-content' }, [
      textNode,
      el('span', {
        id: options.badgeId,
        className: `sidebar__nav-badge${options.badgeClassName ? ` ${options.badgeClassName}` : ''}`,
        hidden: options.badgeAlwaysVisible ? false : true,
        'aria-label': options.badgeAriaLabel || '0 novedades pendientes'
      }, ['0'])
    ])
    : textNode;
  const a = el('a', { href: `#${to}`, className: 'sidebar__nav-link' }, [
    lucideIcon(iconMeta.icon, iconMeta.fallback, 'sidebar__item-icon'),
    content
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

function lucideIcon(iconName, fallback, className) {
  return el('span', { className, 'aria-hidden': 'true' }, [
    el('span', { className: 'sidebar__icon-fallback' }, [fallback || '']),
    el('i', { className: 'sidebar__icon-svg', 'data-lucide': iconName || 'circle' }, [])
  ]);
}

function scheduleLucideIcons(attempt = 0) {
  requestAnimationFrame(() => {
    if (globalThis.lucide?.createIcons) {
      globalThis.lucide.createIcons({
        attrs: {
          'stroke-width': 2,
          width: 18,
          height: 18
        }
      });
      document.documentElement.classList.add('has-lucide-icons');
      return;
    }
    if (attempt < 8) setTimeout(() => scheduleLucideIcons(attempt + 1), 120);
  });
}

function bindPendingNoveltyBadge(container, deps = {}) {
  const badge = qs('#sidebarRegistroDiarioBadge', container);
  if (!badge || typeof deps.listEmployeeDailyStatusRange !== 'function') return () => {};

  let active = true;
  let refreshTimer = null;
  const unsubs = [];
  let employees = [];
  let sedes = [];
  let supernumerarios = [];
  let novedades = [];

  const setCount = (count) => {
    if (!active) return;
    const value = Math.max(0, Number(count || 0));
    badge.hidden = value <= 0;
    badge.textContent = value > 99 ? '99+' : String(value);
    const label = `${value} novedad${value === 1 ? '' : 'es'} pendiente${value === 1 ? '' : 's'} de gestionar`;
    badge.setAttribute('aria-label', label);
    const link = badge.closest('.sidebar__nav-link');
    if (link) {
      link.title = value > 0 ? `Registro Diario - ${label}` : 'Registro Diario';
      link.setAttribute('aria-label', link.title);
    }
  };

  const refresh = async () => {
    const day = todayBogota();
    try {
      const [statusRows, attendanceRows, replacementRows] = await Promise.all([
        deps.listEmployeeDailyStatusRange(day, day),
        deps.listAttendanceRange?.(day, day) || [],
        deps.listImportReplacementsRange?.(day, day) || []
      ]);
      if (!active) return;
      const pending = countPendingManagedNovelties({
        day,
        statusRows,
        attendanceRows,
        replacementRows,
        employees,
        sedes,
        supernumerarios,
        novedades
      });
      setCount(pending);
    } catch (error) {
      if (!active) return;
      setCount(0);
      console.warn('No se pudo actualizar la burbuja de novedades pendientes:', error);
    }
  };

  const scheduleRefresh = () => {
    if (refreshTimer) clearTimeout(refreshTimer);
    refreshTimer = setTimeout(refresh, 250);
  };

  refresh();
  if (typeof deps.streamAttendanceByDate === 'function') {
    unsubs.push(deps.streamAttendanceByDate(todayBogota(), scheduleRefresh, scheduleRefresh));
  }
  if (typeof deps.streamImportReplacementsByDate === 'function') {
    unsubs.push(deps.streamImportReplacementsByDate(todayBogota(), scheduleRefresh, scheduleRefresh));
  }
  if (typeof deps.streamDailyMetricsByDate === 'function') {
    unsubs.push(deps.streamDailyMetricsByDate(todayBogota(), scheduleRefresh, scheduleRefresh));
  }
  if (typeof deps.streamEmployees === 'function') {
    unsubs.push(deps.streamEmployees((rows) => { employees = rows || []; scheduleRefresh(); }));
  }
  if (typeof deps.streamSedes === 'function') {
    unsubs.push(deps.streamSedes((rows) => { sedes = rows || []; scheduleRefresh(); }));
  }
  if (typeof deps.streamSupernumerarios === 'function') {
    unsubs.push(deps.streamSupernumerarios((rows) => { supernumerarios = rows || []; scheduleRefresh(); }));
  }
  if (typeof deps.streamNovedades === 'function') {
    unsubs.push(deps.streamNovedades((rows) => { novedades = rows || []; scheduleRefresh(); }));
  }

  return () => {
    active = false;
    if (refreshTimer) clearTimeout(refreshTimer);
    unsubs.forEach((un) => un?.());
  };
}

function bindFreeSupernumerariosBadge(container, deps = {}) {
  const badge = qs('#sidebarSupernumerariosFreeBadge', container);
  if (!badge) return () => {};
  badge.hidden = false;
  badge.textContent = '0';
  if (typeof deps.streamSupernumerarios !== 'function') return () => {};

  let active = true;
  let refreshTimer = null;
  const unsubs = [];
  let supernumerarios = [];
  let incapacitados = [];
  let occupancyRows = [];

  const setCount = (count) => {
    if (!active) return;
    const value = Math.max(0, Number(count || 0));
    badge.hidden = false;
    badge.classList.toggle('sidebar__nav-badge--ok', value > 0);
    badge.textContent = value > 99 ? '99+' : String(value);
    const label = `${value} supernumerario${value === 1 ? '' : 's'} libre${value === 1 ? '' : 's'} hoy`;
    badge.setAttribute('aria-label', label);
    const link = badge.closest('.sidebar__nav-link');
    if (link) {
      link.title = value > 0 ? `Supernumerarios - ${label}` : 'Supernumerarios';
      link.setAttribute('aria-label', link.title);
    }
  };

  const refreshCount = () => {
    const day = todayBogota();
    const incapKeys = supernumerarioIncapacityKeys(incapacitados);
    const occupiedKeys = supernumerarioOccupancyKeys(occupancyRows);
    const free = (supernumerarios || []).filter((row) => {
      if (!isAvailableSupernumerarioForBadge(row, day)) return false;
      const keys = supernumerarioPersonKeys(row);
      if (keys.some((key) => incapKeys.has(key))) return false;
      if (keys.some((key) => occupiedKeys.has(key))) return false;
      return true;
    }).length;
    setCount(free);
  };

  const refreshOccupancy = async () => {
    const day = todayBogota();
    try {
      if (typeof deps.listSupernumerarioReplacementOccupancy === 'function') {
        occupancyRows = await deps.listSupernumerarioReplacementOccupancy(day) || [];
      } else if (typeof deps.listImportReplacementsRange === 'function') {
        occupancyRows = (await deps.listImportReplacementsRange(day, day) || [])
          .filter((row) => String(row?.decision || '').trim() === 'reemplazo');
      }
      refreshCount();
    } catch (error) {
      console.warn('No se pudo actualizar la burbuja de supernumerarios libres:', error);
      occupancyRows = [];
      refreshCount();
    }
  };

  const scheduleRefresh = () => {
    if (refreshTimer) clearTimeout(refreshTimer);
    refreshTimer = setTimeout(refreshOccupancy, 250);
  };

  unsubs.push(deps.streamSupernumerarios((rows) => {
    supernumerarios = rows || [];
    refreshCount();
  }));
  if (typeof deps.streamIncapacitadosByDate === 'function') {
    unsubs.push(deps.streamIncapacitadosByDate(todayBogota(), (rows) => {
      incapacitados = rows || [];
      refreshCount();
    }));
  }
  if (typeof deps.streamImportReplacementsByDate === 'function') {
    unsubs.push(deps.streamImportReplacementsByDate(todayBogota(), scheduleRefresh, scheduleRefresh));
  }
  refreshOccupancy();

  return () => {
    active = false;
    if (refreshTimer) clearTimeout(refreshTimer);
    unsubs.forEach((un) => un?.());
  };
}

function isAvailableSupernumerarioForBadge(row = {}, day = '') {
  const estado = String(row?.estado || 'activo').trim().toLowerCase();
  if (estado === 'inactivo' || estado === 'eliminado') return false;
  return isPersonActiveForBadgeDate(row, day);
}

function supernumerarioPersonKeys(row = {}) {
  return [
    String(row?.id || row?.employeeId || '').trim() ? `id:${String(row?.id || row?.employeeId || '').trim()}` : '',
    String(row?.documento || '').trim() ? `doc:${String(row?.documento || '').trim()}` : ''
  ].filter(Boolean);
}

function supernumerarioIncapacityKeys(rows = []) {
  const keys = new Set();
  (rows || []).forEach((row) => {
    const id = String(row?.employeeId || '').trim();
    const doc = String(row?.documento || '').trim();
    if (id) keys.add(`id:${id}`);
    if (doc) keys.add(`doc:${doc}`);
  });
  return keys;
}

function supernumerarioOccupancyKeys(rows = []) {
  const keys = new Set();
  (rows || []).forEach((row) => {
    if (String(row?.decision || 'reemplazo').trim() !== 'reemplazo') return;
    const id = String(row?.supernumerarioId || '').trim();
    const doc = String(row?.supernumerarioDocumento || '').trim();
    if (id) keys.add(`id:${id}`);
    if (doc) keys.add(`doc:${doc}`);
  });
  return keys;
}

function isPendingManagedNovelty(row = {}) {
  return String(row?.tipoPersonal || '').trim() === 'empleado'
    && row?.servicioProgramado === true
    && String(row?.decisionCobertura || '').trim() === 'pendiente';
}

function countPendingManagedNovelties({
  day,
  statusRows = [],
  attendanceRows = [],
  replacementRows = [],
  employees = [],
  sedes = [],
  supernumerarios = [],
  novedades = []
} = {}) {
  const pendingStatusKeys = new Set();
  (statusRows || []).filter(isPendingManagedNovelty).forEach((row) => {
    const key = dailyPersonKey(row);
    if (key) pendingStatusKeys.add(key);
  });

  const statusByKey = new Map();
  (statusRows || []).forEach((row) => {
    const key = dailyPersonKey(row);
    if (key) statusByKey.set(key, row);
  });

  const handledReplacementKeys = new Set();
  (replacementRows || []).forEach((row) => {
    const decision = String(row?.decision || '').trim();
    if (!['reemplazo', 'ausentismo'].includes(decision)) return;
    const key = dailyPersonKey(row);
    if (key) handledReplacementKeys.add(key);
  });
  handledReplacementKeys.forEach((key) => pendingStatusKeys.delete(key));

  const pendingAttendanceKeys = new Set();
  (attendanceRows || []).forEach((row) => {
    const key = dailyPersonKey(row);
    if (!key || handledReplacementKeys.has(key)) return;
    if (isSupernumerarioAttendanceForBadge(row, supernumerarios, day)) return;
    if (!isAttendanceReplacementNovelty(row, novedades)) return;
    if (!rowHasScheduledServiceForBadge(row, statusByKey, employees, sedes, day)) return;
    pendingAttendanceKeys.add(key);
  });

  return new Set([...pendingStatusKeys, ...pendingAttendanceKeys]).size;
}

function dailyPersonKey(row = {}) {
  const fecha = String(row?.fecha || row?.fechaOperacion || '').trim();
  const employeeId = String(row?.employeeId || row?.empleadoId || '').trim();
  const documento = String(row?.documento || '').trim();
  if (!fecha || (!employeeId && !documento)) return '';
  return `${fecha}|${employeeId || `doc:${documento}`}`;
}

function isAttendanceReplacementNovelty(row = {}, novedades = []) {
  const raw = String(row?.novedadNombre || row?.novedad || '').trim();
  const code = String(row?.novedadCodigo || (/^\d+$/.test(raw) ? raw : '')).trim();
  if ((!raw && !code) || code === '1' || raw === '1' || code === '7') return false;
  if (['2', '3', '4', '5', '8', '9'].includes(code)) return true;
  const normalizedRaw = normalizeBadgeText(baseNovedadName(raw || code));
  if (!normalizedRaw || normalizedRaw.startsWith('otra sede')) return false;

  const catalog = (novedades || []).find((item) => {
    const itemName = normalizeBadgeText(item?.nombre || '');
    const itemCode = normalizeBadgeText(item?.codigoNovedad || item?.codigo || '');
    return (code && itemCode === normalizeBadgeText(code))
      || (normalizedRaw && (
        (itemName && (itemName === normalizedRaw || itemName.includes(normalizedRaw) || normalizedRaw.includes(itemName)))
        || (itemCode && itemCode === normalizedRaw)
      ));
  });
  if (catalog) return ['si', 'yes', 'true', '1', 'reemplazo'].includes(normalizeBadgeText(catalog?.reemplazo || ''));

  return normalizedRaw.includes('incapacidad')
    || normalizedRaw.includes('accidente laboral')
    || normalizedRaw.includes('calamidad')
    || normalizedRaw.includes('vacaciones')
    || normalizedRaw.includes('permiso no remunerado');
}

function rowHasScheduledServiceForBadge(row = {}, statusByKey = new Map(), employees = [], sedes = [], day = '') {
  const status = statusByKey.get(dailyPersonKey(row));
  if (status) return status.servicioProgramado === true;
  const employeeId = String(row?.empleadoId || row?.employeeId || '').trim();
  const documento = String(row?.documento || '').trim();
  const employee = (employees || []).find((item) => {
    if (employeeId && String(item?.id || '').trim() === employeeId) return true;
    return documento && String(item?.documento || '').trim() === documento;
  });
  return employee ? isEmployeeExpectedForBadgeDate(employee, day, sedes) : false;
}

function isSupernumerarioAttendanceForBadge(row = {}, supernumerarios = [], day = '') {
  const doc = String(row?.documento || '').trim();
  if (!doc) return false;
  return (supernumerarios || []).some((item) => {
    if (String(item?.documento || '').trim() !== doc) return false;
    return isPersonActiveForBadgeDate(item, day);
  });
}

function isEmployeeExpectedForBadgeDate(employee = {}, day = '', sedes = []) {
  if (!isPersonActiveForBadgeDate(employee, day)) return false;
  const sedeCodigo = String(employee?.sedeCodigo || '').trim();
  if (!sedeCodigo) return false;
  const sede = (sedes || []).find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
  return isSedeScheduledForBadgeDate(sede, day);
}

function isPersonActiveForBadgeDate(person = {}, day = '') {
  const ingreso = toBadgeIsoDate(person?.fechaIngreso);
  if (!ingreso || ingreso > day) return false;
  const retiro = toBadgeIsoDate(person?.fechaRetiro);
  const estado = String(person?.estado || 'activo').trim().toLowerCase();
  if (estado === 'inactivo') return Boolean(retiro && retiro >= day);
  if (estado === 'eliminado') return false;
  return !retiro || retiro >= day;
}

function isSedeScheduledForBadgeDate(sede = null, day = '') {
  if (!sede || !day) return false;
  const [year, month, date] = day.split('-').map((value) => Number(value));
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, date || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
  if (jornada === 'lun_sab') return weekday >= 1 && weekday <= 6;
  return weekday >= 1 && weekday <= 5;
}

function baseNovedadName(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return '';
  const noParens = raw.replace(/\s*\(.*\)\s*$/, '').trim();
  if (/^OTRA\s+SEDE\s*:/i.test(noParens)) return 'OTRA SEDE';
  return noParens;
}

function normalizeBadgeText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function toBadgeIsoDate(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const raw = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? '' : parsed.toISOString().slice(0, 10);
  }
  const parsed = value?.toDate ? value.toDate() : (value instanceof Date ? value : null);
  return parsed && !Number.isNaN(parsed.getTime()) ? parsed.toISOString().slice(0, 10) : '';
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
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

function getSectionIconMeta(key) {
  const map = {
    gobierno: { icon: 'shield-check', fallback: 'GO' },
    administracion: { icon: 'building-2', fallback: 'AD' },
    empleados: { icon: 'users', fallback: 'EM' },
    operacion: { icon: 'clipboard-check', fallback: 'OP' },
    reportes: { icon: 'bar-chart-3', fallback: 'RP' },
    cargue_masivo: { icon: 'upload', fallback: 'CM' }
  };
  return map[key] || { icon: 'folder', fallback: '>>' };
}

function getSubsectionIconMeta(key) {
  const map = {
    reportes_diarios: { icon: 'calendar-days', fallback: 'D' }
  };
  return map[key] || { icon: 'folder-open', fallback: '-' };
}

function getNavIconMeta(route) {
  const map = {
    '/permissions': { icon: 'shield-check', fallback: 'CP' },
    '/users': { icon: 'settings', fallback: 'US' },
    '/zones': { icon: 'map', fallback: 'ZN' },
    '/dependencies': { icon: 'network', fallback: 'DP' },
    '/sedes': { icon: 'building-2', fallback: 'SD' },
    '/cargos': { icon: 'briefcase', fallback: 'CG' },
    '/novedades': { icon: 'list-checks', fallback: 'NV' },
    '/employees': { icon: 'badge', fallback: 'EM' },
    '/employee-novelties': { icon: 'history', fallback: 'NE' },
    '/supervisors': { icon: 'user-check', fallback: 'SP' },
    '/supernumerarios': { icon: 'user-plus', fallback: 'SN' },
    '/bulk-upload-sedes': { icon: 'building', fallback: 'BS' },
    '/bulk-upload': { icon: 'file-up', fallback: 'BE' },
    '/imports': { icon: 'message-circle', fallback: 'WA' },
    '/whatsapp-live': { icon: 'message-circle', fallback: 'WA' },
    '/registros-vivo': { icon: 'message-circle', fallback: 'WA' },
    '/registro-sede': { icon: 'clipboard-list', fallback: 'RS' },
    '/lector-qr': { icon: 'scan-line', fallback: 'QR' },
    '/tablets-qr': { icon: 'tablet', fallback: 'TQ' },
    '/registro-qr': { icon: 'qr-code', fallback: 'RQ' },
    '/import-history': { icon: 'clock-3', fallback: 'HI' },
    '/absenteeism': { icon: 'user-x', fallback: 'AU' },
    '/reports': { icon: 'bar-chart-3', fallback: 'RP' },
    '/reports-client': { icon: 'bar-chart-3', fallback: 'RC' },
    '/reports-company': { icon: 'pie-chart', fallback: 'RE' },
    '/reports-daily-history': { icon: 'history', fallback: 'HR' },
    '/reports-consolidated': { icon: 'layers', fallback: 'RC' },
    '/upload': { icon: 'file-heart', fallback: 'IN' }
  };
  return map[route] || { icon: 'circle', fallback: '>>' };
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
