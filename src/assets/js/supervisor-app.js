import { Login } from './components/Login.js';
import { el, qs } from './utils/dom.js';
import { installBrowserAlertReplacement } from './utils/notifications.js';
import { ROLES } from './roles.js';

installBrowserAlertReplacement();

const root = document.getElementById('supervisor-root');
let deps = {};
let currentUser = null;
let currentProfile = null;
let currentRegistry = emptyRegistry(todayBogota());
let activeTab = 'home';
let selectedDate = todayBogota();
let searchText = '';
let statusFilter = 'all';
let sedeFilter = 'all';
let loading = false;
let lastLoadedAt = null;
let supernumerarios = [];
let unSupernumerarios = null;
let supernumerariosStarted = false;
let replacementSavingKey = '';
let replacementMessage = null;

function emptyRegistry(fecha) {
  return {
    fecha,
    zones: [],
    sedes: [],
    employees: [],
    dailyStatus: [],
    attendance: [],
    replacements: [],
    incapacities: [],
    closures: []
  };
}

(async function init() {
  try {
    const fb = await import('./supabase.js');
    deps = {
      authState: fb.authState,
      login: fb.login,
      register: fb.register,
      logout: fb.logout,
      ensureUserProfile: fb.ensureUserProfile,
      loadUserProfile: fb.loadUserProfile,
      createUserProfile: fb.createUserProfile,
      listSupervisorDailyRegistry: fb.listSupervisorDailyRegistry,
      listSupervisorAvailableSupernumerarios: fb.listSupervisorAvailableSupernumerarios,
      streamSupernumerarios: fb.streamSupernumerarios,
      saveImportReplacements: fb.saveImportReplacements
    };
    deps.authState(handleAuthState);
  } catch (error) {
    renderFatal(`No se pudo iniciar la app de supervisores: ${error?.message || error}`);
  }
})();

async function handleAuthState(user) {
  currentUser = user || null;
  if (!user) {
    currentProfile = null;
    stopSupernumerariosStream();
    renderLogin();
    return;
  }
  try {
    await deps.ensureUserProfile?.(user);
    const profile = await deps.loadUserProfile?.(user.uid);
    currentProfile = profile || null;
    const status = String(profile?.estado || 'activo').toLowerCase();
    if (status === 'inactivo' || status === 'eliminado') {
      try {
        sessionStorage.setItem('auth_block_msg', status === 'eliminado' ? 'Tu usuario fue eliminado. Contacta al administrador.' : 'Tu usuario esta inactivo. Contacta al administrador.');
      } catch {}
      await deps.logout?.();
      return;
    }
    if (String(profile?.role || '').toLowerCase() === ROLES.TABLET_QR) {
      window.location.replace('qr.html');
      return;
    }
    if (!canUseSupervisorApp(profile)) {
      renderDenied(profile);
      return;
    }
    startSupernumerariosStream();
    await loadRegistry();
  } catch (error) {
    renderFatal(`No se pudo validar tu acceso: ${error?.message || error}`);
  }
}

function startSupernumerariosStream() {
  if (unSupernumerarios || supernumerariosStarted) return;
  supernumerariosStarted = true;
  if (deps.listSupervisorAvailableSupernumerarios) {
    loadSupervisorSupernumerarios();
    return;
  }
  if (!deps.streamSupernumerarios) return;
  unSupernumerarios = deps.streamSupernumerarios((rows = []) => {
    setSupervisorSupernumerarios(rows);
  });
}

function stopSupernumerariosStream() {
  unSupernumerarios?.();
  unSupernumerarios = null;
  supernumerariosStarted = false;
  supernumerarios = [];
}

async function loadSupervisorSupernumerarios() {
  try {
    const rows = await deps.listSupervisorAvailableSupernumerarios?.();
    setSupervisorSupernumerarios(rows || []);
  } catch (error) {
    console.error('No se pudieron cargar supernumerarios para supervisor:', error);
    if (deps.streamSupernumerarios && !unSupernumerarios) {
      unSupernumerarios = deps.streamSupernumerarios((rows = []) => {
        setSupervisorSupernumerarios(rows);
      });
    }
  }
}

function setSupervisorSupernumerarios(rows = []) {
  supernumerarios = (rows || [])
    .filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo')
    .sort((a, b) => String(a.nombre || '').localeCompare(String(b.nombre || '')));
  if (currentUser && currentProfile) renderApp();
}

function canUseSupervisorApp(profile = {}) {
  const role = String(profile?.role || '').toLowerCase();
  if (role === ROLES.SUPERADMIN || role === ROLES.ADMIN) return true;
  return role === ROLES.SUPERVISOR && profile?.supervisorEligible === true;
}

function supervisorZones(profile = currentProfile) {
  const zones = [
    ...(Array.isArray(profile?.zonasPermitidas) ? profile.zonasPermitidas : []),
    profile?.zonaCodigo
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean);
  return [...new Set(zones)];
}

async function loadRegistry() {
  loading = true;
  renderApp();
  try {
    const zones = supervisorZones();
    currentRegistry = await deps.listSupervisorDailyRegistry?.(selectedDate, zones) || emptyRegistry(selectedDate);
    lastLoadedAt = new Date();
  } catch (error) {
    currentRegistry = { ...emptyRegistry(selectedDate), error: error?.message || String(error) };
  } finally {
    loading = false;
    renderApp();
  }
}

function renderLogin() {
  const shell = el('section', { className: 'supervisor-login' }, [
    el('div', { className: 'supervisor-login__inner' }, [
      el('div', { className: 'supervisor-login__brand' }, [
        el('img', { src: 'src/assets/img/rocky-logo.png', alt: 'Rocky' }),
        el('div', {}, [
          el('h1', {}, ['Supervisores']),
          el('p', {}, ['Registro diario por zona'])
        ])
      ]),
      el('div', { id: 'supervisor-login-mount' })
    ])
  ]);
  root.replaceChildren(shell);
  Login(qs('#supervisor-login-mount', shell), deps);
}

function renderDenied(profile = {}) {
  root.replaceChildren(el('section', { className: 'supervisor-denied' }, [
    el('article', { className: 'supervisor-denied__card' }, [
      el('h1', {}, ['Acceso de supervisores']),
      el('p', {}, ['Este ingreso esta reservado para usuarios supervisores activos y habilitados.']),
      el('p', {}, [`Usuario: ${profile?.email || currentUser?.email || '-'}`]),
      el('div', { className: 'supervisor-card__actions' }, [
        actionButton('Administrativo', () => { window.location.href = 'app.html#/login'; }),
        actionButton('Salir', async () => { await deps.logout?.(); }, true)
      ])
    ])
  ]));
}

function renderFatal(message) {
  root.replaceChildren(el('section', { className: 'supervisor-denied' }, [
    el('article', { className: 'supervisor-denied__card' }, [
      el('h1', {}, ['Rocky Supervisores']),
      el('p', {}, [message])
    ])
  ]));
}

function renderApp() {
  if (!currentUser || !currentProfile) return;
  const rows = buildRegistryRows();
  const registeredRows = rows.filter((row) => row.status !== 'pendiente');
  const filteredRows = filterRows(registeredRows);
  const noveltyRows = rows.filter((row) => row.hasNovelty || row.status === 'novedad' || row.status === 'ausente');
  const summary = summarizeRows(rows);
  const zoneLabel = supervisorZones().join(', ') || 'Sin zona';

  const app = el('div', { className: 'supervisor-app' }, [
    el('header', { className: 'supervisor-topbar' }, [
      el('div', { className: 'supervisor-brand' }, [
        el('img', { className: 'supervisor-brand__logo', src: 'src/assets/img/rocky-logo.png', alt: 'Rocky' }),
        el('div', { className: 'supervisor-brand__copy' }, [
          el('span', { className: 'supervisor-brand__eyebrow' }, ['Rocky']),
          el('strong', { className: 'supervisor-brand__name' }, [displayName()])
        ])
      ]),
      el('span', { className: 'supervisor-zone-pill', title: zoneLabel }, [zoneLabel])
    ]),
    el('main', { className: 'supervisor-main' }, [
      panel('home', [
        hero(summary, 'home'),
        kpiGrid(summary),
        sectionHead('Pendientes prioritarios', `${pendingRows(rows).length} pendientes`),
        listOrEmpty(pendingRows(rows).slice(0, 6), 'No hay pendientes para mostrar.')
      ]),
      panel('registry', [
        hero(summary, 'registry'),
        toolbar(),
        sectionHead('Registro diario', `${filteredRows.length} registros`),
        listOrEmpty(filteredRows, 'No hay registros con los filtros actuales.')
      ]),
      panel('novelties', [
        hero(summary, 'novelties'),
        sectionHead('Novedades y ausencias', `${noveltyRows.length} registros`),
        listOrEmpty(noveltyRows, 'No hay novedades registradas para esta fecha.')
      ]),
      panel('profile', [
        profilePanel(summary)
      ])
    ]),
    bottomNav()
  ]);
  root.replaceChildren(app);
}

function panel(name, children) {
  return el('section', { className: `supervisor-panel${activeTab === name ? ' is-active' : ''}`, dataset: { panel: name } }, children);
}

function hero(summary, section = 'home') {
  const message = sectionMessage(summary, section);
  return el('section', { className: 'supervisor-hero' }, [
    el('div', { className: 'supervisor-hero__top' }, [
      el('div', {}, [
        el('h1', { className: 'supervisor-title' }, [sectionTitle(section)]),
        el('p', { className: `supervisor-subtitle supervisor-subtitle--${message.tone}` }, [message.text])
      ]),
      el('div', { className: 'supervisor-date' }, [
        el('label', { for: 'supervisorDate' }, ['Fecha']),
        el('input', { id: 'supervisorDate', className: 'input', type: 'date', value: selectedDate, onchange: (event) => {
          selectedDate = event.target.value || todayBogota();
          loadRegistry();
        } })
      ])
    ]),
    el('div', { className: 'supervisor-sync' }, [
      el('span', {}, [loading ? 'Actualizando datos...' : lastLoadedAt ? `Actualizado ${formatTime(lastLoadedAt)}` : 'Listo para actualizar']),
      el('button', { className: 'btn supervisor-refresh', type: 'button', onclick: loadRegistry }, ['Actualizar'])
    ])
  ]);
}

function sectionTitle(section) {
  if (section === 'registry') return 'Registro diario';
  if (section === 'novelties') return 'Novedades';
  return 'Hoy en tu zona';
}

function sectionMessage(summary, section = 'home') {
  if (currentRegistry.error) return { tone: 'danger', text: `Error: ${currentRegistry.error}` };
  if (section === 'registry' || section === 'novelties') {
    const pending = Number(summary.noveltyPending || 0);
    if (pending > 0) {
      return { tone: 'danger', text: `Tienes ${pending} novedad${pending === 1 ? '' : 'es'} pendiente${pending === 1 ? '' : 's'} de gestionar.` };
    }
    return { tone: 'ok', text: 'No tienes novedades pendientes de gestionar.' };
  }
  return {
    tone: summaryTone(summary),
    text: summaryLabel(summary)
  };
}

function kpiGrid(summary) {
  return el('section', { className: 'supervisor-kpis' }, [
    kpi('Esperados', summary.expected, 'info'),
    kpi('Correctos', summary.present, 'ok'),
    kpi('Pendientes', summary.pending, 'warn'),
    kpi('Novedades', summary.novelties, 'danger')
  ]);
}

function kpi(label, value, tone) {
  return el('article', { className: `supervisor-kpi supervisor-kpi--${tone}` }, [
    el('span', { className: 'supervisor-kpi__label' }, [label]),
    el('strong', { className: 'supervisor-kpi__value' }, [String(value)])
  ]);
}

function toolbar() {
  const sedes = [...new Set(buildRegistryRows().map((row) => row.sedeNombre).filter(Boolean))].sort((a, b) => a.localeCompare(b));
  return el('section', { className: 'supervisor-toolbar' }, [
    el('input', {
      id: 'supervisorSearch',
      className: 'input',
      placeholder: 'Buscar nombre, documento o sede',
      value: searchText,
      oninput: (event) => {
        searchText = event.target.value || '';
        renderApp();
      }
    }),
    el('div', { className: 'supervisor-filter-row' }, [
      el('select', { className: 'select', value: statusFilter, onchange: (event) => {
        statusFilter = event.target.value || 'all';
        renderApp();
      } }, [
        option('all', 'Todos', statusFilter),
        option('presente', 'Correctos', statusFilter),
        option('novedad', 'Novedades', statusFilter),
        option('ausente', 'Ausentes', statusFilter)
      ]),
      el('select', { className: 'select', value: sedeFilter, onchange: (event) => {
        sedeFilter = event.target.value || 'all';
        renderApp();
      } }, [
        option('all', 'Todas las sedes', sedeFilter),
        ...sedes.map((sede) => option(sede, sede, sedeFilter))
      ])
    ])
  ]);
}

function option(value, label, selected) {
  return el('option', { value, selected: value === selected }, [label]);
}

function sectionHead(title, count) {
  return el('div', { className: 'supervisor-section-head' }, [
    el('h2', {}, [title]),
    el('span', { className: 'supervisor-count' }, [count])
  ]);
}

function listOrEmpty(rows, emptyText) {
  if (!rows.length) return el('p', { className: 'supervisor-empty' }, [emptyText]);
  return el('div', { className: 'supervisor-list' }, rows.map(recordCard));
}

function recordCard(row) {
  const phone = normalizePhone(row.telefono);
  const recordDetailLabel = row.hasNovelty || row.status === 'ausente' ? 'Novedad' : 'Registro';
  const recordDetailValue = row.hasNovelty || row.status === 'ausente'
    ? row.novedad || row.estadoDia || '-'
    : row.status === 'pendiente'
      ? 'Pendiente'
      : row.registro || 'Correcto';
  const cardDetails = [
    detail('Hora', row.hora || '-'),
    detail(recordDetailLabel, recordDetailValue),
    row.incapacidadDias ? detail('Dias incapacidad', `${row.incapacidadDias} dia${row.incapacidadDias === 1 ? '' : 's'}`) : null,
    detail('Zona', row.zonaNombre || row.zonaCodigo || '-'),
    detail('Cobertura', coverageLabel(row))
  ].filter(Boolean);
  const replacementNode = replacementControl(row);
  return el('article', { className: 'supervisor-card' }, [
    el('div', { className: 'supervisor-card__main' }, [
      el('div', {}, [
        el('h3', { className: 'supervisor-card__name' }, [row.nombre || 'Sin nombre']),
        el('p', { className: 'supervisor-card__meta' }, [`${row.documento || '-'} · ${row.sedeNombre || 'Sin sede'}`])
      ]),
      statusBadge(row)
    ]),
    el('div', { className: 'supervisor-card__details' }, cardDetails),
    replacementNode,
    el('div', { className: 'supervisor-card__actions' }, [
      phone ? linkAction('Llamar', `tel:${phone}`) : null,
      phone ? linkAction('WhatsApp', `https://wa.me/${phone}`) : null
    ].filter(Boolean))
  ].filter(Boolean));
}

function detail(label, value) {
  return el('div', { className: 'supervisor-detail' }, [
    el('span', {}, [label]),
    el('strong', {}, [String(value || '-')])
  ]);
}

function statusBadge(row) {
  const labels = {
    presente: 'Correcto',
    pendiente: 'Pendiente',
    novedad: 'Novedad',
    ausente: 'Ausente',
    neutral: 'Sin estado'
  };
  return el('span', { className: `supervisor-status supervisor-status--${row.status || 'neutral'}` }, [labels[row.status] || labels.neutral]);
}

function linkAction(label, href) {
  return el('a', { className: 'btn supervisor-action', href, target: href.startsWith('http') ? '_blank' : undefined, rel: href.startsWith('http') ? 'noopener' : undefined }, [label]);
}

function actionButton(label, onClick, primary = false) {
  return el('button', { className: `btn supervisor-action${primary ? ' btn--primary' : ''}`, type: 'button', onclick: onClick }, [label]);
}

function replacementControl(row) {
  if (!(row.hasNovelty || row.status === 'novedad' || row.status === 'ausente')) return null;
  if (!deps.saveImportReplacements) return null;
  if (hasSavedCoverage(row)) return null;
  const rowKey = replacementRowKey(row);
  const available = replacementOptionsForRow(row);
  const currentValue = row.replacementSupernumerarioId
    || (row.decisionCobertura === 'ausentismo' ? '__ausentismo__' : '');
  const select = el('select', { className: 'select supervisor-replacement-select' }, [
    el('option', { value: '' }, ['Seleccione cobertura...']),
    el('option', { value: '__ausentismo__', selected: currentValue === '__ausentismo__' }, ['Sin reemplazo']),
    ...available.map((item) => el('option', {
      value: item.id,
      selected: currentValue === item.id
    }, [`${item.nombre || item.documento || '-'} (${item.documento || '-'})`]))
  ]);
  const button = el('button', {
    className: 'btn btn--primary supervisor-replacement-save',
    type: 'button',
    disabled: replacementSavingKey === rowKey,
    onclick: () => saveSupervisorReplacement(row, select.value)
  }, [replacementSavingKey === rowKey ? 'Guardando...' : 'Guardar']);
  const message = replacementMessage?.key === rowKey
    ? el('p', { className: `supervisor-replacement-msg is-${replacementMessage.type || 'info'}` }, [replacementMessage.text || ''])
    : null;
  return el('div', { className: 'supervisor-replacement' }, [
    el('label', {}, ['Cobertura de novedad']),
    el('div', { className: 'supervisor-replacement__row' }, [select, button]),
    message
  ].filter(Boolean));
}

function hasSavedCoverage(row = {}) {
  return Boolean(row.replacementId);
}

function coverageLabel(row = {}) {
  if (row.reemplazo) return row.reemplazo;
  if (row.replacementId) return 'Sin reemplazo';
  const decision = String(row.decisionCobertura || '').trim().toLowerCase();
  if (decision === 'reemplazo') return 'Reemplazo guardado';
  if (decision === 'ausentismo') return 'Pendiente';
  return '-';
}

function replacementOptionsForRow(row = {}) {
  const rowDoc = String(row.documento || '').trim();
  const currentId = String(row.replacementSupernumerarioId || '').trim();
  const used = usedSupernumerarioIds(row);
  return (supernumerarios || []).filter((item) => {
    const id = String(item.id || '').trim();
    const doc = String(item.documento || '').trim();
    if (!id) return false;
    if (doc && doc === rowDoc) return false;
    if (id === currentId) return true;
    return !used.has(id);
  });
}

function usedSupernumerarioIds(currentRow = {}) {
  const currentEmployeeId = String(currentRow.employeeId || '').trim();
  const currentDoc = String(currentRow.documento || '').trim();
  const used = new Set();
  (currentRegistry.replacements || []).forEach((row) => {
    if (String(row.decision || '').trim() !== 'reemplazo') return;
    const superId = String(row.supernumerarioId || '').trim();
    if (!superId) return;
    const sameEmployee = currentEmployeeId && String(row.empleadoId || '').trim() === currentEmployeeId;
    const sameDoc = currentDoc && String(row.documento || '').trim() === currentDoc;
    if (!sameEmployee && !sameDoc) used.add(superId);
  });
  return used;
}

async function saveSupervisorReplacement(row = {}, selectedValue = '') {
  const rowKey = replacementRowKey(row);
  const value = String(selectedValue || '').trim();
  if (!value) {
    replacementMessage = { key: rowKey, type: 'error', text: 'Selecciona un supernumerario o sin reemplazo.' };
    renderApp();
    return;
  }
  const employeeId = String(row.employeeId || '').trim();
  if (!employeeId) {
    replacementMessage = { key: rowKey, type: 'error', text: 'No se encontro el empleado para guardar la cobertura.' };
    renderApp();
    return;
  }
  const selected = value === '__ausentismo__'
    ? null
    : (supernumerarios || []).find((item) => String(item.id || '').trim() === value) || null;
  if (value !== '__ausentismo__' && !selected) {
    replacementMessage = { key: rowKey, type: 'error', text: 'Selecciona un supernumerario valido.' };
    renderApp();
    return;
  }
  const assignment = {
    fecha: selectedDate,
    empleadoId: employeeId,
    documento: row.documento || null,
    nombre: row.nombre || null,
    sedeCodigo: row.sedeCodigo || null,
    sedeNombre: row.sedeNombre || null,
    novedadCodigo: row.novedadCodigo || null,
    novedadNombre: row.novedad || row.estadoDia || null,
    decision: selected ? 'reemplazo' : 'ausentismo',
    supernumerarioId: selected?.id || null,
    supernumerarioDocumento: selected?.documento || null,
    supernumerarioNombre: selected?.nombre || null
  };
  replacementSavingKey = rowKey;
  replacementMessage = { key: rowKey, type: 'info', text: 'Guardando cobertura...' };
  renderApp();
  try {
    await deps.saveImportReplacements?.({ fechaOperacion: selectedDate, assignments: [assignment] });
    replacementMessage = { key: rowKey, type: 'ok', text: 'Cobertura guardada.' };
    await loadRegistry();
  } catch (error) {
    replacementMessage = { key: rowKey, type: 'error', text: `Error guardando cobertura: ${error?.message || error}` };
    renderApp();
  } finally {
    replacementSavingKey = '';
    renderApp();
  }
}

function replacementRowKey(row = {}) {
  return `${String(row.employeeId || '').trim()}|${String(row.documento || '').trim()}|${selectedDate}`;
}

function bottomNav() {
  const items = [
    ['home', 'Inicio'],
    ['registry', 'Registros'],
    ['novelties', 'Novedades'],
    ['profile', 'Perfil']
  ];
  return el('nav', { className: 'supervisor-bottom-nav', 'aria-label': 'Navegacion supervisores' }, items.map(([key, label]) => (
    el('button', {
      className: `supervisor-nav-btn${activeTab === key ? ' is-active' : ''}`,
      type: 'button',
      onclick: () => {
        activeTab = key;
        renderApp();
      }
    }, [label])
  )));
}

function profilePanel(summary) {
  const zones = supervisorZones();
  return el('section', { className: 'supervisor-hero' }, [
    el('h1', { className: 'supervisor-title' }, ['Perfil']),
    el('p', { className: 'supervisor-subtitle' }, [currentProfile?.email || currentUser?.email || '-']),
    kpiGrid(summary),
    el('div', { className: 'supervisor-card__details' }, [
      detail('Rol', currentProfile?.role || '-'),
      detail('Zonas', zones.join(', ') || 'Sin zonas asignadas'),
      detail('Estado', currentProfile?.estado || 'activo'),
      detail('Fecha', selectedDate)
    ]),
    el('div', { className: 'supervisor-card__actions' }, [
      actionButton('Administrativo', () => { window.location.href = 'app.html#/login'; }),
      actionButton('Cerrar sesion', async () => { await deps.logout?.(); }, true)
    ])
  ]);
}

function buildRegistryRows() {
  const registry = currentRegistry || emptyRegistry(selectedDate);
  const employeesByDoc = new Map((registry.employees || []).map((emp) => [String(emp.documento || '').trim(), emp]));
  const employeesById = new Map((registry.employees || []).map((emp) => [String(emp.id || '').trim(), emp]));
  const attendanceByDoc = new Map();
  const attendanceByEmployee = new Map();
  (registry.attendance || []).forEach((row) => {
    if (row.documento) attendanceByDoc.set(String(row.documento).trim(), row);
    if (row.empleadoId) attendanceByEmployee.set(String(row.empleadoId).trim(), row);
  });
  const replacementByDoc = new Map((registry.replacements || []).map((row) => [String(row.documento || '').trim(), row]));
  const incapacityByDoc = new Map();
  const incapacityByEmployee = new Map();
  (registry.incapacities || []).forEach((row) => {
    const doc = String(row.documento || '').trim();
    const employeeId = String(row.employeeId || '').trim();
    if (doc && !incapacityByDoc.has(doc)) incapacityByDoc.set(doc, row);
    if (employeeId && !incapacityByEmployee.has(employeeId)) incapacityByEmployee.set(employeeId, row);
  });
  const rows = [];

  (registry.dailyStatus || []).forEach((status) => {
    const doc = String(status.documento || '').trim();
    const employee = employeesByDoc.get(doc) || employeesById.get(String(status.employeeId || '').trim()) || {};
    const attendance = attendanceByDoc.get(doc) || attendanceByEmployee.get(String(status.employeeId || '').trim()) || {};
    const replacement = replacementByDoc.get(doc) || {};
    const incapacity = incapacityByDoc.get(doc) || incapacityByEmployee.get(String(status.employeeId || '').trim()) || {};
    rows.push(normalizeRecord({ status, employee, attendance, replacement, incapacity }));
  });

  if (!rows.length) {
    (registry.employees || []).forEach((employee) => {
      const doc = String(employee.documento || '').trim();
      rows.push(normalizeRecord({
        employee,
        attendance: attendanceByDoc.get(doc) || attendanceByEmployee.get(String(employee.id || '').trim()) || {},
        replacement: replacementByDoc.get(doc) || {},
        incapacity: incapacityByDoc.get(doc) || incapacityByEmployee.get(String(employee.id || '').trim()) || {}
      }));
    });
  }

  const seenDocs = new Set(rows.map((row) => String(row.documento || '').trim()).filter(Boolean));
  (registry.attendance || []).forEach((attendance) => {
    const doc = String(attendance.documento || '').trim();
    if (doc && seenDocs.has(doc)) return;
    rows.push(normalizeRecord({
      attendance,
      employee: employeesByDoc.get(doc) || {},
      replacement: replacementByDoc.get(doc) || {},
      incapacity: incapacityByDoc.get(doc) || incapacityByEmployee.get(String(attendance.empleadoId || '').trim()) || {}
    }));
  });

  return rows.sort((a, b) => {
    const statusRank = { pendiente: 0, novedad: 1, ausente: 2, presente: 3, neutral: 4 };
    const rankDiff = (statusRank[a.status] ?? 9) - (statusRank[b.status] ?? 9);
    if (rankDiff) return rankDiff;
    return String(a.nombre || '').localeCompare(String(b.nombre || ''));
  });
}

function normalizeRecord({ status = {}, employee = {}, attendance = {}, replacement = {}, incapacity = {} }) {
  const doc = status.documento || employee.documento || attendance.documento || replacement.documento || null;
  const hasAttendance = Boolean(attendance.id || status.sourceAttendanceId || status.asistio === true);
  const rawNovelty = status.novedadNombre || attendance.novedadNombre || attendance.novedad || replacement.novedadNombre || null;
  const noveltyCode = status.novedadCodigo || attendance.novedadCodigo || replacement.novedadCodigo || null;
  const estadoDia = status.estadoDia || null;
  const operationalRecord = supervisorOperationalRecord(rawNovelty, noveltyCode, estadoDia);
  const novelty = operationalRecord ? null : supervisorNoveltyLabel(rawNovelty, noveltyCode, estadoDia);
  const normalizedNoveltyState = normalizeSupervisorText(estadoDia || novelty || '');
  const isIncapacity = Boolean(incapacity.id) || /incap/.test(normalizedNoveltyState);
  const isAbsent = /ausen|incap|permiso|retiro|vacacion/.test(normalizedNoveltyState);
  const incapacityDays = isIncapacity ? incapacityDaysForRecord(incapacity, selectedDate) : null;
  const hasNovelty = Boolean(novelty) || Boolean(replacement.id);
  let recordStatus = 'pendiente';
  if (hasAttendance) recordStatus = novelty ? 'novedad' : 'presente';
  else if (operationalRecord) recordStatus = 'presente';
  else if (isAbsent) recordStatus = 'ausente';
  else if (hasNovelty) recordStatus = 'novedad';
  return {
    id: status.id || employee.id || attendance.id || replacement.id || doc || cryptoRandomId(),
    employeeId: status.employeeId || employee.id || attendance.empleadoId || replacement.empleadoId || null,
    documento: doc,
    nombre: status.nombre || employee.nombre || attendance.nombre || replacement.nombre || null,
    telefono: employee.telefono || null,
    sedeCodigo: status.sedeCodigo || employee.sedeCodigo || attendance.sedeCodigo || replacement.sedeCodigo || null,
    sedeNombre: status.sedeNombreSnapshot || employee.sedeNombre || attendance.sedeNombre || replacement.sedeNombre || null,
    zonaCodigo: status.zonaCodigoSnapshot || employee.zonaCodigo || null,
    zonaNombre: status.zonaNombreSnapshot || employee.zonaNombre || null,
    estadoDia,
    novedadCodigo: noveltyCode,
    novedad: novelty,
    registro: operationalRecord || (recordStatus === 'presente' ? 'Correcto' : null),
    incapacidadDias: incapacityDays,
    incapacidadInicio: incapacity.fechaInicio || null,
    incapacidadFin: incapacity.fechaFin || null,
    hora: attendance.hora || formatTime(attendance.createdAt) || null,
    replacementId: replacement.id || null,
    reemplazo: replacement.supernumerarioNombre || status.reemplazadoPorNombre || null,
    replacementSupernumerarioId: replacement.supernumerarioId || status.reemplazadoPorEmployeeId || null,
    replacementSupernumerarioDocumento: replacement.supernumerarioDocumento || status.reemplazadoPorDocumento || null,
    decisionCobertura: replacement.decision || status.decisionCobertura || null,
    status: recordStatus,
    hasNovelty
  };
}

function supervisorOperationalRecord(value, code, estadoDia) {
  const normalizedCode = String(code || '').trim();
  const text = normalizeSupervisorText(value || estadoDia || '');
  if (text === 'ok') return 'OK';
  if (normalizedCode === '1' || text === 'trabajando') return 'Trabajando';
  if (normalizedCode === '7' || text === 'compensatorio') return 'Compensatorio';
  return null;
}

function supervisorNoveltyLabel(value, code, estadoDia) {
  const normalizedCode = String(code || '').trim();
  const text = String(value || '').trim();
  if (text && normalizedCode !== '1' && normalizedCode !== '7') return text;
  const state = normalizeSupervisorText(estadoDia || '');
  if (!state || state === 'sin_registro' || state === 'trabajando' || state === 'compensatorio' || state === 'ok') return null;
  return formatSupervisorState(estadoDia);
}

function normalizeSupervisorText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();
}

function formatSupervisorState(value) {
  const text = String(value || '').replace(/_/g, ' ').trim().toLowerCase();
  if (!text) return '';
  return text.replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function incapacityDaysForRecord(incapacity = {}, referenceDate = selectedDate) {
  const start = normalizeIsoDate(incapacity.fechaInicio);
  const end = normalizeIsoDate(incapacity.fechaFin);
  const refDate = normalizeIsoDate(referenceDate);
  if (!start || !end || end < start) return null;
  const effectiveStart = refDate && refDate > start ? refDate : start;
  return inclusiveDaysBetween(effectiveStart, end);
}

function inclusiveDaysBetween(startDate, endDate) {
  const start = normalizeIsoDate(startDate);
  const end = normalizeIsoDate(endDate);
  if (!start || !end || end < start) return null;
  const [sy, sm, sd] = start.split('-').map((n) => Number(n));
  const [ey, em, ed] = end.split('-').map((n) => Number(n));
  const startUtc = Date.UTC(sy, (sm || 1) - 1, sd || 1);
  const endUtc = Date.UTC(ey, (em || 1) - 1, ed || 1);
  return Math.floor((endUtc - startUtc) / 86400000) + 1;
}

function normalizeIsoDate(value) {
  const iso = String(value || '').slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(iso) ? iso : null;
}

function filterRows(rows) {
  const term = searchText.trim().toLowerCase();
  return rows.filter((row) => {
    if (statusFilter !== 'all' && row.status !== statusFilter) return false;
    if (sedeFilter !== 'all' && row.sedeNombre !== sedeFilter) return false;
    if (!term) return true;
    return [row.nombre, row.documento, row.sedeNombre, row.novedad, row.zonaNombre]
      .join(' ')
      .toLowerCase()
      .includes(term);
  });
}

function summarizeRows(rows) {
  return {
    expected: rows.length,
    present: rows.filter((row) => row.status === 'presente').length,
    pending: rows.filter((row) => row.status === 'pendiente').length,
    novelties: rows.filter((row) => row.hasNovelty || row.status === 'novedad' || row.status === 'ausente').length,
    noveltyPending: rows.filter((row) => isNoveltyPendingManagement(row)).length
  };
}

function isNoveltyPendingManagement(row = {}) {
  return (row.hasNovelty || row.status === 'novedad' || row.status === 'ausente')
    && !hasSavedCoverage(row);
}

function pendingRows(rows) {
  return rows.filter((row) => row.status === 'pendiente');
}

function summaryLabel(summary) {
  if (!supervisorZones().length) return 'No tienes zonas asignadas todavia.';
  if (!summary.expected) return 'No hay registros cargados para esta fecha.';
  if (summary.pending > 0) return `Hay ${summary.pending} personas pendientes por revisar.`;
  return 'El registro diario de tus zonas esta al dia.';
}

function summaryTone(summary) {
  if (!supervisorZones().length || !summary.expected) return 'neutral';
  if (summary.pending > 0) return 'warn';
  return 'ok';
}

function displayName() {
  return currentProfile?.displayName || currentProfile?.email || currentUser?.email || 'Supervisor';
}

function normalizePhone(value) {
  const digits = String(value || '').replace(/\D+/g, '');
  if (!digits) return '';
  if (digits.startsWith('57')) return digits;
  if (digits.length === 10) return `57${digits}`;
  return digits;
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Bogota',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date());
}

function formatTime(value) {
  try {
    const date = value instanceof Date ? value : value ? new Date(value) : null;
    if (!date || Number.isNaN(date.getTime())) return '';
    return date.toLocaleTimeString('es-CO', { hour: '2-digit', minute: '2-digit', hour12: false });
  } catch {
    return '';
  }
}

function cryptoRandomId() {
  try {
    return crypto.randomUUID();
  } catch {
    return `tmp_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }
}
