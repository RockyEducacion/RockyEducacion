import { el, qs } from '../utils/dom.js';
import { getState } from '../state.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';

const SOURCE_OPTIONS = [
  'Enfermedad General',
  'Accidente Laboral',
  'Calamidad',
  'Licencia No Remunerada',
  'Licencia Remunerada',
  'Vacaciones',
  'Incapacidad'
];

const CHANNEL_LABELS = {
  whatsapp: 'WhatsApp',
  portal_web: 'Portal empleados',
  manual: 'Manual'
};

export const CargarDatos = (mount, deps = {}) => {
  const portalMode = typeof deps.apiRequest === 'function';
  const profile = getState()?.userProfile || {};
  const role = String(profile?.role || '').trim().toLowerCase();
  const ownDocument = portalMode
    ? sanitizeDocument(deps?.portalSession?.documento)
    : sanitizeDocument(profile?.documento);
  const canManageAll = !portalMode && role !== 'empleado';
  const today = todayBogota();

  let employees = [];
  let sedes = [];
  let incapRows = [];
  let editingId = null;
  let unEmployees = () => {};
  let unSedes = () => {};
  let hasQueried = false;
  let queryError = '';

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Incapacidades']),
    el('p', { className: 'text-muted' }, [
      portalMode
        ? 'Adjunta el soporte de la incapacidad que registraste por WhatsApp.'
        : 'Consulta y administra las incapacidades reportadas por WhatsApp y por la web de empleados.'
    ]),
    el('div', { className: 'tabs mt-2 hidden' }, [
      el('button', { id: 'incTabCreateBtn', className: 'tab', type: 'button' }, ['Registrar']),
      el('button', { id: 'incTabListBtn', className: 'tab is-active', type: 'button' }, ['Consultar'])
    ]),
    el('div', { id: 'incTabCreate', className: 'hidden' }, [
      el('div', { className: 'employee-panel' }, [
        el('div', { className: 'form-row' }, buildCreateFields(portalMode, canManageAll)),
        el('p', { id: 'incIdentityHint', className: 'text-muted mt-1' }, [' ']),
        el('span', { id: 'incCreateMsg', className: 'text-muted' }, [' '])
      ])
    ]),
    el('div', { id: 'incTabList' }, [
      el('div', { className: 'form-row' }, [
        el('div', {}, [
          el('label', { className: 'label', htmlFor: 'incQueryStart' }, ['Fecha inicio']),
          el('input', { id: 'incQueryStart', className: 'input', type: 'date', value: today })
        ]),
        el('div', {}, [
          el('label', { className: 'label', htmlFor: 'incQueryEnd' }, ['Fecha fin']),
          el('input', { id: 'incQueryEnd', className: 'input', type: 'date', value: today })
        ]),
        el('div', {}, [
          el('label', { className: 'label', htmlFor: 'incSearch' }, ['Buscar']),
          el('input', { id: 'incSearch', className: 'input', placeholder: 'Documento, nombre, origen o soporte...' })
        ]),
        el('div', {}, [
          el('label', { className: 'label', htmlFor: 'incStatus' }, ['Estado']),
          el('select', { id: 'incStatus', className: 'select' }, [
            el('option', { value: '' }, ['Todos']),
            el('option', { value: 'activo' }, ['Vigentes']),
            el('option', { value: 'inactivo' }, ['Anuladas'])
          ])
        ]),
        el('div', {}, [
          el('label', { className: 'label', htmlFor: 'incChannel' }, ['Canal']),
          el('select', { id: 'incChannel', className: 'select' }, [
            el('option', { value: '' }, ['Todos']),
            el('option', { value: 'whatsapp' }, ['WhatsApp']),
            el('option', { value: 'portal_web' }, ['Portal empleados']),
            el('option', { value: 'manual' }, ['Manual'])
          ])
        ]),
        el('div', {}, [
          el('label', { className: 'label', htmlFor: 'incSupportFilter' }, ['Soporte']),
          el('select', { id: 'incSupportFilter', className: 'select' }, [
            el('option', { value: '' }, ['Todos']),
            el('option', { value: 'with' }, ['Con soporte']),
            el('option', { value: 'without' }, ['Sin soporte'])
          ])
        ]),
        el('button', { id: 'incQueryBtn', className: 'btn btn--primary', type: 'button' }, ['Buscar']),
        el('span', { id: 'incListMeta', className: 'right text-muted' }, ['Cargando incapacidades...'])
      ]),
      el('div', { className: 'mt-2 table-wrap' }, [
        el('table', { className: 'table', id: 'incTable' }, [
          el('thead', {}, [
            el('tr', {}, [
              el('th', { 'data-sort': 'documento', style: 'cursor:pointer' }, ['Documento']),
              el('th', { 'data-sort': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
              el('th', { 'data-sort': 'source', style: 'cursor:pointer' }, ['Tipo']),
              el('th', { 'data-sort': 'fechaInicio', style: 'cursor:pointer' }, ['Inicio']),
              el('th', { 'data-sort': 'fechaFin', style: 'cursor:pointer' }, ['Fin']),
              el('th', { 'data-sort': 'canalRegistro', style: 'cursor:pointer' }, ['Canal']),
              el('th', { 'data-sort': 'soporte', style: 'cursor:pointer' }, ['Soporte']),
              el('th', { 'data-sort': 'estado', style: 'cursor:pointer' }, ['Estado']),
              el('th', {}, ['Acciones'])
            ])
          ]),
          el('tbody', {})
        ])
      ])
    ])
  ]);

  const paginator = createTablePagination(ui, { id: 'incapacidades', after: '#incTabList .table-wrap', onChange: renderList });

  mount.replaceChildren(ui);

  const tabCreateBtn = qs('#incTabCreateBtn', ui);
  const tabListBtn = qs('#incTabListBtn', ui);
  const tabCreate = qs('#incTabCreate', ui);
  const tabList = qs('#incTabList', ui);
  const employeeInput = qs('#incEmployeeSearch', ui);
  const employeeList = qs('#incEmployeeList', ui);
  const sourceSelect = qs('#incSource', ui);
  const startInput = qs('#incStart', ui);
  const endInput = qs('#incEnd', ui);
  const fileInput = qs('#incSupport', ui);
  const saveBtn = qs('#incSaveBtn', ui);
  const resetBtn = qs('#incResetBtn', ui);
  const createMsg = qs('#incCreateMsg', ui);
  const identityHint = qs('#incIdentityHint', ui);
  const queryStartInput = qs('#incQueryStart', ui);
  const queryEndInput = qs('#incQueryEnd', ui);
  const queryBtn = qs('#incQueryBtn', ui);
  const searchInput = qs('#incSearch', ui);
  const statusFilter = qs('#incStatus', ui);
  const channelFilter = qs('#incChannel', ui);
  const supportFilter = qs('#incSupportFilter', ui);
  const listMeta = qs('#incListMeta', ui);
  const tbody = ui.querySelector('tbody');
  let sortKey = 'fechaInicio';
  let sortDir = -1;

  qs('.tabs', ui)?.classList.add('hidden');
  tabCreate.classList.add('hidden');
  tabList.classList.remove('hidden');
  const btnOpenCreate = portalMode
    ? null
    : el('button', { id: 'incOpenCreate', className: 'btn btn--primary right', type: 'button' }, ['Crear incapacidad']);
  if (btnOpenCreate) qs('#incTabList .form-row', ui)?.append(btnOpenCreate);

  function setTab(which) {
    const create = which === 'create';
    tabCreateBtn.classList.toggle('is-active', create);
    tabListBtn.classList.toggle('is-active', !create);
    tabCreate.classList.toggle('hidden', !create);
    tabList.classList.toggle('hidden', create);
  }

  tabCreateBtn.addEventListener('click', () => setTab('create'));
  tabListBtn.addEventListener('click', () => setTab('list'));

  searchInput?.addEventListener('input', () => { paginator.reset(); renderList(); });
  statusFilter?.addEventListener('change', () => { paginator.reset(); renderList(); });
  channelFilter?.addEventListener('change', () => { paginator.reset(); renderList(); });
  supportFilter?.addEventListener('change', () => { paginator.reset(); renderList(); });
  queryBtn?.addEventListener('click', runQuery);
  saveBtn?.addEventListener('click', onSave);
  resetBtn?.addEventListener('click', resetForm);
  btnOpenCreate?.addEventListener('click', () => openIncapacityModal());
  initSorting();

  if (!portalMode && typeof deps.streamEmployees === 'function') {
    unEmployees = deps.streamEmployees((rows) => {
      employees = rows || [];
      renderEmployeeOptions();
      refreshIdentityHint();
      renderList();
    });
  }
  if (!portalMode && typeof deps.streamSedes === 'function') {
    unSedes = deps.streamSedes((rows) => {
      sedes = rows || [];
      renderList();
    });
  }

  refreshIdentityHint();
  renderList();
  Promise.resolve().then(() => runQuery());

  return () => {
    unEmployees?.();
    unSedes?.();
  };

  async function runQuery() {
    const dateFrom = String(queryStartInput?.value || '').trim();
    const dateTo = String(queryEndInput?.value || '').trim();
    if (!dateFrom || !dateTo) {
      listMeta.textContent = 'Selecciona el rango de fechas a consultar.';
      return;
    }
    if (dateTo < dateFrom) {
      listMeta.textContent = 'La fecha final no puede ser menor a la inicial.';
      return;
    }
    listMeta.textContent = 'Consultando incapacidades...';
    try {
      queryError = '';
      if (portalMode) {
        const data = await deps.apiRequest(`/api/employee-incapacities?dateFrom=${encodeURIComponent(dateFrom)}&dateTo=${encodeURIComponent(dateTo)}`, { method: 'GET' });
        incapRows = Array.isArray(data?.rows) ? data.rows : [];
      } else {
        incapRows = await deps.listIncapacidadesRange?.(dateFrom, dateTo) || [];
      }
      hasQueried = true;
      paginator.reset();
      renderList();
    } catch (error) {
      incapRows = [];
      hasQueried = true;
      paginator.reset();
      queryError = `Error: ${error?.message || error}`;
      renderList();
    }
  }

  function renderEmployeeOptions() {
    if (!employeeList) return;
    const items = employees
      .filter((row) => String(row?.estado || 'activo').trim().toLowerCase() === 'activo')
      .map((row) => employeeLabel(row))
      .filter((value, index, all) => value && all.indexOf(value) === index)
      .map((value) => el('option', { value }));
    employeeList.replaceChildren(...items);
  }

  function currentFixedEmployee() {
    if (portalMode) {
      return {
        employeeId: deps?.portalSession?.employeeId || null,
        documento: ownDocument || null,
        nombre: String(deps?.portalSession?.nombre || 'Empleado').trim() || 'Empleado'
      };
    }
    if (!ownDocument) return null;
    const matched = employees.find((row) => sanitizeDocument(row?.documento) === ownDocument);
    return {
      employeeId: matched?.id || null,
      documento: ownDocument,
      nombre: matched?.nombre || String(profile?.displayName || 'Empleado').trim() || 'Empleado'
    };
  }

  function refreshIdentityHint() {
    const fixed = !canManageAll ? currentFixedEmployee() : resolveEmployeeInput(employeeInput?.value || '');
    if (!fixed?.documento) {
      identityHint.textContent = canManageAll
        ? 'Selecciona un empleado para registrar o editar la incapacidad.'
        : 'No fue posible resolver el empleado autenticado.';
      return;
    }
    identityHint.textContent = `Empleado objetivo: ${fixed.nombre || '-'} (${fixed.documento || '-'})`;
  }

  function resolveEmployeeInput(value) {
    const raw = String(value || '').trim();
    if (!raw) return null;
    const rawDoc = sanitizeDocument(raw);
    const byDoc = employees.find((row) => sanitizeDocument(row?.documento) === rawDoc);
    if (byDoc) return normalizeEmployeeInfo(byDoc);
    const byLabel = employees.find((row) => employeeLabel(row).toLowerCase() === raw.toLowerCase());
    if (byLabel) return normalizeEmployeeInfo(byLabel);
    const byName = employees.find((row) => String(row?.nombre || '').trim().toLowerCase() === raw.toLowerCase());
    return byName ? normalizeEmployeeInfo(byName) : null;
  }

  function normalizeEmployeeInfo(row = {}) {
    return {
      employeeId: row.id || null,
      documento: sanitizeDocument(row.documento),
      nombre: String(row.nombre || '').trim() || row.documento || 'Empleado'
    };
  }

  async function onSave() {
    const target = canManageAll ? resolveEmployeeInput(employeeInput?.value || '') : currentFixedEmployee();
    const fechaInicio = String(startInput?.value || '').trim();
    const fechaFin = String(endInput?.value || '').trim();
    const supportFile = fileInput?.files?.[0] || null;
    const source = String(sourceSelect?.value || 'Enfermedad General').trim() || 'Enfermedad General';

    setMessage(createMsg, ' ');
    refreshIdentityHint();

    if (!target?.documento) {
      setMessage(createMsg, 'Selecciona un empleado valido.', 'error');
      return;
    }
    if (!fechaInicio || !fechaFin) {
      setMessage(createMsg, 'Selecciona fecha de inicio y fecha de terminacion.', 'error');
      return;
    }
    if (fechaFin < fechaInicio) {
      setMessage(createMsg, 'La fecha de terminacion no puede ser menor a la fecha de inicio.', 'error');
      return;
    }
    if (!editingId && !supportFile) {
      setMessage(createMsg, 'Adjunta el soporte de la incapacidad.', 'error');
      return;
    }

    saveBtn.disabled = true;
    resetBtn.disabled = true;
    setMessage(createMsg, editingId ? 'Actualizando incapacidad...' : 'Registrando incapacidad...', 'ok');

    try {
      if (portalMode) {
        if (!supportFile) throw new Error('Adjunta el soporte de la incapacidad.');
        const supportDataUrl = await fileToDataUrl(supportFile);
        const result = await deps.apiRequest('/api/employee-incapacities', {
          method: 'POST',
          body: JSON.stringify({
            fechaInicio,
            fechaFin,
            source,
            soporte: {
              name: supportFile.name,
              dataUrl: supportDataUrl
            }
          })
        });
        if (result?.row) incapRows = [result.row, ...incapRows];
        resetForm();
        setTab('list');
        if (hasQueried) await runQuery();
        renderList();
        setMessage(createMsg, 'Incapacidad registrada correctamente.', 'ok');
        return;
      }

      const currentRow = editingId ? incapRows.find((row) => row.id === editingId) || null : null;
      let supportInfo = {
        url: currentRow?.soporteUrl || null,
        name: currentRow?.soporteNombre || null,
        mimeType: currentRow?.soporteTipo || null,
        path: currentRow?.soporteStoragePath || null
      };
      if (supportFile && typeof deps.uploadIncapacidadSupport === 'function') {
        supportInfo = await deps.uploadIncapacidadSupport(supportFile, {
          documento: target.documento,
          employeeId: target.employeeId
        });
      }

      const payload = {
        fechaInicio,
        fechaFin,
        source
      };

      if (editingId) {
        if (target.employeeId) payload.employeeId = target.employeeId;
        if (target.documento) payload.documento = target.documento;
        if (target.nombre) payload.nombre = target.nombre;
        if (supportFile) {
          payload.soporteUrl = supportInfo.url;
          payload.soporteNombre = supportInfo.name;
          payload.soporteTipo = supportInfo.mimeType;
          payload.soporteStoragePath = supportInfo.path;
        }
        await deps.updateIncapacidad?.(editingId, payload);
        setMessage(createMsg, 'Incapacidad actualizada correctamente.', 'ok');
      } else {
        payload.employeeId = target.employeeId;
        payload.documento = target.documento;
        payload.nombre = target.nombre;
        payload.canalRegistro = currentRow?.canalRegistro || 'portal_web';
        payload.soporteUrl = supportInfo.url;
        payload.soporteNombre = supportInfo.name;
        payload.soporteTipo = supportInfo.mimeType;
        payload.soporteStoragePath = supportInfo.path;
        await deps.createIncapacidad?.(payload);
        setMessage(createMsg, 'Incapacidad registrada correctamente.', 'ok');
      }
      resetForm();
      setTab('list');
      if (hasQueried) await runQuery();
    } catch (error) {
      setMessage(createMsg, `Error: ${error?.message || error}`, 'error');
    } finally {
      saveBtn.disabled = false;
      resetBtn.disabled = false;
    }
  }

  function resetForm() {
    editingId = null;
    if (employeeInput) employeeInput.value = '';
    if (sourceSelect) sourceSelect.value = SOURCE_OPTIONS[0];
    if (startInput) startInput.value = '';
    if (endInput) endInput.value = '';
    if (fileInput) fileInput.value = '';
    if (saveBtn) saveBtn.textContent = 'Registrar incapacidad';
    if (resetBtn) resetBtn.textContent = 'Limpiar';
    refreshIdentityHint();
  }

  async function openIncapacityModal(row = null) {
    editingId = row?.id || null;
    const isEdit = Boolean(row?.id);
    const employeeOptions = employees
      .filter((item) => String(item?.estado || 'activo').trim().toLowerCase() === 'activo')
      .map((item) => employeeLabel(item))
      .filter((value, index, all) => value && all.indexOf(value) === index);
    const fields = [];
    if (canManageAll) {
      fields.push({
        id: 'employee',
        label: 'Empleado',
        type: 'datalist',
        required: true,
        placeholder: 'Nombre o documento del empleado',
        options: employeeOptions,
        value: row ? employeeLabel({ documento: row.documento, nombre: row.nombre }) : ''
      });
    }
    fields.push(
      {
        id: 'source',
        label: 'Tipo',
        type: 'select',
        required: true,
        value: SOURCE_OPTIONS.includes(row?.source) ? row.source : SOURCE_OPTIONS[0],
        options: SOURCE_OPTIONS.map((value) => ({ value, label: value }))
      },
      { id: 'fechaInicio', label: 'Fecha inicio', type: 'date', required: true, value: normalizeInputDate(row?.fechaInicio) },
      { id: 'fechaFin', label: 'Fecha terminacion', type: 'date', required: true, value: normalizeInputDate(row?.fechaFin) },
      {
        id: 'support',
        label: isEdit && row?.soporteUrl ? 'Soporte nuevo (opcional)' : 'Soporte',
        type: 'file',
        required: !isEdit && !row?.soporteUrl
      }
    );

    const modal = await showActionModal({
      title: isEdit ? 'Editar incapacidad' : (portalMode ? 'Nueva incapacidad' : 'Crear incapacidad'),
      message: isEdit
        ? `Incapacidad: ${row?.nombre || row?.documento || '-'}`
        : 'Completa la informacion para registrar una incapacidad.',
      confirmText: isEdit ? 'Guardar cambios' : (portalMode ? 'Registrar incapacidad' : 'Crear incapacidad'),
      fields
    });
    if (!modal?.confirmed) return;
    await saveIncapacityFromModal(row, modal.values);
  }

  async function saveIncapacityFromModal(row = null, values = {}) {
    const isEdit = Boolean(row?.id);
    const target = canManageAll
      ? (resolveEmployeeInput(values.employee || '') || (row ? normalizeEmployeeInfo(row) : null))
      : currentFixedEmployee();
    const fechaInicio = String(values.fechaInicio || '').trim();
    const fechaFin = String(values.fechaFin || '').trim();
    const source = String(values.source || SOURCE_OPTIONS[0]).trim() || SOURCE_OPTIONS[0];
    const supportFile = values.support || null;

    if (!target?.documento) return showInfoModal('Dato faltante', ['Selecciona un empleado valido.']);
    if (!fechaInicio || !fechaFin) return showInfoModal('Dato faltante', ['Selecciona fecha de inicio y fecha de terminacion.']);
    if (fechaFin < fechaInicio) return showInfoModal('Fechas invalidas', ['La fecha de terminacion no puede ser menor a la fecha de inicio.']);
    if (!isEdit && !supportFile) return showInfoModal('Dato faltante', ['Adjunta el soporte de la incapacidad.']);

    try {
      if (portalMode) {
        if (!isEdit) {
          if (!supportFile) throw new Error('Adjunta el soporte de la incapacidad.');
          const supportDataUrl = await fileToDataUrl(supportFile);
          const result = await deps.apiRequest('/api/employee-incapacities', {
            method: 'POST',
            body: JSON.stringify({
              fechaInicio,
              fechaFin,
              source,
              soporte: {
                name: supportFile.name,
                dataUrl: supportDataUrl
              }
            })
          });
          if (result?.row) incapRows = [result.row, ...incapRows];
        }
      } else {
        let supportInfo = {
          url: row?.soporteUrl || null,
          name: row?.soporteNombre || null,
          mimeType: row?.soporteTipo || null,
          path: row?.soporteStoragePath || null
        };
        if (supportFile && typeof deps.uploadIncapacidadSupport === 'function') {
          supportInfo = await deps.uploadIncapacidadSupport(supportFile, {
            documento: target.documento,
            employeeId: target.employeeId
          });
        }
        const payload = {
          employeeId: target.employeeId,
          documento: target.documento,
          nombre: target.nombre,
          fechaInicio,
          fechaFin,
          source
        };
        if (supportFile || !isEdit) {
          payload.soporteUrl = supportInfo.url;
          payload.soporteNombre = supportInfo.name;
          payload.soporteTipo = supportInfo.mimeType;
          payload.soporteStoragePath = supportInfo.path;
        }
        if (isEdit) {
          await deps.updateIncapacidad?.(row.id, payload);
        } else {
          payload.canalRegistro = 'manual';
          await deps.createIncapacidad?.(payload);
        }
      }
      editingId = null;
      if (hasQueried) await runQuery();
      else renderList();
      showInfoModal(isEdit ? 'Incapacidad actualizada' : 'Incapacidad creada', [
        isEdit ? 'Los cambios se guardaron correctamente.' : 'La incapacidad se registro correctamente.'
      ]);
    } catch (error) {
      showInfoModal('No fue posible guardar', [String(error?.message || error || 'Error desconocido.')]);
    }
  }

  function visibleRows() {
    if (!hasQueried) return [];
    const term = String(searchInput?.value || '').trim().toLowerCase();
    const estado = String(statusFilter?.value || '').trim();
    const canal = String(channelFilter?.value || '').trim();
    const support = String(supportFilter?.value || '').trim();
    return sortRows(incapRows
      .filter((row) => canManageAll || portalMode || sanitizeDocument(row?.documento) === ownDocument)
      .filter((row) => !estado || String(row?.estado || '').trim().toLowerCase() === estado)
      .filter((row) => !canal || String(row?.canalRegistro || '').trim().toLowerCase() === canal)
      .filter((row) => {
        if (!support) return true;
        const hasSupport = Boolean(String(row?.soporteUrl || row?.soporteNombre || '').trim());
        return support === 'with' ? hasSupport : !hasSupport;
      })
      .filter((row) => {
        if (!term) return true;
        const blob = [
          row?.documento,
          row?.nombre,
          row?.source,
          row?.canalRegistro,
          row?.soporteNombre
        ].join(' ').toLowerCase();
        return blob.includes(term);
      }));
  }

  function renderList() {
    if (!hasQueried) {
      paginator.slice([]);
      listMeta.textContent = 'Selecciona el rango y pulsa Buscar para consultar incapacidades.';
      tbody.replaceChildren(el('tr', {}, [
        el('td', { colSpan: 9, className: 'text-muted' }, ['Aun no se ha ejecutado ninguna consulta.'])
      ]));
      return;
    }
    const rows = visibleRows();
    listMeta.textContent = queryError || `${rows.length} incapacidad(es) visibles.`;
    if (!rows.length) {
      paginator.slice(rows);
      tbody.replaceChildren(el('tr', {}, [
        el('td', { colSpan: 9, className: 'text-muted' }, ['No hay incapacidades para mostrar.'])
      ]));
      return;
    }
    const pageRows = paginator.slice(rows);
    tbody.replaceChildren(...pageRows.map((row) => renderRow(row)));
    updateSortIndicators();
  }

  function initSorting() {
    ui.querySelectorAll('#incTable th[data-sort]').forEach((th) => {
      th.addEventListener('click', () => {
        const key = String(th.getAttribute('data-sort') || '').trim();
        if (!key) return;
        if (sortKey === key) sortDir *= -1;
        else {
          sortKey = key;
          sortDir = key === 'fechaInicio' || key === 'fechaFin' ? -1 : 1;
        }
        paginator.reset();
        renderList();
      });
    });
    updateSortIndicators();
  }

  function sortRows(rows = []) {
    const out = [...rows];
    out.sort((left, right) => {
      const a = sortValue(left, sortKey);
      const b = sortValue(right, sortKey);
      if (a === b) return 0;
      return a > b ? sortDir : -sortDir;
    });
    return out;
  }

  function sortValue(row = {}, key = '') {
    if (key === 'fechaInicio' || key === 'fechaFin') return sortableDate(row?.[key]);
    if (key === 'canalRegistro') return channelLabel(row).toLowerCase();
    if (key === 'soporte') return String(row?.soporteNombre || row?.soporteUrl || '').toLowerCase();
    return String(row?.[key] || '').toLowerCase();
  }

  function sortableDate(value) {
    const input = normalizeInputDate(value);
    return input || '';
  }

  function updateSortIndicators() {
    ui.querySelectorAll('#incTable th[data-sort]').forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[▲▼]$/, '');
      th.dataset.baseLabel = base;
      const key = String(th.getAttribute('data-sort') || '').trim();
      th.textContent = key === sortKey ? `${base} ${sortDir === 1 ? '▲' : '▼'}` : base;
    });
  }

  function renderRow(row) {
    return el('tr', {}, [
      el('td', {}, [row?.documento || '-']),
      el('td', {}, [row?.nombre || '-']),
      el('td', {}, [row?.source || '-']),
      el('td', {}, [formatDate(row?.fechaInicio)]),
      el('td', {}, [formatDate(row?.fechaFin)]),
      el('td', {}, [channelLabel(row)]),
      el('td', {}, [row?.soporteUrl ? supportLink(row) : el('span', { className: 'text-muted' }, ['Sin soporte'])]),
      el('td', {}, [statusBadge(row?.estado)]),
      el('td', {}, [actionsCell(row)])
    ]);
  }

  function actionsCell(row) {
    const btnMore = el('button', { className: 'btn btn--icon', type: 'button', title: 'Mas opciones', 'aria-label': 'Mas opciones' }, ['\u22EF']);
    btnMore.addEventListener('click', () => openMoreOptionsModal(row));
    const actionButtons = [btnMore];
    if (row?.soporteUrl && !portalMode) {
      const btnPdf = el('button', { className: 'btn btn--icon', type: 'button', title: 'Descargar soporte PDF', 'aria-label': 'Descargar soporte PDF' }, ['PDF']);
      btnPdf.addEventListener('click', () => downloadSupportPdf(row));
      actionButtons.push(btnPdf);
    }
    const btnInfo = el('button', { className: 'btn btn--icon', type: 'button', title: 'Ver informacion del empleado', 'aria-label': 'Ver informacion del empleado' }, ['\u24D8']);
    btnInfo.addEventListener('click', () => showEmployeeInfo(row));
    actionButtons.push(btnInfo);
    return el('div', { className: 'row-actions' }, actionButtons);
  }

  function showEmployeeInfo(row) {
    const employee = resolveEmployeeForRow(row);
    const sedeCode = String(employee?.sedeCodigo || '').trim();
    const sede = sedes.find((item) => String(item?.codigo || '').trim() === sedeCode) || null;
    const sedeName = employee?.sedeNombre || sede?.nombre || sedeCode || '-';
    const zonaName = employee?.zonaNombre || sede?.zonaNombre || employee?.zonaCodigo || sede?.zonaCodigo || '-';
    const dependenciaName = sede?.dependenciaNombre || sede?.dependenciaCodigo || '-';
    showInfoModal('Informacion del empleado', [
      `Documento: ${row?.documento || employee?.documento || '-'}`,
      `Nombre: ${row?.nombre || employee?.nombre || '-'}`,
      `Telefono: ${employee?.telefono || '-'}`,
      `Cargo: ${employee?.cargoNombre || employee?.cargoCodigo || '-'}`,
      `Sede: ${sedeName}`,
      `Zona: ${zonaName}`,
      `Dependencia: ${dependenciaName}`
    ]);
  }

  function resolveEmployeeForRow(row = {}) {
    const employeeId = String(row?.employeeId || '').trim();
    const document = sanitizeDocument(row?.documento);
    return employees.find((item) => employeeId && String(item?.id || '').trim() === employeeId)
      || employees.find((item) => document && sanitizeDocument(item?.documento) === document)
      || null;
  }

  async function openMoreOptionsModal(row) {
    const isActive = String(row?.estado || '').trim().toLowerCase() === 'activo';
    const options = [
      { value: '', label: 'Seleccione...' },
      { value: 'upload', label: row?.soporteUrl ? 'Reemplazar soporte' : 'Cargar soporte' }
    ];
    if (!portalMode) {
      options.push({ value: 'edit', label: 'Editar' });
      options.push({ value: 'toggle', label: isActive ? 'Anular' : 'Reactivar' });
    }

    const modal = await showActionModal({
      title: 'Mas opciones',
      message: `Incapacidad: ${row?.nombre || row?.documento || '-'}`,
      confirmText: 'Continuar',
      fields: [{
        id: 'action',
        label: 'Accion',
        type: 'select',
        required: true,
        options
      }]
    });
    if (!modal?.confirmed) return;
    if (modal.values.action === 'upload') return uploadSupport(row);
    if (modal.values.action === 'edit') return startEdit(row);
    if (modal.values.action === 'toggle') return toggleStatus(row);
  }

  function startEdit(row) {
    openIncapacityModal(row);
  }

  async function toggleStatus(row) {
    const nextStatus = String(row?.estado || '').trim().toLowerCase() === 'activo' ? 'inactivo' : 'activo';
    const modal = await showActionModal({
      title: nextStatus === 'activo' ? 'Reactivar incapacidad' : 'Anular incapacidad',
      message: `Vas a dejar la incapacidad como ${nextStatus === 'activo' ? 'vigente' : 'anulada'}.`,
      confirmText: nextStatus === 'activo' ? 'Reactivar' : 'Anular'
    });
    if (!modal?.confirmed) return;
    try {
      await deps.setIncapacidadStatus?.(row.id, nextStatus);
      if (hasQueried) await runQuery();
    } catch (error) {
      showInfoModal('No fue posible actualizar', [String(error?.message || error || 'Error desconocido.')]);
    }
  }

  async function uploadSupport(row) {
    try {
      const file = await pickSupportFile();
      if (!file) return;

      if (portalMode) {
        const supportDataUrl = await fileToDataUrl(file);
        await deps.apiRequest(`/api/employee-incapacities/${encodeURIComponent(row.id)}/support`, {
          method: 'POST',
          body: JSON.stringify({
            soporte: {
              name: file.name,
              dataUrl: supportDataUrl
            }
          })
        });
      } else {
        const supportInfo = await deps.uploadIncapacidadSupport(file, {
          documento: row?.documento,
          employeeId: row?.employeeId
        });
        await deps.updateIncapacidad?.(row.id, {
          soporteUrl: supportInfo.url,
          soporteNombre: supportInfo.name,
          soporteTipo: supportInfo.mimeType,
          soporteStoragePath: supportInfo.path
        });
      }

      if (hasQueried) await runQuery();
      showInfoModal('Soporte actualizado', ['El soporte se cargo correctamente para la incapacidad seleccionada.']);
    } catch (error) {
      showInfoModal('No fue posible cargar el soporte', [String(error?.message || error || 'Error desconocido.')]);
    }
  }

  async function downloadSupportPdf(row) {
    try {
      const pdfBlob = await supportBlobAsPdf(row);
      const name = supportPdfName(row);
      downloadBlob(pdfBlob, name);
    } catch (error) {
      showInfoModal('No fue posible descargar el soporte', [String(error?.message || error || 'Error desconocido.')]);
    }
  }
};

function buildCreateFields(portalMode, canManageAll) {
  const fields = [];
  if (canManageAll) {
    fields.push(
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'incEmployeeSearch' }, ['Empleado']),
        el('input', {
          id: 'incEmployeeSearch',
          className: 'input',
          list: 'incEmployeeList',
          placeholder: 'Nombre o documento del empleado'
        }),
        el('datalist', { id: 'incEmployeeList' }, [])
      ])
    );
  }
  fields.push(
    el('div', {}, [
      el('label', { className: 'label', htmlFor: 'incSource' }, ['Tipo']),
      el('select', { id: 'incSource', className: 'select' }, SOURCE_OPTIONS.map((value, index) =>
        el('option', { value, selected: index === 0 }, [value])
      ))
    ]),
    el('div', {}, [
      el('label', { className: 'label', htmlFor: 'incStart' }, ['Fecha inicio']),
      el('input', { id: 'incStart', className: 'input', type: 'date' })
    ]),
    el('div', {}, [
      el('label', { className: 'label', htmlFor: 'incEnd' }, ['Fecha terminacion']),
      el('input', { id: 'incEnd', className: 'input', type: 'date' })
    ]),
    el('div', {}, [
      el('label', { className: 'label', htmlFor: 'incSupport' }, ['Soporte']),
      el('input', { id: 'incSupport', className: 'input', type: 'file', accept: 'application/pdf,image/png,image/jpeg,image/webp' })
    ]),
    el('button', { id: 'incSaveBtn', className: 'btn btn--primary', type: 'button' }, [portalMode ? 'Registrar incapacidad' : 'Registrar incapacidad']),
    el('button', { id: 'incResetBtn', className: 'btn', type: 'button' }, ['Limpiar'])
  );
  return fields;
}

function sanitizeDocument(value) {
  return String(value || '').replace(/\D+/g, '').trim();
}

function employeeLabel(row = {}) {
  const doc = sanitizeDocument(row?.documento);
  const name = String(row?.nombre || '').trim() || 'Empleado';
  return doc ? `${name} (${doc})` : name;
}

function setMessage(node, text, state = '') {
  if (!node) return;
  node.textContent = text || ' ';
  if (state) node.dataset.state = state;
  else delete node.dataset.state;
}

function formatDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  return raw;
}

function normalizeInputDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  try {
    const date = value?.toDate ? value.toDate() : new Date(value);
    if (!date || Number.isNaN(date.getTime())) return '';
    const pad = (number) => String(number).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  } catch (_) {
    return '';
  }
}

function channelLabel(row = {}) {
  const key = String(row?.canalRegistro || '').trim().toLowerCase();
  if (CHANNEL_LABELS[key]) return CHANNEL_LABELS[key];
  if (String(row?.whatsappMessageId || '').trim()) return 'WhatsApp';
  return '-';
}

function statusBadge(state) {
  const active = String(state || 'activo').trim().toLowerCase() === 'activo';
  return el('span', { className: `badge ${active ? 'badge--ok' : 'badge--off'}` }, [active ? 'Vigente' : 'Anulada']);
}

function supportLink(row = {}) {
  const label = String(row?.soporteNombre || '').trim() || 'Soporte';
  const anchor = el('a', {
    href: row?.soporteUrl || '#',
    target: '_blank',
    rel: 'noopener noreferrer',
    className: 'link'
  }, [label]);
  return anchor;
}

function pickSupportFile() {
  return new Promise((resolve) => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'application/pdf,image/png,image/jpeg,image/webp';
    input.addEventListener('change', () => resolve(input.files?.[0] || null), { once: true });
    input.click();
  });
}

async function supportBlobAsPdf(row = {}) {
  const blob = await fetchSupportBlob(row);
  if (String(blob?.type || '').trim().toLowerCase() === 'application/pdf') return blob;
  return imageBlobToPdfBlob(blob);
}

async function fetchSupportBlob(row = {}) {
  const url = String(row?.soporteUrl || '').trim();
  if (!url) throw new Error('La incapacidad no tiene soporte para descargar.');
  const response = await fetch(url);
  if (!response.ok) throw new Error('No fue posible descargar el soporte.');
  return response.blob();
}

function supportPdfName(row = {}) {
  const base = String(row?.soporteNombre || row?.documento || 'soporte')
    .replace(/\.[a-zA-Z0-9]+$/i, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .trim();
  return `${base || 'soporte'}.pdf`;
}

function downloadBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name || 'archivo.pdf';
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function imageBlobToPdfBlob(blob) {
  const image = await loadImageFromBlob(blob);
  const width = Math.max(1, Number(image.naturalWidth || image.width || 1));
  const height = Math.max(1, Number(image.naturalHeight || image.height || 1));
  const canvas = document.createElement('canvas');
  canvas.width = width;
  canvas.height = height;
  const context = canvas.getContext('2d');
  if (!context) throw new Error('No fue posible preparar el soporte para PDF.');
  context.fillStyle = '#ffffff';
  context.fillRect(0, 0, width, height);
  context.drawImage(image, 0, 0, width, height);
  const jpegDataUrl = canvas.toDataURL('image/jpeg', 0.92);
  const jpegBase64 = String(jpegDataUrl.split(',')[1] || '').trim();
  if (!jpegBase64) throw new Error('No fue posible convertir la imagen a PDF.');
  const jpegBytes = base64ToUint8Array(jpegBase64);
  const pdfBytes = buildPdfFromJpeg(jpegBytes, width, height);
  return new Blob([pdfBytes], { type: 'application/pdf' });
}

function loadImageFromBlob(blob) {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(blob);
    const image = new Image();
    image.onload = () => {
      URL.revokeObjectURL(url);
      resolve(image);
    };
    image.onerror = () => {
      URL.revokeObjectURL(url);
      reject(new Error('No fue posible leer el soporte como imagen.'));
    };
    image.src = url;
  });
}

function base64ToUint8Array(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function buildPdfFromJpeg(jpegBytes, width, height) {
  const encoder = new TextEncoder();
  const content = `q\n${width} 0 0 ${height} 0 0 cm\n/Im0 Do\nQ\n`;
  const objects = [
    encodePdfChunk(encoder, '1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n'),
    encodePdfChunk(encoder, '2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n'),
    encodePdfChunk(encoder, `3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${width} ${height}] /Resources << /XObject << /Im0 4 0 R >> >> /Contents 5 0 R >>\nendobj\n`),
    concatUint8Arrays([
      encodePdfChunk(encoder, `4 0 obj\n<< /Type /XObject /Subtype /Image /Width ${width} /Height ${height} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length ${jpegBytes.length} >>\nstream\n`),
      jpegBytes,
      encodePdfChunk(encoder, '\nendstream\nendobj\n')
    ]),
    encodePdfChunk(encoder, `5 0 obj\n<< /Length ${encoder.encode(content).length} >>\nstream\n${content}endstream\nendobj\n`)
  ];

  const header = encodePdfChunk(encoder, '%PDF-1.4\n%\xC2\xA5\xC2\xB1\xC3\xAB\n');
  let offset = header.length;
  const offsets = [0];
  for (const object of objects) {
    offsets.push(offset);
    offset += object.length;
  }

  const xrefStart = offset;
  const xrefLines = ['xref', `0 ${objects.length + 1}`, '0000000000 65535 f '];
  for (let index = 1; index < offsets.length; index += 1) {
    xrefLines.push(`${String(offsets[index]).padStart(10, '0')} 00000 n `);
  }
  const xref = encodePdfChunk(encoder, `${xrefLines.join('\n')}\n`);
  const trailer = encodePdfChunk(encoder, `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefStart}\n%%EOF`);

  return concatUint8Arrays([header, ...objects, xref, trailer]);
}

function encodePdfChunk(encoder, value) {
  return encoder.encode(String(value || ''));
}

function concatUint8Arrays(chunks = []) {
  const total = chunks.reduce((sum, chunk) => sum + (chunk?.length || 0), 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  return merged;
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = () => reject(new Error('No fue posible leer el soporte seleccionado.'));
    reader.readAsDataURL(file);
  });
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}
