import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { createTablePagination } from '../utils/pagination.js';
import { showActionModal } from '../utils/actionModal.js';
import { can, PERMS } from '../permissions.js';

const ACTIONS = {
  create_employee: { type: 'create', label: 'Creacion' },
  transfer_employee: { type: 'transfer', label: 'Traslado' },
  change_employee_cargo: { type: 'cargo', label: 'Cambio de cargo' },
  retire_employee: { type: 'retire', label: 'Retiro' },
  update_programmed_employee_assignment: { type: 'schedule', label: 'Programacion actualizada' },
  cancel_programmed_employee_assignment: { type: 'schedule', label: 'Programacion cancelada' }
};

export const EmployeeNovelties = (mount, deps = {}) => {
  const today = todayBogota();
  const canManageSchedules = can(PERMS.MANAGE_EMPLOYEE_SCHEDULES);
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Novedades de empleados']),
    el('p', { className: 'text-muted' }, ['Consulta altas, traslados, cambios de cargo y retiros registrados desde el modulo de empleados.']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'novFrom' }, ['Fecha inicio']),
        el('input', { id: 'novFrom', className: 'input', type: 'date', value: today })
      ]),
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'novTo' }, ['Fecha fin']),
        el('input', { id: 'novTo', className: 'input', type: 'date', value: today })
      ]),
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'novType' }, ['Tipo']),
        el('select', { id: 'novType', className: 'select' }, [
          el('option', { value: '' }, ['Todos']),
          el('option', { value: 'create' }, ['Creacion']),
          el('option', { value: 'transfer' }, ['Traslado']),
          el('option', { value: 'cargo' }, ['Cambio de cargo']),
          el('option', { value: 'schedule' }, ['Programacion']),
          el('option', { value: 'retire' }, ['Retiro'])
        ])
      ]),
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'novSearch' }, ['Buscar']),
        el('input', { id: 'novSearch', className: 'input', placeholder: 'Documento, nombre, sede, cargo o usuario...' })
      ]),
      el('span', { id: 'novMeta', className: 'right text-muted' }, ['Cargando novedades...'])
    ]),
    el('div', { className: 'mt-2 table-wrap' }, [
      el('table', { className: 'table', id: 'novTable' }, [
        el('thead', {}, [el('tr', {}, [
          el('th', { 'data-sort': 'date', style: 'cursor:pointer' }, ['Fecha']),
          el('th', { 'data-sort': 'typeLabel', style: 'cursor:pointer' }, ['Tipo']),
          el('th', { 'data-sort': 'documento', style: 'cursor:pointer' }, ['Documento']),
          el('th', { 'data-sort': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
          el('th', { 'data-sort': 'fromLabel', style: 'cursor:pointer' }, ['Anterior']),
          el('th', { 'data-sort': 'toLabel', style: 'cursor:pointer' }, ['Nuevo']),
          el('th', { 'data-sort': 'actorEmail', style: 'cursor:pointer' }, ['Usuario']),
          el('th', {}, ['Acciones'])
        ])]),
        el('tbody', {})
      ])
    ])
  ]);

  const dateFromInput = qs('#novFrom', ui);
  const dateToInput = qs('#novTo', ui);
  const typeInput = qs('#novType', ui);
  const searchInput = qs('#novSearch', ui);
  const meta = qs('#novMeta', ui);
  const tbody = qs('#novTable tbody', ui);
  const paginator = createTablePagination(ui, { id: 'employeeNovelties', after: '.table-wrap', onChange: render });

  let auditRows = [];
  let historyRows = [];
  let employees = [];
  let sedes = [];
  let cargos = [];
  let sortKey = 'date';
  let sortDir = -1;

  [dateFromInput, dateToInput, typeInput, searchInput].forEach((node) => {
    node?.addEventListener('input', () => { paginator.reset(); render(); });
    node?.addEventListener('change', () => { paginator.reset(); render(); });
  });
  ui.querySelectorAll('th[data-sort]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else {
        sortKey = key;
        sortDir = key === 'date' ? -1 : 1;
      }
      paginator.reset();
      render();
    });
  });

  mount.replaceChildren(ui);

  const unAudit = deps.streamAuditLogs?.((rows) => {
    auditRows = rows || [];
    render();
  }, 1000) || (() => {});
  const unHistory = deps.streamEmployeeCargoHistoryAll?.((rows) => {
    historyRows = rows || [];
    render();
  }) || (() => {});
  const unEmployees = deps.streamEmployees?.((rows) => {
    employees = rows || [];
    render();
  }) || (() => {});
  const unSedes = deps.streamSedes?.((rows) => {
    sedes = rows || [];
    render();
  }) || (() => {});
  const unCargos = deps.streamCargos?.((rows) => {
    cargos = rows || [];
    render();
  }) || (() => {});

  render();

  return () => {
    unAudit?.();
    unHistory?.();
    unEmployees?.();
    unSedes?.();
    unCargos?.();
  };

  function render() {
    const rows = filteredRows();
    const pageRows = paginator.slice(rows);
    tbody.replaceChildren(...pageRows.map((row) => renderRow(row)));
    meta.textContent = `${rows.length} novedad(es) visibles.`;
    updateSortIndicators();
  }

  function filteredRows() {
    const from = String(dateFromInput?.value || '').trim();
    const to = String(dateToInput?.value || '').trim();
    const type = String(typeInput?.value || '').trim();
    const term = String(searchInput?.value || '').trim().toLowerCase();

    const history = historyNoveltyRows();
    const audit = auditRows
      .map(normalizeAuditRow)
      .filter(Boolean);
    const rows = [
      ...history,
      ...audit.filter((row) => !hasMatchingHistoryNovelty(row, history))
    ]
      .filter((row) => isDateVisible(row, from, to))
      .filter((row) => isTypeVisible(row, type))
      .filter((row) => {
        if (!term) return true;
        return [
          row.typeLabel,
          row.documento,
          row.nombre,
          row.fromLabel,
          row.toLabel,
          row.actorEmail,
          row.note
        ].join(' ').toLowerCase().includes(term);
      });

    return sortRows(rows);
  }

  function isDateVisible(row, from, to) {
    if (isPendingFutureProgram(row)) return true;
    return (!from || row.date >= from) && (!to || row.date <= to);
  }

  function isTypeVisible(row, type) {
    if (!type) return true;
    if (type === 'schedule') return row.type === 'schedule' || isPendingFutureProgram(row);
    return row.type === type;
  }

  function isPendingFutureProgram(row = {}) {
    return row.source === 'history' && row.isProgrammed === true;
  }

  function normalizeAuditRow(row) {
    if (String(row?.targetType || '').trim() !== 'employee') return null;
    const action = ACTIONS[String(row?.action || '').trim()];
    if (!action) return null;
    const employee = employeeByRow(row);
    const after = row?.after || {};
    const before = row?.before || {};
    const date = effectiveAuditDate(row, action.type);
    const normalized = {
      id: row.id,
      targetId: row?.targetId || null,
      date,
      datetime: formatDateTime(row?.ts),
      source: 'audit',
      type: action.type,
      typeLabel: action.label,
      documento: after.documento || before.documento || employee?.documento || '-',
      nombre: after.nombre || before.nombre || employee?.nombre || '-',
      actorEmail: row?.actorEmail || '-',
      note: row?.note || '',
      before,
      after,
      employee
    };
    const movement = movementLabels(normalized);
    return { ...normalized, ...movement };
  }

  function historyNoveltyRows() {
    const byEmployee = new Map();
    (historyRows || []).forEach((row) => {
      const key = String(row?.employeeId || row?.documento || '').trim();
      if (!key) return;
      if (!byEmployee.has(key)) byEmployee.set(key, []);
      byEmployee.get(key).push(row);
    });

    const rows = [];
    byEmployee.forEach((items) => {
      const ordered = [...items].sort((left, right) => {
        const a = String(left?.fechaIngreso || left?.createdAt || '').trim();
        const b = String(right?.fechaIngreso || right?.createdAt || '').trim();
        if (a === b) return String(left?.createdAt || '').localeCompare(String(right?.createdAt || ''));
        return a.localeCompare(b);
      });
      ordered.forEach((item, index) => {
        const employee = employeeByHistoryRow(item);
        const base = {
          id: `history:${item.id}:start`,
          date: toInputDate(item.fechaIngreso || item.createdAt),
          datetime: formatDateTime(item.fechaIngreso || item.createdAt),
          source: 'history',
          documento: item.documento || employee?.documento || '-',
          nombre: employee?.nombre || item.employeeCodigo || item.documento || '-',
          actorEmail: '-',
          note: sourceLabel(item.source),
          before: index > 0 ? historyAssignmentData(ordered[index - 1]) : {},
          after: historyAssignmentData(item),
          employee,
          historyItem: item,
          previousHistoryItem: index > 0 ? ordered[index - 1] : null,
          isProgrammed: isProgrammedHistoryRow(item)
        };
        if (index === 0) {
          rows.push({
            ...base,
            type: 'create',
            typeLabel: 'Creacion',
            fromLabel: '-',
            toLabel: assignmentLabel(item)
          });
          return;
        }
        const previous = ordered[index - 1];
        const sedeChanged = String(previous?.sedeCodigo || '').trim() !== String(item?.sedeCodigo || '').trim();
        const cargoChanged = String(previous?.cargoCodigo || '').trim() !== String(item?.cargoCodigo || '').trim();
        if (!sedeChanged && !cargoChanged) return;
        rows.push({
          ...base,
          id: `history:${item.id}:move`,
          type: sedeChanged ? 'transfer' : 'cargo',
          typeLabel: sedeChanged ? 'Traslado' : 'Cambio de cargo',
          fromLabel: assignmentLabel(previous),
          toLabel: assignmentLabel(item)
        });
      });
      const last = ordered[ordered.length - 1] || null;
      if (last?.fechaRetiro) {
        const employee = employeeByHistoryRow(last);
        rows.push({
          id: `history:${last.id}:retire`,
          date: toInputDate(last.fechaRetiro),
          datetime: formatDateTime(last.fechaRetiro),
          source: 'history',
          type: 'retire',
          typeLabel: 'Retiro',
          documento: last.documento || employee?.documento || '-',
          nombre: employee?.nombre || last.employeeCodigo || last.documento || '-',
          fromLabel: assignmentLabel(last),
          toLabel: `Retiro ${formatDate(last.fechaRetiro)}`.trim(),
          actorEmail: '-',
          note: sourceLabel(last.source),
          before: historyAssignmentData(last),
          after: { estado: 'inactivo', fechaRetiro: toInputDate(last.fechaRetiro) },
          employee
        });
      }
    });
    return rows;
  }

  function movementLabels(row) {
    if (row.type === 'create') {
      return {
        fromLabel: '-',
        toLabel: employeeContextLabel(row.employee, row.after)
      };
    }
    if (row.type === 'transfer') {
      return {
        fromLabel: sedeLabel(row.before?.sedeCodigo, row.before?.sedeNombre),
        toLabel: sedeLabel(row.after?.sedeCodigo, row.after?.sedeNombre)
      };
    }
    if (row.type === 'cargo') {
      return {
        fromLabel: row.before?.cargoNombre || row.before?.cargoCodigo || '-',
        toLabel: row.after?.cargoNombre || row.after?.cargoCodigo || '-'
      };
    }
    if (row.type === 'retire') {
      return {
        fromLabel: 'Activo',
        toLabel: `Retiro ${formatDate(row.after?.fechaRetiro) || ''}`.trim()
      };
    }
    if (row.type === 'schedule') {
      return {
        fromLabel: assignmentLabel(row.before),
        toLabel: assignmentLabel(row.after)
      };
    }
    return { fromLabel: '-', toLabel: '-' };
  }

  function renderRow(row) {
    return el('tr', {}, [
      el('td', {}, [row.date || '-']),
      el('td', {}, [typeBadge(row.type, row.typeLabel)]),
      el('td', {}, [row.documento || '-']),
      el('td', {}, [row.nombre || '-']),
      el('td', {}, [row.fromLabel || '-']),
      el('td', {}, [row.toLabel || '-']),
      el('td', {}, [row.actorEmail || '-']),
      el('td', {}, [actionsCell(row)])
    ]);
  }

  function actionsCell(row) {
    const btnInfo = el('button', { className: 'btn btn--icon', type: 'button', title: 'Ver informacion', 'aria-label': 'Ver informacion' }, ['\u24D8']);
    btnInfo.addEventListener('click', () => showInfo(row));
    const actions = [];
    if (canManageSchedules && row.source === 'history' && row.isProgrammed && row.previousHistoryItem) {
      const btnEdit = el('button', { className: 'btn btn--icon', type: 'button', title: 'Editar programacion', 'aria-label': 'Editar programacion' }, ['\u270E']);
      btnEdit.addEventListener('click', () => editProgrammedAssignment(row));
      const btnCancel = el('button', { className: 'btn btn--icon btn--danger', type: 'button', title: 'Cancelar programacion', 'aria-label': 'Cancelar programacion' }, ['\u2716']);
      btnCancel.addEventListener('click', () => cancelProgrammedAssignment(row));
      actions.push(btnEdit, btnCancel);
    }
    actions.push(btnInfo);
    return el('div', { className: 'row-actions' }, actions);
  }

  async function editProgrammedAssignment(row) {
    const item = row.historyItem || {};
    const modal = await showActionModal({
      title: 'Editar programacion',
      message: `Empleado: ${row.nombre || '-'} - inicio actual ${formatDate(item.fechaIngreso)}`,
      confirmText: 'Guardar programacion',
      fields: [
        { id: 'cargo', label: 'Cargo programado', type: 'select', required: true, value: item.cargoCodigo || '', options: cargoOptions(item.cargoCodigo) },
        { id: 'sede', label: 'Sede programada', type: 'select', required: true, value: item.sedeCodigo || '', options: sedeOptions(item.sedeCodigo) },
        { id: 'fechaIngreso', label: 'Fecha inicio', type: 'date', required: true, min: addDaysToInputDate(today, 1), value: toInputDate(item.fechaIngreso) },
        { id: 'detail', label: 'Detalle', type: 'textarea', required: true, placeholder: 'Describe la correccion de la programacion' }
      ]
    });
    if (!modal.confirmed) return;
    const fechaIngreso = String(modal.values.fechaIngreso || '').trim();
    const cargoCodigo = String(modal.values.cargo || '').trim();
    const sedeCodigo = String(modal.values.sede || '').trim();
    if (!validInputDate(fechaIngreso)) return alert('Fecha invalida. Usa formato AAAA-MM-DD.');
    if (fechaIngreso <= today) return alert('Solo puedes editar programaciones con inicio posterior a hoy.');
    const cargo = cargos.find((entry) => String(entry?.codigo || '').trim() === cargoCodigo) || null;
    const sede = sedes.find((entry) => String(entry?.codigo || '').trim() === sedeCodigo) || null;
    try {
      await deps.updateProgrammedEmployeeAssignment?.(item.id, {
        cargoCodigo,
        cargoNombre: cargo?.nombre || null,
        sedeCodigo,
        sedeNombre: sede?.nombre || null,
        fechaIngreso
      });
      await deps.addAuditLog?.({
        targetType: 'employee',
        targetId: row.employee?.id || item.employeeId || null,
        action: 'update_programmed_employee_assignment',
        before: historyAssignmentData(item),
        after: {
          cargoCodigo,
          cargoNombre: cargo?.nombre || null,
          sedeCodigo,
          sedeNombre: sede?.nombre || null,
          fechaIngreso,
          fechaRetiro: null
        },
        note: modal.values.detail || null
      });
      alert('Programacion actualizada.');
    } catch (error) {
      alert('Error: ' + (error?.message || error));
    }
  }

  async function cancelProgrammedAssignment(row) {
    const item = row.historyItem || {};
    const previous = row.previousHistoryItem || {};
    const modal = await showActionModal({
      title: 'Cancelar programacion',
      message: `Se cancelara el cambio futuro de ${row.nombre || '-'} y se restaurara: ${assignmentLabel(previous)}.`,
      confirmText: 'Cancelar programacion',
      fields: [
        { id: 'currentProgram', label: 'Programacion actual', type: 'text', readonly: true, value: `${assignmentLabel(item)} desde ${formatDate(item.fechaIngreso)}` },
        { id: 'restoreTo', label: 'Se restaura', type: 'text', readonly: true, value: assignmentLabel(previous) },
        { id: 'detail', label: 'Detalle', type: 'textarea', required: true, placeholder: 'Describe el motivo de la cancelacion' }
      ]
    });
    if (!modal.confirmed) return;
    try {
      await deps.cancelProgrammedEmployeeAssignment?.(item.id);
      await deps.addAuditLog?.({
        targetType: 'employee',
        targetId: row.employee?.id || item.employeeId || null,
        action: 'cancel_programmed_employee_assignment',
        before: historyAssignmentData(item),
        after: historyAssignmentData(previous),
        note: modal.values.detail || null
      });
      alert('Programacion cancelada.');
    } catch (error) {
      alert('Error: ' + (error?.message || error));
    }
  }

  function showInfo(row) {
    const employee = row.employee || {};
    const sede = sedeForEmployee(employee, row.after);
    showInfoModal('Detalle de novedad', [
      `Tipo: ${row.typeLabel}`,
      `Fecha: ${row.datetime || row.date || '-'}`,
      `Documento: ${row.documento || '-'}`,
      `Nombre: ${row.nombre || '-'}`,
      `Telefono: ${employee.telefono || '-'}`,
      `Cargo actual: ${employee.cargoNombre || employee.cargoCodigo || '-'}`,
      `Sede actual: ${employee.sedeNombre || sede?.nombre || employee.sedeCodigo || '-'}`,
      `Zona: ${employee.zonaNombre || sede?.zonaNombre || '-'}`,
      `Dependencia: ${sede?.dependenciaNombre || '-'}`,
      `Anterior: ${row.fromLabel || '-'}`,
      `Nuevo: ${row.toLabel || '-'}`,
      `Usuario: ${row.actorEmail || '-'}`,
      `Detalle: ${row.note || '-'}`
    ]);
  }

  function employeeByRow(row) {
    const targetId = String(row?.targetId || '').trim();
    const document = String(row?.after?.documento || row?.before?.documento || '').trim();
    return employees.find((item) => targetId && String(item?.id || '').trim() === targetId)
      || employees.find((item) => document && String(item?.documento || '').trim() === document)
      || null;
  }

  function employeeByHistoryRow(row) {
    const employeeId = String(row?.employeeId || '').trim();
    const document = String(row?.documento || '').trim();
    return employees.find((item) => employeeId && String(item?.id || '').trim() === employeeId)
      || employees.find((item) => document && String(item?.documento || '').trim() === document)
      || null;
  }

  function employeeContextLabel(employee, fallback = {}) {
    const cargo = employee?.cargoNombre || fallback?.cargoNombre || fallback?.cargoCodigo || '-';
    const sede = employee?.sedeNombre || sedeLabel(fallback?.sedeCodigo, fallback?.sedeNombre);
    return `${cargo} / ${sede}`;
  }

  function historyAssignmentData(row = {}) {
    return {
      cargoCodigo: row.cargoCodigo || null,
      cargoNombre: row.cargoNombre || null,
      sedeCodigo: row.sedeCodigo || null,
      sedeNombre: row.sedeNombre || null,
      fechaIngreso: toInputDate(row.fechaIngreso),
      fechaRetiro: toInputDate(row.fechaRetiro)
    };
  }

  function assignmentLabel(row = {}) {
    const cargo = row?.cargoNombre || row?.cargoCodigo || '-';
    const sede = sedeLabel(row?.sedeCodigo, row?.sedeNombre);
    return `${cargo} / ${sede}`;
  }

  function effectiveAuditDate(row, type) {
    const after = row?.after || {};
    const before = row?.before || {};
    if (type === 'transfer' || type === 'cargo') return toInputDate(after.assignmentFechaIngreso || row?.ts);
    if (type === 'schedule') return toInputDate(after.fechaIngreso || before.fechaIngreso || row?.ts);
    if (type === 'retire') return toInputDate(after.fechaRetiro || row?.ts);
    return toInputDate(row?.ts);
  }

  function hasMatchingHistoryNovelty(row, history) {
    const employeeId = String(row?.employee?.id || row?.targetId || '').trim();
    const document = String(row?.documento || '').trim();
    return history.some((item) => {
      if (item.type !== row.type) return false;
      if (item.date !== row.date) return false;
      const itemEmployeeId = String(item?.employee?.id || '').trim();
      const itemDocument = String(item?.documento || '').trim();
      return (employeeId && itemEmployeeId && employeeId === itemEmployeeId) || (document && itemDocument && document === itemDocument);
    });
  }

  function sourceLabel(value) {
    const source = String(value || '').trim();
    if (!source) return 'Historial operativo';
    const map = {
      create_employee: 'Creacion de empleado',
      bulk_create_employee: 'Cargue masivo',
      sede_change: 'Cambio de sede',
      cargo_change: 'Cambio de cargo',
      scheduled_assignment_update: 'Actualizacion programada',
      employee_update: 'Actualizacion de empleado',
      reactivate_employee: 'Reactivacion de empleado'
    };
    return map[source] || source;
  }

  function sedeForEmployee(employee = {}, fallback = {}) {
    const code = String(employee?.sedeCodigo || fallback?.sedeCodigo || '').trim();
    return sedes.find((item) => String(item?.codigo || '').trim() === code) || null;
  }

  function sedeLabel(code, name = '') {
    const rawCode = String(code || '').trim();
    const sede = sedes.find((item) => rawCode && String(item?.codigo || '').trim() === rawCode) || null;
    return String(name || sede?.nombre || rawCode || '-').trim() || '-';
  }

  function isProgrammedHistoryRow(row = {}) {
    const ingreso = toInputDate(row?.fechaIngreso);
    return Boolean(ingreso && ingreso > today && !row?.fechaRetiro);
  }

  function sedeOptions(selectedCode = '') {
    const current = String(selectedCode || '').trim();
    const active = (sedes || []).filter((item) => String(item?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
    const hasCurrent = active.some((item) => String(item?.codigo || '').trim() === current);
    const rows = hasCurrent || !current ? active : [...active, { codigo: current, nombre: sedeLabel(current) }];
    return rows.map((item) => ({
      value: item.codigo || '',
      label: `${item.codigo || ''} - ${item.nombre || ''}`.trim()
    }));
  }

  function cargoOptions(selectedCode = '') {
    const current = String(selectedCode || '').trim();
    const active = (cargos || []).filter((item) => String(item?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
    const hasCurrent = active.some((item) => String(item?.codigo || '').trim() === current);
    const rows = hasCurrent || !current ? active : [...active, { codigo: current, nombre: current }];
    return rows.map((item) => ({
      value: item.codigo || '',
      label: `${item.codigo || ''} - ${item.nombre || ''}`.trim()
    }));
  }

  function sortRows(rows) {
    return [...rows].sort((left, right) => {
      const a = sortValue(left, sortKey);
      const b = sortValue(right, sortKey);
      if (a === b) return 0;
      return a > b ? sortDir : -sortDir;
    });
  }

  function sortValue(row, key) {
    if (key === 'date') return String(row.datetime || row.date || '');
    return String(row?.[key] || '').toLowerCase();
  }

  function updateSortIndicators() {
    ui.querySelectorAll('th[data-sort]').forEach((th) => {
      const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[▲▼]$/, '');
      th.dataset.baseLabel = base;
      const key = String(th.getAttribute('data-sort') || '').trim();
      th.textContent = key === sortKey ? `${base} ${sortDir === 1 ? '▲' : '▼'}` : base;
    });
  }
};

function typeBadge(type, label) {
  const map = {
    create: 'badge--ok',
    transfer: '',
    cargo: '',
    schedule: '',
    retire: 'badge--off'
  };
  return el('span', { className: `badge ${map[type] || ''}` }, [label || '-']);
}

function toInputDate(value) {
  try {
    if (typeof value === 'string') {
      const raw = value.trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    }
    const date = value ? new Date(value) : null;
    if (!date || Number.isNaN(date.getTime())) return '';
    const pad = (n) => String(n).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  } catch (_) {
    return '';
  }
}

function formatDate(value) {
  const input = toInputDate(value);
  return input || '-';
}

function formatDateTime(value) {
  try {
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value.trim())) return value.trim();
    const date = value ? new Date(value) : null;
    return date && !Number.isNaN(date.getTime()) ? date.toLocaleString() : '-';
  } catch (_) {
    return '-';
  }
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

function validInputDate(value) {
  const raw = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return false;
  return !Number.isNaN(new Date(`${raw}T00:00:00`).getTime());
}

function addDaysToInputDate(value, days = 1) {
  const raw = String(value || '').trim();
  if (!validInputDate(raw)) return '';
  const [year, month, day] = raw.split('-').map((part) => Number(part));
  const date = new Date(Date.UTC(year, (month || 1) - 1, day || 1));
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}
