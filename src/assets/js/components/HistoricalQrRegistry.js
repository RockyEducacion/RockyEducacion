import { el, qs, enableSectionToggles } from '../utils/dom.js';
import { createTablePagination } from '../utils/pagination.js';
import { can, PERMS } from '../permissions.js';

export const HistoricalQrRegistry = (mount, deps = {}) => {
  const canExport = can(PERMS.EXPORT_REPORTS_QR_HISTORY);
  const maxDate = yesterdayBogota();
  let selectedDate = maxDate;
  let rows = [];
  let pendingRows = [];
  let running = false;
  let searchTerm = '';
  let pendingZone = 'all';
  let sortKey = 'entryAt';
  let sortDir = -1;
  let pendingSortKey = 'zona';
  let pendingSortDir = 1;

  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Historico Registro QR']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label', for: 'historicalQrDate' }, ['Fecha']),
        el('input', {
          id: 'historicalQrDate',
          className: 'input',
          type: 'date',
          value: selectedDate,
          max: maxDate
        }, [])
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'historicalQrSearch' }, ['Buscar']),
        el('input', {
          id: 'historicalQrSearch',
          className: 'input',
          placeholder: 'Cedula, nombre, sede o celular...'
        }, [])
      ]),
      el('button', { id: 'btnGenerateHistoricalQr', className: 'btn btn--primary', type: 'button' }, ['Consultar fecha']),
      el('button', { id: 'btnExportHistoricalQr', className: 'btn', type: 'button', disabled: true, title: canExport ? '' : 'Modo consulta: no puedes exportar.' }, ['Exportar Excel']),
      el('span', { id: 'historicalQrMsg', className: 'text-muted' }, [' '])
    ]),
    el('section', { className: 'wa-stats wa-stats--nov wa-stats--qr-registry mt-2' }, [
      statCard('Programados QR', 'historicalQrScheduled', '0'),
      statCard('Ingresos QR', 'historicalQrEntries', '0'),
      statCard('Con salida', 'historicalQrWithExit', '0'),
      statCard('Pendientes ingreso', 'historicalQrPending', '0'),
      statCard('Alertas celular', 'historicalQrPhoneAlerts', '0')
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Registros QR del dia']),
      el('div', { className: 'responsive-records' }, [
        el('div', { className: 'table-wrap responsive-table-view' }, [
          el('table', { className: 'table wa-live-table', id: 'tblHistoricalQr' }, [
            el('thead', {}, [
              el('tr', {}, [
                el('th', { 'data-sort-qr': 'documento', style: 'cursor:pointer' }, ['Cedula']),
                el('th', { 'data-sort-qr': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
                el('th', { 'data-sort-qr': 'sede', style: 'cursor:pointer' }, ['Sede']),
                el('th', { 'data-sort-qr': 'entryAt', style: 'cursor:pointer' }, ['Ingreso']),
                el('th', { 'data-sort-qr': 'exitAt', style: 'cursor:pointer' }, ['Salida']),
                el('th', { 'data-sort-qr': 'employeePhone', style: 'cursor:pointer' }, ['Celular empleado']),
                el('th', { 'data-sort-qr': 'entryPhone', style: 'cursor:pointer' }, ['Celular ingreso']),
                el('th', { 'data-sort-qr': 'exitPhone', style: 'cursor:pointer' }, ['Celular salida']),
                el('th', { 'data-sort-qr': 'distance', style: 'cursor:pointer' }, ['Distancia']),
                el('th', { 'data-sort-qr': 'alert', style: 'cursor:pointer' }, ['Alerta'])
              ])
            ]),
            el('tbody', {})
          ])
        ]),
        el('div', { id: 'historicalQrCards', className: 'record-card-list' }, [])
      ]),
      el('p', { id: 'historicalQrTotal', className: 'text-muted' }, ['Total registros QR: 0'])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Pendientes de ingreso QR']),
      el('div', { style: 'display:flex;justify-content:space-between;gap:.75rem;align-items:center;flex-wrap:wrap;' }, [
        el('span', { id: 'historicalQrPendingSummary', className: 'text-muted', style: 'font-size:.86rem;' }, ['0 empleados pendientes']),
        el('div', { className: 'wa-field', style: 'min-width:220px;' }, [
          el('label', { className: 'label', for: 'historicalQrPendingZoneFilter' }, ['Zona']),
          el('select', { id: 'historicalQrPendingZoneFilter', className: 'input wa-input' }, [
            el('option', { value: 'all' }, ['Todas las zonas'])
          ])
        ])
      ]),
      el('div', { id: 'historicalQrPendingEmpty', className: 'text-muted mt-1', style: 'display:none;' }, ['Sin pendientes de ingreso QR para la fecha seleccionada.']),
      el('div', { id: 'historicalQrPendingWrap', className: 'responsive-records mt-1' }, [
        el('div', { className: 'table-wrap responsive-table-view' }, [
          el('table', { id: 'tblHistoricalQrPending', className: 'table wa-live-table' }, [
            el('thead', {}, [
              el('tr', {}, [
                el('th', { 'data-pending-sort-qr': 'documento', style: 'cursor:pointer' }, ['Cedula']),
                el('th', { 'data-pending-sort-qr': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
                el('th', { 'data-pending-sort-qr': 'telefono', style: 'cursor:pointer' }, ['Telefono']),
                el('th', { 'data-pending-sort-qr': 'sede', style: 'cursor:pointer' }, ['Sede']),
                el('th', { 'data-pending-sort-qr': 'dependencia', style: 'cursor:pointer' }, ['Dependencia']),
                el('th', { 'data-pending-sort-qr': 'zona', style: 'cursor:pointer' }, ['Zona'])
              ])
            ]),
            el('tbody', {})
          ])
        ]),
        el('div', { id: 'historicalQrPendingCards', className: 'record-card-list' }, [])
      ])
    ])
  ]);

  const recordsPaginator = createTablePagination(ui, { id: 'historicalQrRecords', after: '#historicalQrCards', onChange: render });
  const pendingPaginator = createTablePagination(ui, { id: 'historicalQrPending', after: '#historicalQrPendingCards', onChange: render });

  qs('#btnGenerateHistoricalQr', ui)?.addEventListener('click', generateReport);
  qs('#btnExportHistoricalQr', ui)?.addEventListener('click', exportExcel);
  qs('#historicalQrDate', ui)?.addEventListener('change', () => {
    rows = [];
    pendingRows = [];
    recordsPaginator.reset();
    pendingPaginator.reset();
    render();
    setMessage(' ');
  });
  qs('#historicalQrSearch', ui)?.addEventListener('input', (event) => {
    searchTerm = event.target.value || '';
    recordsPaginator.reset();
    pendingPaginator.reset();
    render();
  });
  qs('#historicalQrPendingZoneFilter', ui)?.addEventListener('change', (event) => {
    pendingZone = String(event.target.value || 'all').trim();
    pendingPaginator.reset();
    render();
  });
  ui.querySelectorAll('#tblHistoricalQr th[data-sort-qr]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-sort-qr') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else {
        sortKey = key;
        sortDir = 1;
      }
      recordsPaginator.reset();
      render();
    });
  });
  ui.querySelectorAll('#tblHistoricalQrPending th[data-pending-sort-qr]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = String(th.getAttribute('data-pending-sort-qr') || '').trim();
      if (!key) return;
      if (pendingSortKey === key) pendingSortDir *= -1;
      else {
        pendingSortKey = key;
        pendingSortDir = 1;
      }
      pendingPaginator.reset();
      render();
    });
  });

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  render();
  return ui;

  function statCard(label, id, value) {
    return el('article', { className: 'wa-stat card' }, [
      el('small', { className: 'wa-stat__label' }, [label]),
      el('strong', { id, className: 'wa-stat__value' }, [value])
    ]);
  }

  function setMessage(text) {
    qs('#historicalQrMsg', ui).textContent = text || ' ';
  }

  function todayBogota() {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
  }

  function yesterdayBogota() {
    const cursor = new Date(`${todayBogota()}T00:00:00Z`);
    cursor.setUTCDate(cursor.getUTCDate() - 1);
    return cursor.toISOString().slice(0, 10);
  }

  function normalize(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  function formatHour(value) {
    try {
      const date = value ? new Date(value) : null;
      if (!date || Number.isNaN(date.getTime())) return '-';
      return date.toLocaleTimeString('es-CO', {
        timeZone: 'America/Bogota',
        hour12: false,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
    } catch (_) {
      return '-';
    }
  }

  function formatEntry(row = {}) {
    const hour = formatHour(row.entryAt);
    const label = String(row.entryLabel || '').trim();
    if (!label) return hour;
    return hour === '-' ? label : `${hour} (${label})`;
  }

  function phone(value) {
    return String(value || '').trim() || '-';
  }

  function distanceLabel(row = {}) {
    const values = [row.entryDistanceMeters, row.exitDistanceMeters]
      .filter((value) => Number.isFinite(Number(value)))
      .map((value) => `${Number(value)} m`);
    return values.length ? values.join(' / ') : '-';
  }

  function alertText(row = {}) {
    return row.phoneDifferent ? 'Celular diferente' : 'OK';
  }

  function alertBadge(row = {}) {
    return row.phoneDifferent
      ? el('span', { className: 'badge badge--off' }, ['Celular diferente'])
      : el('span', { className: 'badge badge--ok' }, ['OK']);
  }

  function recordSearchText(row = {}) {
    return normalize([
      row.documento,
      row.nombre,
      row.sedeNombre,
      row.sedeCodigo,
      row.employeePhone,
      row.entryPhone,
      row.exitPhone
    ].join(' '));
  }

  function pendingSearchText(row = {}) {
    return normalize([
      row.documento,
      row.nombre,
      row.telefono,
      row.sedeNombre,
      row.sedeCodigo,
      row.dependenciaNombre,
      row.zonaNombre
    ].join(' '));
  }

  function sortValue(row = {}, key) {
    if (key === 'sede') return normalize(row.sedeNombre || row.sedeCodigo);
    if (key === 'distance') return Math.max(Number(row.entryDistanceMeters || 0), Number(row.exitDistanceMeters || 0));
    if (key === 'alert') return row.phoneDifferent ? 1 : 0;
    return normalize(row[key]);
  }

  function pendingSortValue(row = {}, key) {
    if (key === 'sede') return normalize(row.sedeNombre || row.sedeCodigo);
    if (key === 'dependencia') return normalize(row.dependenciaNombre || row.dependenciaCodigo);
    if (key === 'zona') return normalize(row.zonaNombre || row.zonaCodigo);
    return normalize(row[key]);
  }

  function sortRows(list, key, dir, getter = sortValue) {
    return [...(list || [])].sort((a, b) => {
      const va = getter(a, key);
      const vb = getter(b, key);
      if (va === vb) return 0;
      return va > vb ? dir : -dir;
    });
  }

  function updateZoneOptions(sourceRows = []) {
    const select = qs('#historicalQrPendingZoneFilter', ui);
    if (!select) return;
    const current = String(select.value || pendingZone || 'all').trim();
    const zones = [...new Set(sourceRows.map((row) => String(row.zonaNombre || row.zonaCodigo || '').trim()).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b));
    select.replaceChildren(
      el('option', { value: 'all' }, ['Todas las zonas']),
      ...zones.map((zone) => el('option', { value: zone }, [zone]))
    );
    select.value = zones.includes(current) ? current : 'all';
    pendingZone = String(select.value || 'all').trim();
  }

  function filteredRecords() {
    const term = normalize(searchTerm);
    return term ? rows.filter((row) => recordSearchText(row).includes(term)) : rows;
  }

  function filteredPending() {
    const term = normalize(searchTerm);
    const searched = term ? pendingRows.filter((row) => pendingSearchText(row).includes(term)) : pendingRows;
    updateZoneOptions(searched);
    return searched.filter((row) => (
      pendingZone === 'all' || String(row.zonaNombre || row.zonaCodigo || '').trim() === pendingZone
    ));
  }

  function renderStats(visibleRecords = rows, visiblePending = pendingRows) {
    const entries = rows.filter((row) => row.entryAt).length;
    qs('#historicalQrScheduled', ui).textContent = String(entries + pendingRows.length);
    qs('#historicalQrEntries', ui).textContent = String(entries);
    qs('#historicalQrWithExit', ui).textContent = String(rows.filter((row) => row.exitAt).length);
    qs('#historicalQrPending', ui).textContent = String(pendingRows.length);
    qs('#historicalQrPhoneAlerts', ui).textContent = String(rows.filter((row) => row.phoneDifferent).length);
    qs('#historicalQrTotal', ui).textContent = `Total registros QR: ${rows.length}. Filtrados: ${visibleRecords.length}.`;
    qs('#historicalQrPendingSummary', ui).textContent = `${visiblePending.length} de ${pendingRows.length} empleado${pendingRows.length === 1 ? '' : 's'} pendiente${pendingRows.length === 1 ? '' : 's'}`;
  }

  function renderRecords(sourceRows) {
    const tbody = qs('#tblHistoricalQr tbody', ui);
    const cards = qs('#historicalQrCards', ui);
    const pageRows = recordsPaginator.slice(sourceRows);
    if (!pageRows.length) {
      tbody.replaceChildren(el('tr', {}, [el('td', { colSpan: 10, className: 'text-muted' }, ['Sin registros QR para la fecha o filtros seleccionados.'])]));
      cards.replaceChildren(el('p', { className: 'text-muted record-card__empty' }, ['Sin registros QR para la fecha o filtros seleccionados.']));
      return;
    }
    tbody.replaceChildren(...pageRows.map((row) => el('tr', { className: row.phoneDifferent ? 'table-row-warning' : '' }, [
      el('td', {}, [row.documento || '-']),
      el('td', {}, [row.nombre || '-']),
      el('td', {}, [row.sedeNombre || row.sedeCodigo || '-']),
      el('td', {}, [formatEntry(row)]),
      el('td', {}, [formatHour(row.exitAt)]),
      el('td', {}, [phone(row.employeePhone)]),
      el('td', { className: row.entryPhoneDifferent ? 'text-danger' : '' }, [phone(row.entryPhone)]),
      el('td', { className: row.exitPhoneDifferent ? 'text-danger' : '' }, [phone(row.exitPhone)]),
      el('td', {}, [distanceLabel(row)]),
      el('td', {}, [alertBadge(row)])
    ])));
    cards.replaceChildren(...pageRows.map((row) => recordCard({
      title: row.nombre || '-',
      subtitle: `Cedula: ${row.documento || '-'}`,
      badge: alertBadge(row),
      warning: row.phoneDifferent,
      meta: [
        ['Sede', row.sedeNombre || row.sedeCodigo || '-'],
        ['Ingreso', formatEntry(row)],
        ['Salida', formatHour(row.exitAt)],
        ['Celular empleado', phone(row.employeePhone)],
        ['Celular ingreso', phone(row.entryPhone)],
        ['Celular salida', phone(row.exitPhone)],
        ['Distancia', distanceLabel(row)]
      ]
    })));
  }

  function renderPending(sourceRows) {
    const tbody = qs('#tblHistoricalQrPending tbody', ui);
    const cards = qs('#historicalQrPendingCards', ui);
    const empty = qs('#historicalQrPendingEmpty', ui);
    const wrap = qs('#historicalQrPendingWrap', ui);
    const pageRows = pendingPaginator.slice(sourceRows);
    if (!pageRows.length) {
      tbody.replaceChildren();
      cards.replaceChildren();
      if (empty) {
        empty.textContent = pendingZone === 'all'
          ? 'Sin pendientes de ingreso QR para la fecha seleccionada.'
          : 'Sin pendientes de ingreso QR para la zona seleccionada.';
        empty.style.display = '';
      }
      if (wrap) wrap.style.display = 'none';
      return;
    }
    if (empty) empty.style.display = 'none';
    if (wrap) wrap.style.display = '';
    tbody.replaceChildren(...pageRows.map((row) => el('tr', {}, [
      el('td', {}, [row.documento || '-']),
      el('td', {}, [row.nombre || '-']),
      el('td', {}, [phone(row.telefono)]),
      el('td', {}, [row.sedeNombre || row.sedeCodigo || '-']),
      el('td', {}, [row.dependenciaNombre || row.dependenciaCodigo || '-']),
      el('td', {}, [row.zonaNombre || row.zonaCodigo || '-'])
    ])));
    cards.replaceChildren(...pageRows.map((row) => recordCard({
      title: row.nombre || '-',
      subtitle: `Cedula: ${row.documento || '-'}`,
      badge: el('span', { className: 'badge' }, [row.zonaNombre || row.zonaCodigo || 'Pendiente']),
      meta: [
        ['Telefono', phone(row.telefono)],
        ['Sede', row.sedeNombre || row.sedeCodigo || '-'],
        ['Dependencia', row.dependenciaNombre || row.dependenciaCodigo || '-'],
        ['Zona', row.zonaNombre || row.zonaCodigo || '-']
      ]
    })));
  }

  function recordCard({ title, subtitle, badge, meta = [], warning = false }) {
    return el('article', { className: `record-card ${warning ? 'table-row-warning' : ''}`.trim() }, [
      el('div', { className: 'record-card__header' }, [
        el('div', { className: 'record-card__identity' }, [
          el('strong', { className: 'record-card__title' }, [title]),
          el('span', { className: 'record-card__subtitle' }, [subtitle])
        ]),
        badge || el('span', { className: 'badge' }, ['QR'])
      ]),
      el('dl', { className: 'record-card__meta' }, meta.map(([label, value]) => el('div', { className: 'record-card__meta-item' }, [
        el('dt', {}, [label]),
        el('dd', {}, Array.isArray(value) ? value : [value || '-'])
      ])))
    ]);
  }

  function render() {
    const visibleRecords = sortRows(filteredRecords(), sortKey, sortDir);
    const visiblePending = sortRows(filteredPending(), pendingSortKey, pendingSortDir, pendingSortValue);
    renderRecords(visibleRecords);
    renderPending(visiblePending);
    renderStats(visibleRecords, visiblePending);
    updateSortIndicators(ui, '#tblHistoricalQr th[data-sort-qr]', 'data-sort-qr', sortKey, sortDir);
    updateSortIndicators(ui, '#tblHistoricalQrPending th[data-pending-sort-qr]', 'data-pending-sort-qr', pendingSortKey, pendingSortDir);
    const exportBtn = qs('#btnExportHistoricalQr', ui);
    if (exportBtn) exportBtn.disabled = !canExport || (rows.length === 0 && pendingRows.length === 0);
  }

  async function generateReport() {
    if (running) return;
    const input = qs('#historicalQrDate', ui);
    const date = String(input?.value || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      setMessage('Selecciona una fecha valida para generar el reporte.');
      return;
    }
    if (date >= todayBogota()) {
      setMessage('Este modulo solo permite consultar fechas anteriores al dia actual.');
      return;
    }
    if (date > maxDate) {
      setMessage(`La fecha maxima permitida es ${maxDate}.`);
      return;
    }
    running = true;
    selectedDate = date;
    const btn = qs('#btnGenerateHistoricalQr', ui);
    try {
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Consultando...';
      }
      const isClosed = await deps.isOperationDayClosed?.(date);
      if (!isClosed) throw new Error('Solo se pueden generar reportes historicos de dias cerrados.');
      if (typeof deps.listDailyQrRecords !== 'function') throw new Error('No esta disponible la consulta de registro QR.');
      const summary = await deps.listDailyQrRecords?.(date);
      rows = (summary?.rows || []).map((row) => ({ ...row, fecha: date }));
      pendingRows = (summary?.pendingRows || []).map((row) => ({ ...row, fecha: date }));
      recordsPaginator.reset();
      pendingPaginator.reset();
      render();
      setMessage(`Reporte QR generado para ${date}. Registros: ${rows.length}. Pendientes: ${pendingRows.length}.`);
    } catch (error) {
      rows = [];
      pendingRows = [];
      recordsPaginator.reset();
      pendingPaginator.reset();
      render();
      setMessage(`Error al generar historico QR: ${error?.message || error}`);
    } finally {
      running = false;
      if (btn) {
        btn.disabled = false;
        btn.textContent = 'Consultar fecha';
      }
    }
  }

  async function exportExcel() {
    const btn = qs('#btnExportHistoricalQr', ui);
    try {
      if (!rows.length && !pendingRows.length) throw new Error('Primero genera el reporte.');
      if (!canExport) throw new Error('No tienes permiso para exportar este reporte.');
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Generando...';
      }
      const mod = await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb = mod.utils.book_new();
      const recordsSheet = mod.utils.json_to_sheet(rows.map((row) => ({
        Fecha: selectedDate,
        Cedula: row.documento || '',
        Nombre: row.nombre || '',
        Sede: row.sedeNombre || row.sedeCodigo || '',
        Ingreso: formatEntry(row),
        Salida: formatHour(row.exitAt),
        'Celular empleado': phone(row.employeePhone),
        'Celular ingreso': phone(row.entryPhone),
        'Celular salida': phone(row.exitPhone),
        'Distancia ingreso': row.entryDistanceMeters ?? '',
        'Distancia salida': row.exitDistanceMeters ?? '',
        Alerta: alertText(row)
      })));
      recordsSheet['!cols'] = [{ wch: 12 }, { wch: 18 }, { wch: 30 }, { wch: 28 }, { wch: 18 }, { wch: 14 }, { wch: 18 }, { wch: 18 }, { wch: 18 }, { wch: 18 }, { wch: 18 }, { wch: 22 }];
      mod.utils.book_append_sheet(wb, recordsSheet, 'Registros QR');

      const pendingSheet = mod.utils.json_to_sheet(pendingRows.map((row) => ({
        Fecha: selectedDate,
        Cedula: row.documento || '',
        Nombre: row.nombre || '',
        Telefono: phone(row.telefono),
        Sede: row.sedeNombre || row.sedeCodigo || '',
        Dependencia: row.dependenciaNombre || row.dependenciaCodigo || '',
        Zona: row.zonaNombre || row.zonaCodigo || ''
      })));
      pendingSheet['!cols'] = [{ wch: 12 }, { wch: 18 }, { wch: 30 }, { wch: 18 }, { wch: 28 }, { wch: 24 }, { wch: 24 }];
      mod.utils.book_append_sheet(wb, pendingSheet, 'Pendientes QR');

      mod.writeFile(wb, `historico_registro_qr_${selectedDate}.xlsx`);
      setMessage(`Excel QR generado correctamente para ${selectedDate}.`);
    } catch (error) {
      setMessage(`Error al generar Excel: ${error?.message || error}`);
    } finally {
      if (btn) {
        btn.disabled = !canExport || (rows.length === 0 && pendingRows.length === 0);
        btn.textContent = 'Exportar Excel';
      }
    }
  }
};

function updateSortIndicators(scope, selector, attrName, activeKey, dir) {
  scope.querySelectorAll(selector).forEach((th) => {
    const base = th.dataset.baseLabel || String(th.textContent || '').replace(/\s[\^v]$/, '');
    th.dataset.baseLabel = base;
    const key = String(th.getAttribute(attrName) || '').trim();
    th.textContent = key && key === activeKey ? base + ' ' + (dir === 1 ? '^' : 'v') : base;
  });
}
