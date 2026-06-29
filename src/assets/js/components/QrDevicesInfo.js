import { el, qs } from '../utils/dom.js';
import { createTablePagination } from '../utils/pagination.js';
import { showActionModal } from '../utils/actionModal.js';
import { showInfoModal } from '../utils/infoModal.js';

export const QrDevicesInfo = (mount, deps = {}) => {
  let rows = [];
  let searchTerm = '';
  let statusFilter = 'activo';
  let sortKey = 'deviceName';
  let sortDir = 1;

  const ui = el('section', { className: 'main-card' }, [
    el('div', { className: 'wa-header__top' }, [
      el('h2', {}, ['Tablets QR']),
      el('span', { id: 'qrDevicesStatus', className: 'badge badge--off' }, ['Cargando'])
    ]),
    el('section', { className: 'wa-stats wa-stats--nov mt-2' }, [
      statCard('Tablets activas', 'qrDevicesActive', '0'),
      statCard('Sedes atendidas', 'qrDevicesSites', '0'),
      statCard('Sin actividad', 'qrDevicesIdle', '0')
    ]),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label', for: 'qrDeviceSearch' }, ['Buscar']),
        el('input', { id: 'qrDeviceSearch', className: 'input', placeholder: 'Tablet, sede o usuario...' })
      ]),
      el('div', {}, [
        el('label', { className: 'label', for: 'qrDeviceStatusFilter' }, ['Estado']),
        el('select', { id: 'qrDeviceStatusFilter', className: 'select' }, [
          el('option', { value: 'activo', selected: true }, ['Activas']),
          el('option', { value: '' }, ['Todas']),
          el('option', { value: 'inactivo' }, ['Inactivas'])
        ])
      ])
    ]),
    el('div', { className: 'mt-2 table-wrap' }, [
      el('table', { id: 'qrDevicesTable', className: 'table wa-live-table' }, [
        el('thead', {}, [
          el('tr', {}, [
            th('deviceName', 'Tablet'),
            th('estado', 'Estado'),
            th('sedes', 'Sedes que atiende'),
            th('lastSeenAt', 'Ultima actividad'),
            th('createdAt', 'Creada'),
            th('createdByEmail', 'Creada por'),
            el('th', {}, ['Acciones'])
          ])
        ]),
        el('tbody', { id: 'qrDevicesTbody' }, [
          el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Cargando tablets QR...'])])
        ])
      ])
    ]),
    el('p', { id: 'qrDevicesMsg', className: 'text-muted mt-2' }, [' '])
  ]);

  const paginator = createTablePagination(ui, { id: 'qrDevices', after: '#qrDevicesTable', onChange: render });

  function statCard(label, id, value) {
    return el('article', { className: 'wa-stat card' }, [
      el('small', { className: 'wa-stat__label' }, [label]),
      el('strong', { id, className: 'wa-stat__value' }, [value])
    ]);
  }

  function th(key, label) {
    return el('th', { 'data-sort': key, style: 'cursor:pointer' }, [label]);
  }

  function normalize(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
  }

  function formatDateTime(value) {
    try {
      const date = value ? new Date(value) : null;
      if (!date || Number.isNaN(date.getTime())) return '-';
      return date.toLocaleString('es-CO', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch (_) {
      return '-';
    }
  }

  function siteLabel(site = {}) {
    const name = String(site.sedeNombre || '').trim();
    const code = String(site.sedeCodigo || '').trim();
    if (name && code) return `${name} (${code})`;
    return name || code || '-';
  }

  function rowText(row = {}) {
    return normalize([
      row.deviceName,
      row.estado,
      row.createdByEmail,
      ...(row.sedes || []).flatMap((site) => [site.sedeNombre, site.sedeCodigo])
    ].join(' '));
  }

  function sortValue(row = {}, key) {
    if (key === 'sedes') return normalize((row.sedes || []).map(siteLabel).join(' '));
    if (key === 'lastSeenAt' || key === 'createdAt') return row[key] ? new Date(row[key]).getTime() : 0;
    return normalize(row[key]);
  }

  function filteredRows() {
    const term = normalize(searchTerm);
    return rows.filter((row) => {
      const status = effectiveStatus(row);
      return (!statusFilter || status === statusFilter)
        && (!term || rowText(row).includes(term));
    });
  }

  function effectiveStatus(row = {}) {
    const active = String(row.estado || 'activo').trim().toLowerCase() === 'activo' && !row.revokedAt;
    return active ? 'activo' : 'inactivo';
  }

  function renderStats() {
    const active = rows.filter((row) => effectiveStatus(row) === 'activo');
    const siteCodes = new Set(active.flatMap((row) => (row.sedes || []).map((site) => String(site.sedeCodigo || '').trim()).filter(Boolean)));
    const idle = active.filter((row) => !row.lastSeenAt).length;
    qs('#qrDevicesActive', ui).textContent = String(active.length);
    qs('#qrDevicesSites', ui).textContent = String(siteCodes.size);
    qs('#qrDevicesIdle', ui).textContent = String(idle);
    const status = qs('#qrDevicesStatus', ui);
    status.className = `badge ${active.length ? 'badge--ok' : 'badge--off'}`;
    status.textContent = active.length ? 'Con tablets activas' : 'Sin tablets activas';
  }

  function render() {
    const data = filteredRows();
    const sorted = [...data].sort((a, b) => {
      const va = sortValue(a, sortKey);
      const vb = sortValue(b, sortKey);
      if (va === vb) return 0;
      return va > vb ? sortDir : -sortDir;
    });
    const pageRows = paginator.slice(sorted);
    const tbody = qs('#qrDevicesTbody', ui);
    if (!pageRows.length) {
      tbody.replaceChildren(el('tr', {}, [el('td', { colSpan: 7, className: 'text-muted' }, ['Sin tablets QR para los filtros actuales.'])]));
    } else {
      tbody.replaceChildren(...pageRows.map((row) => el('tr', {}, [
        el('td', {}, [row.deviceName || '-']),
        el('td', {}, [statusBadge(row)]),
        el('td', {}, [siteBadges(row.sedes || [])]),
        el('td', {}, [formatDateTime(row.lastSeenAt)]),
        el('td', {}, [formatDateTime(row.createdAt)]),
        el('td', {}, [row.createdByEmail || '-']),
        el('td', {}, [actionsCell(row)])
      ])));
    }
    qs('#qrDevicesMsg', ui).textContent = `Tablets filtradas: ${data.length}.`;
    renderStats();
    updateSortIndicators();
  }

  function statusBadge(row = {}) {
    const active = effectiveStatus(row) === 'activo';
    return el('span', { className: `badge ${active ? 'badge--ok' : 'badge--off'}` }, [active ? 'Activa' : 'Inactiva']);
  }

  function siteBadges(sites = []) {
    if (!sites.length) return el('span', { className: 'text-muted' }, ['Sin sedes']);
    return el('div', { style: 'display:flex;gap:.35rem;flex-wrap:wrap;' }, sites.map((site) => (
      el('span', { className: 'badge' }, [siteLabel(site)])
    )));
  }

  function actionsCell(row = {}) {
    const box = el('div', { className: 'row-actions' }, []);
    const btnMore = el('button', { className: 'btn btn--icon', type: 'button', title: 'Mas opciones', 'aria-label': 'Mas opciones' }, ['\u22EF']);
    btnMore.addEventListener('click', () => openMoreOptionsModal(row));
    const btnInfo = el('button', { className: 'btn btn--icon', type: 'button', title: 'Ver informacion', 'aria-label': 'Ver informacion' }, ['\u24D8']);
    btnInfo.addEventListener('click', () => showDeviceInfo(row));
    box.append(btnMore, btnInfo);
    return box;
  }

  async function openMoreOptionsModal(row = {}) {
    const active = effectiveStatus(row) === 'activo';
    const nextStatus = active ? 'inactivo' : 'activo';
    const modal = await showActionModal({
      title: 'Mas opciones',
      message: `Tablet: ${row.deviceName || '-'}`,
      confirmText: 'Continuar',
      fields: [{
        id: 'action',
        label: 'Accion',
        type: 'select',
        required: true,
        options: [
          { value: '', label: 'Seleccione...' },
          { value: 'toggle', label: active ? 'Inactivar tablet' : 'Reactivar tablet' }
        ]
      }]
    });
    if (!modal.confirmed || modal.values.action !== 'toggle') return;
    await confirmStatusChange(row, nextStatus);
  }

  async function confirmStatusChange(row = {}, nextStatus) {
    const isInactive = nextStatus === 'inactivo';
    const modal = await showActionModal({
      title: isInactive ? 'Inactivar tablet QR' : 'Reactivar tablet QR',
      message: `Tablet: ${row.deviceName || '-'}`,
      confirmText: isInactive ? 'Inactivar' : 'Reactivar',
      fields: [{ id: 'detail', label: 'Detalle', type: 'textarea', required: true, placeholder: 'Escribe el motivo o detalle de esta accion' }]
    });
    if (!modal.confirmed) return;
    try {
      await deps.setQrDeviceStatus?.(row.id, nextStatus);
      await deps.addAuditLog?.({
        targetType: 'sede_device',
        targetId: row.id,
        action: isInactive ? 'deactivate_qr_device' : 'activate_qr_device',
        before: { estado: row.estado, revokedAt: row.revokedAt || null },
        after: { estado: nextStatus, revokedAt: isInactive ? new Date().toISOString() : null },
        note: modal.values.detail || null
      });
      await loadRows();
    } catch (error) {
      showInfoModal('No fue posible actualizar', [String(error?.message || error || 'Error desconocido.')]);
    }
  }

  function showDeviceInfo(row = {}) {
    const hasModification = Boolean(row.lastModifiedAt || row.lastModifiedByEmail || row.revokedAt || row.revokedByEmail);
    const event = row.revokedAt
      ? 'Inactivacion'
      : hasModification
        ? 'Ultima modificacion'
        : 'Creacion';
    const user = row.revokedAt
      ? (row.revokedByEmail || row.lastModifiedByEmail || row.createdByEmail || '-')
      : hasModification
        ? (row.lastModifiedByEmail || row.createdByEmail || '-')
        : (row.createdByEmail || '-');
    const date = row.revokedAt || row.lastModifiedAt || row.createdAt || null;
    showInfoModal('Informacion de tablet QR', [
      `Evento: ${event}`,
      `Usuario: ${user}`,
      `Fecha: ${formatDateTime(date)}`,
      `Tablet: ${row.deviceName || '-'}`,
      `Sedes: ${(row.sedes || []).map(siteLabel).join(', ') || '-'}`
    ]);
  }

  function updateSortIndicators() {
    ui.querySelectorAll('th[data-sort]').forEach((node) => {
      const base = node.dataset.baseLabel || node.textContent.replace(/\s[\^v▲▼]$/, '');
      node.dataset.baseLabel = base;
      const key = node.getAttribute('data-sort');
      node.textContent = sortKey === key ? `${base} ${sortDir === 1 ? '▲' : '▼'}` : base;
    });
  }

  qs('#qrDeviceSearch', ui)?.addEventListener('input', (event) => {
    searchTerm = event.target.value || '';
    paginator.reset();
    render();
  });
  qs('#qrDeviceStatusFilter', ui)?.addEventListener('change', (event) => {
    statusFilter = String(event.target.value || '').trim();
    paginator.reset();
    render();
  });
  ui.querySelectorAll('th[data-sort]').forEach((node) => {
    node.addEventListener('click', () => {
      const key = String(node.getAttribute('data-sort') || '').trim();
      if (!key) return;
      if (sortKey === key) sortDir *= -1;
      else { sortKey = key; sortDir = 1; }
      paginator.reset();
      render();
    });
  });

  async function loadRows() {
    try {
      if (typeof deps.listQrDevices === 'function') {
        rows = await deps.listQrDevices();
      } else if (typeof deps.streamQrDevices === 'function') {
        await new Promise((resolve) => {
          const un = deps.streamQrDevices((nextRows = []) => {
            rows = Array.isArray(nextRows) ? nextRows : [];
            try { un?.(); } catch (_) {}
            resolve();
          }, () => {
            rows = [];
            try { un?.(); } catch (_) {}
            resolve();
          });
        });
      } else {
        rows = [];
        qs('#qrDevicesMsg', ui).textContent = 'No esta disponible la consulta de tablets QR.';
      }
      render();
    } catch (error) {
      rows = [];
      render();
      qs('#qrDevicesMsg', ui).textContent = `Error cargando tablets QR: ${error?.message || error}`;
    }
  }

  mount.replaceChildren(ui);
  loadRows();

  return () => {
  };
};
