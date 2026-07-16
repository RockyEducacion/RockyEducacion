import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';

const SUPPORT_ACCEPT = 'application/pdf,image/png,image/jpeg,image/webp';

export function EmployeeIncapacities(mount, { apiRequest, session } = {}) {
  if (typeof apiRequest !== 'function') {
    mount.replaceChildren(employeeIncapacityCard([
      el('p', { className: 'text-muted' }, ['No fue posible cargar el portal de incapacidades.'])
    ]));
    return () => {};
  }

  const today = todayBogota();
  const defaultFrom = addDaysToIsoDate(today, -90) || today;
  let rows = [];
  let loading = false;

  const msg = el('p', { className: 'employee-incapacity-message text-muted' }, [' ']);
  const list = el('div', { className: 'employee-incapacity-list' }, []);
  const ui = employeeIncapacityCard([
    el('div', { className: 'employee-incapacity-header' }, [
      el('div', {}, [
        el('p', { className: 'employee-card__kicker employee-incapacity-title' }, ['Incapacidades']),
        el('p', { className: 'text-muted' }, ['Adjunta el soporte de las incapacidades'])
      ]),
      el('button', { id: 'empIncRefresh', className: 'btn', type: 'button' }, ['Actualizar'])
    ]),
    msg,
    list,
    el('div', { className: 'employee-incapacity-filters' }, [
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'empIncFrom' }, ['Desde']),
        el('input', { id: 'empIncFrom', className: 'input', type: 'date', value: defaultFrom, max: today })
      ]),
      el('div', {}, [
        el('label', { className: 'label', htmlFor: 'empIncTo' }, ['Hasta']),
        el('input', { id: 'empIncTo', className: 'input', type: 'date', value: today, max: today })
      ]),
      el('button', { id: 'empIncSearch', className: 'btn btn--primary', type: 'button' }, ['Buscar'])
    ])
  ]);

  mount.replaceChildren(ui);

  qs('#empIncRefresh', ui)?.addEventListener('click', loadRows);
  qs('#empIncSearch', ui)?.addEventListener('click', loadRows);
  loadRows();

  return () => {};

  async function loadRows() {
    if (loading) return;
    const dateFrom = String(qs('#empIncFrom', ui)?.value || '').trim();
    const dateTo = String(qs('#empIncTo', ui)?.value || '').trim();
    if (!dateFrom || !dateTo) {
      setMessage('Selecciona el rango de fechas.', 'error');
      return;
    }
    if (dateTo < dateFrom) {
      setMessage('La fecha final no puede ser menor a la inicial.', 'error');
      return;
    }

    loading = true;
    setBusy(true);
    setMessage('Consultando incapacidades...', 'ok');
    renderRows({ busy: true });

    try {
      const data = await apiRequest(`/api/employee-incapacities?dateFrom=${encodeURIComponent(dateFrom)}&dateTo=${encodeURIComponent(dateTo)}`, { method: 'GET' });
      rows = Array.isArray(data?.rows) ? data.rows : [];
      setMessage(`${rows.length} incapacidad(es) en el rango seleccionado.`, rows.length ? 'ok' : '');
    } catch (error) {
      rows = [];
      setMessage(error?.message || 'No fue posible consultar tus incapacidades.', 'error');
      if (error?.redirectMain) {
        window.setTimeout(() => {
          window.location.href = 'access.html';
        }, 1400);
      }
    } finally {
      loading = false;
      setBusy(false);
      renderRows();
    }
  }

  function renderRows({ busy = loading } = {}) {
    if (busy) {
      list.replaceChildren(
        el('div', { className: 'employee-incapacity-empty' }, ['Cargando...'])
      );
      return;
    }

    if (!rows.length) {
      list.replaceChildren(
        el('div', { className: 'employee-incapacity-empty' }, ['No hay incapacidades para mostrar en este rango.'])
      );
      return;
    }

    try {
      const cards = rows.map(renderIncapacityCard);
      list.replaceChildren(...cards);
    } catch (error) {
      console.error('No se pudieron renderizar las incapacidades del portal:', error);
      list.replaceChildren(
        el('div', { className: 'employee-incapacity-empty' }, ['No fue posible mostrar las incapacidades. Actualiza e intenta nuevamente.'])
      );
    }
  }

  function renderIncapacityCard(row) {
    const hasSupport = Boolean(String(row?.soporteUrl || '').trim());
    const supportButton = el('button', { className: `btn ${hasSupport ? '' : 'btn--primary'}`, type: 'button' }, [
      hasSupport ? 'Reemplazar soporte' : 'Adjuntar soporte'
    ]);
    supportButton.addEventListener('click', () => uploadSupport(row));

    const actions = [supportButton];
    if (hasSupport) {
      actions.unshift(el('a', {
        className: 'btn',
        href: row.soporteUrl,
        target: '_blank',
        rel: 'noopener noreferrer'
      }, ['Ver soporte']));
    }

    return el('article', { className: `employee-incapacity-item ${hasSupport ? 'has-support' : 'missing-support'}` }, [
      el('div', { className: 'employee-incapacity-item__main' }, [
        el('div', {}, [
          el('p', { className: 'employee-incapacity-item__type' }, [row?.source || 'Incapacidad']),
          el('h3', {}, [`${formatDate(row?.fechaInicio)} a ${formatDate(row?.fechaFin)}`])
        ]),
        statusBadge(row?.estado)
      ]),
      el('dl', { className: 'employee-incapacity-details' }, [
        detailItem('Canal', channelLabel(row)),
        detailItem('Soporte', hasSupport ? (row?.soporteNombre || 'Cargado') : 'Pendiente'),
        detailItem('Registro', formatDate(row?.createdAt))
      ]),
      el('div', { className: 'employee-incapacity-actions' }, actions)
    ]);
  }

  async function uploadSupport(row) {
    try {
      const file = await pickSupportFile();
      if (!file) return;
      setMessage('Cargando soporte...', 'ok');
      const supportDataUrl = await fileToDataUrl(file);
      const result = await apiRequest(`/api/employee-incapacities/${encodeURIComponent(row.id)}/support`, {
        method: 'POST',
        body: JSON.stringify({
          soporte: {
            name: file.name,
            dataUrl: supportDataUrl
          }
        })
      });
      if (result?.row) {
        rows = rows.map((item) => String(item?.id || '') === String(row?.id || '') ? result.row : item);
        renderRows();
      }
      setMessage('Soporte cargado correctamente.', 'ok');
      showInfoModal('Soporte actualizado', ['El soporte quedo asociado a la incapacidad seleccionada.']);
    } catch (error) {
      setMessage(error?.message || 'No fue posible cargar el soporte.', 'error');
      if (error?.redirectMain) {
        window.setTimeout(() => {
          window.location.href = 'access.html';
        }, 1400);
      }
    }
  }

  function setBusy(value) {
    qs('#empIncRefresh', ui)?.toggleAttribute('disabled', value);
    qs('#empIncSearch', ui)?.toggleAttribute('disabled', value);
  }

  function setMessage(text, state = '') {
    msg.textContent = text || ' ';
    if (state) msg.dataset.state = state;
    else delete msg.dataset.state;
  }
}

function employeeIncapacityCard(children = []) {
  return el('section', { className: 'main-card employee-card employee-incapacity-card' }, children);
}

function detailItem(label, value) {
  return el('div', {}, [
    el('dt', {}, [label]),
    el('dd', {}, [value || '-'])
  ]);
}

function statusBadge(state) {
  const active = String(state || 'activo').trim().toLowerCase() === 'activo';
  return el('span', { className: `badge ${active ? 'badge--ok' : 'badge--off'}` }, [active ? 'Vigente' : 'Anulada']);
}

function channelLabel(row = {}) {
  const key = String(row?.canalRegistro || '').trim().toLowerCase();
  if (key === 'whatsapp') return 'WhatsApp';
  if (key === 'portal_web') return 'Portal empleados';
  if (key === 'manual') return 'Manual';
  if (String(row?.whatsappMessageId || '').trim()) return 'WhatsApp';
  return '-';
}

function formatDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  try {
    const date = new Date(raw);
    if (Number.isNaN(date.getTime())) return raw;
    return date.toLocaleDateString('es-CO');
  } catch {
    return raw;
  }
}

function pickSupportFile() {
  return new Promise((resolve) => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = SUPPORT_ACCEPT;
    input.addEventListener('change', () => resolve(input.files?.[0] || null), { once: true });
    input.click();
  });
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = () => reject(new Error('No fue posible leer el soporte seleccionado.'));
    reader.readAsDataURL(file);
  });
}

function addDaysToIsoDate(value, days = 1) {
  const iso = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;
  const [year, month, day] = iso.split('-').map((n) => Number(n));
  const utc = new Date(Date.UTC(year, (month || 1) - 1, day || 1));
  utc.setUTCDate(utc.getUTCDate() + Number(days || 0));
  const y = utc.getUTCFullYear();
  const m = String(utc.getUTCMonth() + 1).padStart(2, '0');
  const d = String(utc.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}
