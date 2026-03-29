import { el, qs } from '../utils/dom.js';

export const ImportHistory = (mount, deps = {}) => {
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Historial']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [el('label', { className: 'label' }, ['Buscar']), el('input', { id: 'txtSearch', className: 'input', placeholder: 'Fecha o usuario...' })]),
      el('div', {}, [el('label', { className: 'label' }, ['Fecha cierre']), el('input', { id: 'fltDate', className: 'input', type: 'date' })]),
      el('button', { id: 'btnClear', className: 'btn', type: 'button' }, ['Limpiar filtros'])
    ]),
    el('div', { className: 'mt-2 table-wrap' }, [
      el('table', { className: 'table' }, [
        el('thead', {}, [el('tr', {}, [
          el('th', { 'data-sort': 'fecha', style: 'cursor:pointer' }, ['Fecha cierre']),
          el('th', { 'data-sort': 'confirmedBy', style: 'cursor:pointer' }, ['Confirmado por']),
          el('th', { 'data-sort': 'planeados', style: 'cursor:pointer' }, ['Planeados']),
          el('th', { 'data-sort': 'contratados', style: 'cursor:pointer' }, ['Contratados']),
          el('th', { 'data-sort': 'registrados', style: 'cursor:pointer' }, ['Asistencias']),
          el('th', { 'data-sort': 'faltan', style: 'cursor:pointer' }, ['Faltan']),
          el('th', { 'data-sort': 'sobran', style: 'cursor:pointer' }, ['Sobran']),
          el('th', { 'data-sort': 'ausentismos', style: 'cursor:pointer' }, ['Ausentismos']),
          el('th', {}, ['Detalle'])
        ])]),
        el('tbody', {})
      ])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { id: 'detailTitle', className: 'section-title' }, ['Detalle del dia']),
      el('div', { className: 'table-wrap' }, [
        el('table', { className: 'table', id: 'tblDetail' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', { 'data-sort-detail': 'fecha', style: 'cursor:pointer' }, ['Fecha']),
            el('th', { 'data-sort-detail': 'hora', style: 'cursor:pointer' }, ['Hora']),
            el('th', { 'data-sort-detail': 'sede', style: 'cursor:pointer' }, ['Sede']),
            el('th', { 'data-sort-detail': 'documento', style: 'cursor:pointer' }, ['Documento']),
            el('th', { 'data-sort-detail': 'nombre', style: 'cursor:pointer' }, ['Nombre']),
            el('th', { 'data-sort-detail': 'novedad', style: 'cursor:pointer' }, ['Novedad']),
            el('th', { 'data-sort-detail': 'estado', style: 'cursor:pointer' }, ['Estado'])
          ])]),
          el('tbody', {})
        ])
      ])
    ]),
    el('p', { id: 'msg', className: 'text-muted mt-2' }, ['Cargando...'])
  ]);

  const tbody = qs('tbody', ui);
  let snapshot = [];
  let sortKey = 'fecha';
  let sortDir = -1;
  let detailSnapshot = [];
  let detailSortKey = 'hora';
  let detailSortDir = -1;

  const sortVal = (r, key) => {
    if (key === 'fecha') return String(r.fecha || '');
    if (key === 'confirmedBy') return String(r.confirmedBy || '').toLowerCase();
    return Number(r[key] || 0);
  };

  function toRow(raw) {
    return {
      id: raw.id,
      fecha: String(raw.fecha || raw.id || '').trim(),
      confirmedBy: String(raw.closedByEmail || raw.closedByUid || raw.source || '-').trim(),
      planeados: Number(raw.planeados || 0),
      contratados: Number(raw.contratados || 0),
      registrados: Number(raw.asistencias || raw.registrados || 0),
      faltan: Number(raw.faltan || 0),
      sobran: Number(raw.sobran || 0),
      ausentismos: Number(raw.ausentismos || 0)
    };
  }

  function updateSortIndicators(selector, key, dir) {
    ui.querySelectorAll(selector).forEach((th) => {
      const base = th.dataset.baseLabel || th.textContent.replace(/\s[\^v\u25B2\u25BC]$/, '');
      th.dataset.baseLabel = base;
      const thKey = th.getAttribute('data-sort') || th.getAttribute('data-sort-detail');
      th.textContent = key === thKey ? `${base} ${dir === 1 ? '\u25B2' : '\u25BC'}` : base;
    });
  }

  function applyFilters() {
    const term = String(qs('#txtSearch', ui).value || '').trim().toLowerCase();
    const fltDate = String(qs('#fltDate', ui).value || '').trim();
    const filtered = snapshot.filter((r) => {
      const blob = `${r.fecha || ''} ${r.confirmedBy || ''}`.toLowerCase();
      const matchesTerm = !term || blob.includes(term);
      const matchesDate = !fltDate || String(r.fecha || '') === fltDate;
      return matchesTerm && matchesDate;
    });
    const sorted = [...filtered].sort((a, b) => {
      const va = sortVal(a, sortKey);
      const vb = sortVal(b, sortKey);
      if (va === vb) return 0;
      return va > vb ? sortDir : -sortDir;
    });
    return { filtered: sorted, count: filtered.length };
  }

  function attendanceKey(item = {}) {
    return [
      String(item?.fecha || '').trim(),
      String(item?.employeeId || item?.empleadoId || '').trim(),
      String(item?.documento || '').trim()
    ].join('|');
  }

  function statusDetailState(row) {
    const estadoDia = String(row?.estadoDia || '').trim();
    const decision = String(row?.decisionCobertura || '').trim().toLowerCase();
    const tipoPersonal = String(row?.tipoPersonal || 'empleado').trim().toLowerCase();
    const reemplazaA = row?.reemplazaANombre || row?.reemplazaADocumento || '-';
    const reemplazadoPor = row?.reemplazadoPorNombre || row?.reemplazadoPorDocumento || '-';

    if (tipoPersonal === 'supernumerario' && estadoDia === 'trabajado_reemplazo') {
      return `Supernumerario reemplazando a ${reemplazaA}`;
    }
    if (decision === 'reemplazo') {
      return `Reemplazado por ${reemplazadoPor}`;
    }
    if (row?.asistio === true) {
      return tipoPersonal === 'supernumerario' ? 'Trabajo supernumerario' : 'Trabajo';
    }
    if (decision === 'ausentismo' || row?.cuentaPagoServicio === false) {
      return 'Ausentismo';
    }
    if (estadoDia === 'sin_registro') {
      return 'Sin registro';
    }
    if (estadoDia === 'incapacidad') {
      return 'Incapacidad';
    }
    if (estadoDia === 'vacaciones') {
      return 'Vacaciones';
    }
    if (estadoDia === 'compensatorio') {
      return 'Compensatorio';
    }
    if (estadoDia === 'ausente_con_novedad') {
      return 'Ausente con novedad';
    }
    if (estadoDia === 'ausente_sin_reemplazo') {
      return 'Ausentismo';
    }
    if (estadoDia === 'no_programado') {
      return 'No programado';
    }
    return estadoDia
      ? estadoDia.replace(/_/g, ' ').replace(/\b\w/g, (ch) => ch.toUpperCase())
      : '-';
  }

  async function showDetail(row) {
    const date = String(row?.fecha || '').trim();
    if (!date) return;
    qs('#detailTitle', ui).textContent = `Detalle del dia ${date}`;
    qs('#msg', ui).textContent = 'Cargando detalle...';
    try {
      const [statusRows, attendance] = await Promise.all([
        deps.listEmployeeDailyStatusRange?.(date, date) || [],
        deps.listAttendanceRange?.(date, date) || []
      ]);

      const attendanceByKey = new Map();
      (attendance || []).forEach((item) => {
        attendanceByKey.set(attendanceKey(item), item);
      });

      detailSnapshot = (statusRows || [])
        .slice()
        .sort((a, b) => {
          const hourA = String(attendanceByKey.get(attendanceKey(a))?.hora || '');
          const hourB = String(attendanceByKey.get(attendanceKey(b))?.hora || '');
          const byHour = hourB.localeCompare(hourA);
          if (byHour !== 0) return byHour;
          const bySede = String(a?.sedeNombreSnapshot || a?.sedeCodigo || '').localeCompare(String(b?.sedeNombreSnapshot || b?.sedeCodigo || ''));
          if (bySede !== 0) return bySede;
          return String(a?.nombre || '').localeCompare(String(b?.nombre || ''));
        })
        .map((statusRow) => {
          const attendanceRow = attendanceByKey.get(attendanceKey(statusRow)) || null;
          return {
            fecha: statusRow.fecha || date,
            hora: attendanceRow?.hora || '-',
            sede: statusRow.sedeNombreSnapshot || statusRow.sedeCodigo || '-',
            documento: statusRow.documento || '-',
            nombre: statusRow.nombre || '-',
            novedad: statusRow.novedadNombre || statusRow.novedadCodigo || '-',
            estado: statusDetailState(statusRow)
          };
        });
      renderDetail();
      qs('#msg', ui).textContent = 'Consulta OK';
    } catch (err) {
      qs('#msg', ui).textContent = `Error cargando detalle: ${err?.message || err}`;
    }
  }

  function render() {
    const { filtered, count } = applyFilters();
    tbody.replaceChildren(...filtered.map((r) => {
      const tr = el('tr', {}, []);
      const btn = el('button', { className: 'btn', type: 'button' }, ['Ver']);
      btn.addEventListener('click', () => showDetail(r));
      tr.append(
        el('td', {}, [r.fecha || '-']),
        el('td', {}, [r.confirmedBy || '-']),
        el('td', {}, [String(r.planeados)]),
        el('td', {}, [String(r.contratados)]),
        el('td', {}, [String(r.registrados)]),
        el('td', {}, [String(r.faltan)]),
        el('td', {}, [String(r.sobran)]),
        el('td', {}, [String(r.ausentismos)]),
        el('td', {}, [btn])
      );
      return tr;
    }));
    qs('#msg', ui).textContent = `Total cierres filtrados: ${count}`;
    updateSortIndicators('th[data-sort]', sortKey, sortDir);
  }

  function detailSortVal(r, key) {
    if (key === 'fecha') return String(r.fecha || '');
    if (key === 'hora') return String(r.hora || '');
    if (key === 'sede') return String(r.sede || '').toLowerCase();
    if (key === 'documento') return String(r.documento || '');
    if (key === 'nombre') return String(r.nombre || '').toLowerCase();
    if (key === 'novedad') return String(r.novedad || '').toLowerCase();
    if (key === 'estado') return String(r.estado || '').toLowerCase();
    return '';
  }

  function renderDetail() {
    const rows = [...detailSnapshot].sort((a, b) => {
      const va = detailSortVal(a, detailSortKey);
      const vb = detailSortVal(b, detailSortKey);
      if (va === vb) return 0;
      return va > vb ? detailSortDir : -detailSortDir;
    });
    const tb = qs('#tblDetail tbody', ui);
    tb.replaceChildren(...rows.map((r) => el('tr', {}, [
      el('td', {}, [r.fecha]),
      el('td', {}, [r.hora]),
      el('td', {}, [r.sede]),
      el('td', {}, [r.documento]),
      el('td', {}, [r.nombre]),
      el('td', {}, [r.novedad]),
      el('td', {}, [r.estado])
    ])));
    updateSortIndicators('#tblDetail th[data-sort-detail]', detailSortKey, detailSortDir);
  }

  qs('#txtSearch', ui).addEventListener('input', render);
  qs('#fltDate', ui).addEventListener('change', render);
  qs('#btnClear', ui).addEventListener('click', () => {
    qs('#txtSearch', ui).value = '';
    qs('#fltDate', ui).value = '';
    render();
  });
  ui.querySelectorAll('th[data-sort]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-sort');
      if (sortKey === key) sortDir *= -1; else { sortKey = key; sortDir = 1; }
      render();
    });
  });
  ui.querySelectorAll('#tblDetail th[data-sort-detail]').forEach((th) => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-sort-detail');
      if (detailSortKey === key) detailSortDir *= -1;
      else { detailSortKey = key; detailSortDir = 1; }
      renderDetail();
    });
  });

  mount.replaceChildren(ui);
  const unClosures = deps.streamDailyClosures?.((arr) => {
    snapshot = (arr || [])
      .filter((r) => r && (r.locked === true || String(r.status || '').trim() === 'closed'))
      .map(toRow);
    render();
  });

  if (!deps.streamDailyClosures) {
    qs('#msg', ui).textContent = 'No hay conexion para historial de cierres.';
  }

  return () => {
    unClosures?.();
  };
};


