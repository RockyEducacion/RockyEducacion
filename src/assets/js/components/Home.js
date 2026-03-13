import { el, qs } from '../utils/dom.js';

export const Home = async (mount, deps = {}) => {
  const ui = el('section', { className: 'main-card' }, [
    el('div', { className: 'section-block', style: heroStyle() }, [
      el('div', {}, [
        el('p', { className: 'text-muted', style: 'margin:0 0 .35rem 0; letter-spacing:.08em; text-transform:uppercase;' }, ['Dashboard estable']),
        el('h2', { style: 'margin:0; font-size:2rem; line-height:1.05;' }, ['Panel operativo consolidado']),
        el('p', { className: 'mt-1', style: 'max-width:760px; margin-bottom:0;' }, [
          'Los dias cerrados usan cifras congeladas desde ',
          el('strong', {}, ['daily_closures']),
          '. Solo el dia actual, si sigue abierto, usa lectura en vivo desde ',
          el('strong', {}, ['daily_metrics']),
          '.'
        ])
      ]),
      el('div', { className: 'form-row mt-2', style: 'align-items:end;' }, [
        el('div', {}, [el('label', { className: 'label' }, ['Mes']), el('input', { id: 'monthPick', className: 'input', type: 'month' })]),
        el('button', { id: 'btnLoad', className: 'btn btn--primary', type: 'button' }, ['Actualizar']),
        el('span', { id: 'msg', className: 'text-muted' }, [' '])
      ])
    ]),
    el('div', { className: 'perms-grid mt-2', style: 'grid-template-columns:repeat(auto-fit,minmax(180px,1fr));' }, [
      statCard('Dias cerrados', 'kClosedDays', '#0f766e'),
      statCard('Dia actual', 'kTodayStatus', '#1d4ed8'),
      statCard('Planeados mes', 'kPlanned', '#111827'),
      statCard('Contratados mes', 'kExpected', '#4f46e5'),
      statCard('Ausentismos mes', 'kAbsenteeism', '#b91c1c'),
      statCard('Pagados mes', 'kPaid', '#0369a1'),
      statCard('No contratados mes', 'kNotContracted', '#b45309'),
      statCard('Asistencias mes', 'kAttendance', '#166534')
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title', style: 'margin-bottom:.35rem;' }, ['Serie diaria consolidada']),
      el('p', { className: 'text-muted', style: 'margin-top:0;' }, ['Historico cerrado + dia actual abierto en vivo.']),
      el('div', { style: 'min-height:340px;' }, [el('canvas', { id: 'chartStableOps' })])
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('div', { className: 'form-row', style: 'align-items:center;' }, [
        el('h3', { className: 'section-title', style: 'margin:0;' }, ['Detalle diario']),
        el('span', { id: 'summaryBadge', className: 'text-muted' }, [' '])
      ]),
      el('div', { className: 'mt-1 table-wrap' }, [
        el('table', { className: 'table' }, [
          el('thead', {}, [el('tr', {}, [
            el('th', {}, ['Fecha']),
            el('th', {}, ['Fuente']),
            el('th', {}, ['Planeados']),
            el('th', {}, ['Contratados']),
            el('th', {}, ['Ausentismos']),
            el('th', {}, ['Pagados']),
            el('th', {}, ['No contratados']),
            el('th', {}, ['Asistencias'])
          ])]),
          el('tbody', { id: 'dashboardRows' })
        ])
      ])
    ])
  ]);

  mount.replaceChildren(ui);

  const msg = qs('#msg', ui);
  const monthPick = qs('#monthPick', ui);
  const btnLoad = qs('#btnLoad', ui);
  const tbody = qs('#dashboardRows', ui);
  const summaryBadge = qs('#summaryBadge', ui);

  let chart = null;
  let ChartMod = null;
  let unMetric = null;
  let unClosures = null;
  let activeModel = null;
  let activeMonth = '';

  monthPick.value = await getDefaultMonth();
  btnLoad.addEventListener('click', () => loadMonth(monthPick.value));
  await loadMonth(monthPick.value);

  return () => {
    cleanupSubscriptions();
    if (chart) chart.destroy();
  };

  async function getDefaultMonth() {
    const today = todayBogota();
    const from = shiftDay(today, -62);
    try {
      const [closures, metrics] = await Promise.all([
        typeof deps.listDailyClosuresRange === 'function' ? deps.listDailyClosuresRange(from, today) : [],
        typeof deps.listDailyMetricsRange === 'function' ? deps.listDailyMetricsRange(from, today) : []
      ]);
      const dates = [
        ...(closures || []).map((row) => String(row?.fecha || '').trim()),
        ...(metrics || []).map((row) => String(row?.fecha || '').trim())
      ]
        .filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value))
        .sort();
      return String(dates[dates.length - 1] || today).slice(0, 7);
    } catch {
      return today.slice(0, 7);
    }
  }

  async function loadMonth(month) {
    if (!/^\d{4}-\d{2}$/.test(String(month || '').trim())) {
      msg.textContent = 'Selecciona un mes valido.';
      return;
    }

    activeMonth = month;
    cleanupSubscriptions();
    msg.textContent = 'Consultando metricas consolidadas...';

    try {
      activeModel = await fetchMonthModel(month);
      renderModel(activeModel);
      attachLiveSubscriptions(activeModel);
    } catch (error) {
      activeModel = null;
      tbody.replaceChildren();
      summaryBadge.textContent = ' ';
      msg.textContent = `Error: ${error?.message || error}`;
    }
  }

  async function fetchMonthModel(month) {
    const today = todayBogota();
    const { from, to } = monthRange(month);
    const visibleTo = minIsoDate(to, today);
    const days = from <= visibleTo ? eachDay(from, visibleTo) : [];
    const [metricsRows, closureRows] = await Promise.all([
      typeof deps.listDailyMetricsRange === 'function' ? deps.listDailyMetricsRange(from, visibleTo) : [],
      typeof deps.listDailyClosuresRange === 'function' ? deps.listDailyClosuresRange(from, visibleTo) : []
    ]);

    return {
      month,
      today,
      days,
      metricsByDay: new Map((metricsRows || []).map((row) => [String(row.fecha || '').trim(), row])),
      closuresByDay: new Map((closureRows || []).map((row) => [String(row.fecha || '').trim(), row]))
    };
  }

  function attachLiveSubscriptions(model) {
    if (!model || !model.days.length) return;
    if (!activeMonth.startsWith(model.today.slice(0, 7))) return;

    if (typeof deps.streamDailyMetricsByDate === 'function') {
      unMetric = deps.streamDailyMetricsByDate(model.today, (row) => {
        if (!activeModel || activeModel.month !== model.month) return;
        if (row) activeModel.metricsByDay.set(model.today, row);
        else activeModel.metricsByDay.delete(model.today);
        renderModel(activeModel);
      });
    }

    if (typeof deps.streamDailyClosures === 'function') {
      unClosures = deps.streamDailyClosures((rows) => {
        if (!activeModel || activeModel.month !== model.month) return;
        const updated = new Map(activeModel.closuresByDay);
        const monthPrefix = `${model.month}-`;
        Array.isArray(rows) && rows.forEach((row) => {
          const day = String(row?.fecha || '').trim();
          if (!day.startsWith(monthPrefix)) return;
          updated.set(day, row);
        });
        activeModel.closuresByDay = updated;
        renderModel(activeModel);
      });
    }
  }

  function cleanupSubscriptions() {
    try { unMetric?.(); } catch {}
    try { unClosures?.(); } catch {}
    unMetric = null;
    unClosures = null;
  }

  function renderModel(model) {
    const rows = buildDashboardRows(model);
    const summary = summarizeRows(rows, model.today);

    setText('#kClosedDays', String(summary.closedDays));
    setText('#kTodayStatus', summary.todayStatus);
    setText('#kPlanned', formatNumber(summary.planned));
    setText('#kExpected', formatNumber(summary.expected));
    setText('#kAbsenteeism', formatNumber(summary.absenteeism));
    setText('#kPaid', formatNumber(summary.paid));
    setText('#kNotContracted', formatNumber(summary.notContracted));
    setText('#kAttendance', formatNumber(summary.attendance));

    summaryBadge.textContent = summary.pendingPastDays > 0
      ? `${summary.pendingPastDays} dia(s) historicos sin cierre quedaron fuera del consolidado`
      : 'Historico totalmente consolidado';

    tbody.replaceChildren(...rows.slice().reverse().map((row) => renderTableRow(row)));
    renderChart(rows, model.month).catch((error) => {
      console.error('No se pudo renderizar el dashboard:', error);
      msg.textContent = `Error al renderizar grafico: ${error?.message || error}`;
    });

    msg.textContent = summary.pendingPastDays > 0
      ? `Dashboard actualizado. Fuente: cierres diarios + dia actual en vivo. Pendientes historicos: ${summary.pendingPastDays}.`
      : 'Dashboard actualizado. Fuente estable: cierres diarios + dia actual en vivo si sigue abierto.';
  }

  async function renderChart(rows, month) {
    if (!ChartMod) {
      ChartMod = await import('https://cdn.jsdelivr.net/npm/chart.js@4.4.1/+esm');
      ChartMod.Chart.register(...ChartMod.registerables);
    }
    const canvas = qs('#chartStableOps', ui);
    if (chart) chart.destroy();
    const series = rows.filter((row) => row.includeInTotals);
    const { Chart } = ChartMod;
    chart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: series.map((row) => row.fecha.slice(8, 10)),
        datasets: [
          {
            label: `Pagados (${month})`,
            data: series.map((row) => row.pagados),
            backgroundColor: '#0f766e',
            borderRadius: 6,
            stack: 'totales'
          },
          {
            label: `Ausentismos (${month})`,
            data: series.map((row) => row.ausentismos),
            backgroundColor: '#dc2626',
            borderRadius: 6,
            stack: 'totales'
          },
          {
            label: `No contratados (${month})`,
            data: series.map((row) => row.noContratados),
            backgroundColor: '#f59e0b',
            borderRadius: 6,
            stack: 'totales'
          },
          {
            type: 'line',
            label: `Planeados (${month})`,
            data: series.map((row) => row.planeados),
            borderColor: '#1d4ed8',
            backgroundColor: '#1d4ed8',
            borderWidth: 2,
            pointRadius: 2,
            pointHoverRadius: 4,
            tension: 0.25
          },
          {
            type: 'line',
            label: `Contratados (${month})`,
            data: series.map((row) => row.contratados),
            borderColor: '#7c3aed',
            backgroundColor: '#7c3aed',
            borderWidth: 2,
            pointRadius: 2,
            pointHoverRadius: 4,
            tension: 0.25
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        plugins: {
          legend: { position: 'bottom' }
        },
        scales: {
          x: { stacked: true },
          y: { beginAtZero: true, stacked: true, ticks: { precision: 0 } }
        }
      }
    });
  }

  function setText(selector, value) {
    const target = qs(selector, ui);
    if (target) target.textContent = value;
  }
};

function buildDashboardRows(model) {
  return (model?.days || []).map((day) => {
    const closure = model?.closuresByDay?.get(day) || null;
    const metric = model?.metricsByDay?.get(day) || null;
    const isToday = day === model?.today;

    if (isClosedDay(closure)) {
      return {
        fecha: day,
        planeados: Number(closure?.planeados || 0),
        contratados: Number(closure?.contratados || 0),
        ausentismos: Number(closure?.ausentismos || 0),
        pagados: Number(closure?.pagados || 0),
        noContratados: Number(closure?.noContratados || 0),
        attendance: Number(metric?.attendanceCount || 0),
        source: 'closed',
        sourceLabel: 'Cerrado',
        includeInTotals: true
      };
    }

    if (isToday && metric) {
      return {
        fecha: day,
        planeados: Number(metric?.planned || 0),
        contratados: Number(metric?.expected || 0),
        ausentismos: Number(metric?.absenteeism || 0),
        pagados: Number(metric?.paidServices || 0),
        noContratados: Number(metric?.noContracted || 0),
        attendance: Number(metric?.attendanceCount || 0),
        source: 'live',
        sourceLabel: 'Hoy abierto',
        includeInTotals: true
      };
    }

    return {
      fecha: day,
      planeados: 0,
      contratados: 0,
      ausentismos: 0,
      pagados: 0,
      noContratados: 0,
      attendance: 0,
      source: isToday ? 'today_no_data' : 'pending',
      sourceLabel: isToday ? 'Hoy sin datos' : 'Sin cierre',
      includeInTotals: false
    };
  });
}

function summarizeRows(rows, today) {
  const included = (rows || []).filter((row) => row.includeInTotals);
  const todayRow = (rows || []).find((row) => row.fecha === today) || null;
  const pendingPastDays = (rows || []).filter((row) => row.fecha < today && row.source === 'pending').length;
  return {
    closedDays: (rows || []).filter((row) => row.source === 'closed').length,
    todayStatus: todayRow?.source === 'closed' ? 'Cerrado' : todayRow?.source === 'live' ? 'Abierto' : 'Sin datos',
    planned: sumRows(included, 'planeados'),
    expected: sumRows(included, 'contratados'),
    absenteeism: sumRows(included, 'ausentismos'),
    paid: sumRows(included, 'pagados'),
    notContracted: sumRows(included, 'noContratados'),
    attendance: sumRows(included, 'attendance'),
    pendingPastDays
  };
}

function renderTableRow(row) {
  return el('tr', { 'data-day': row.fecha }, [
    el('td', {}, [formatDateLabel(row.fecha)]),
    el('td', {}, [sourceBadge(row)]),
    el('td', {}, [formatNumber(row.planeados)]),
    el('td', {}, [formatNumber(row.contratados)]),
    el('td', {}, [formatNumber(row.ausentismos)]),
    el('td', {}, [formatNumber(row.pagados)]),
    el('td', {}, [formatNumber(row.noContratados)]),
    el('td', {}, [formatNumber(row.attendance)])
  ]);
}

function sourceBadge(row) {
  const palette = {
    closed: ['#dcfce7', '#166534'],
    live: ['#dbeafe', '#1d4ed8'],
    today_no_data: ['#e5e7eb', '#374151'],
    pending: ['#fef3c7', '#b45309']
  };
  const [bg, fg] = palette[row?.source] || palette.pending;
  return el('span', {
    style: `display:inline-flex;align-items:center;gap:.35rem;padding:.2rem .55rem;border-radius:999px;background:${bg};color:${fg};font-weight:700;font-size:.8rem;`
  }, [row?.sourceLabel || '-']);
}

function statCard(label, id, accent) {
  return el('div', {
    className: 'perm-item',
    style: `border-top:4px solid ${accent}; min-height:108px; display:flex; align-items:center;`
  }, [
    el('div', {}, [
      el('div', { className: 'text-muted', style: 'font-size:.82rem; text-transform:uppercase; letter-spacing:.04em;' }, [label]),
      el('div', { id, style: 'font-size:1.7rem;font-weight:800;line-height:1.1;margin-top:.2rem;' }, ['0'])
    ])
  ]);
}

function heroStyle() {
  return [
    'background:linear-gradient(135deg,#f8fafc 0%,#e0f2fe 52%,#ecfccb 100%)',
    'border:1px solid #dbeafe',
    'padding:1.25rem'
  ].join(';');
}

function isClosedDay(row) {
  if (!row) return false;
  return row.locked === true || String(row.status || '').trim().toLowerCase() === 'closed';
}

function sumRows(rows, key) {
  return (rows || []).reduce((sum, row) => sum + Number(row?.[key] || 0), 0);
}

function formatNumber(value) {
  return new Intl.NumberFormat('es-CO').format(Number(value || 0));
}

function formatDateLabel(value) {
  const iso = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return iso || '-';
  const date = new Date(`${iso}T00:00:00Z`);
  return new Intl.DateTimeFormat('es-CO', {
    timeZone: 'America/Bogota',
    month: 'short',
    day: '2-digit'
  }).format(date);
}

function monthRange(month) {
  const [year, monthNumber] = String(month).split('-').map(Number);
  const first = new Date(Date.UTC(year, (monthNumber || 1) - 1, 1));
  const last = new Date(Date.UTC(year, monthNumber || 1, 0));
  return { from: toIso(first), to: toIso(last) };
}

function eachDay(from, to) {
  const out = [];
  const start = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  for (let day = new Date(start); day <= end; day.setUTCDate(day.getUTCDate() + 1)) {
    out.push(toIso(day));
  }
  return out;
}

function toIso(date) {
  return date.toISOString().slice(0, 10);
}

function todayBogota() {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Bogota',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  return fmt.format(new Date());
}

function shiftDay(day, delta) {
  const base = new Date(`${day}T00:00:00Z`);
  base.setUTCDate(base.getUTCDate() + Number(delta || 0));
  return toIso(base);
}

function minIsoDate(left, right) {
  return left <= right ? left : right;
}
