import { el } from '../utils/dom.js';
import { getState } from '../state.js';

export const Home = async (mount, deps = {}) => {
  const profile = getState()?.userProfile || {};
  const displayName = String(profile?.displayName || profile?.email || 'usuario').trim();
  const role = String(profile?.role || 'sin rol').trim();
  const today = todayBogota();
  const monthStart = monthStartBogota(today);

  const hero = el('div', { className: 'home-hero' }, [
    el('div', { className: 'home-hero__content' }, [
      el('h2', { className: 'home-hero__title' }, ['Centro de control operativo']),
      el('p', { className: 'home-hero__lead' }, [
        `Hola, ${displayName}.`
      ]),
      el('div', { className: 'home-hero__meta' }, [
        heroPill('Rol activo', role),
        heroPill('Fecha de corte', today),
        heroPill('Plataforma', 'RockyEDU')
      ])
    ])
  ]);

  const summaryToggle = sectionToggle('Dashboard operativo');
  const summaryBlock = el('div', { className: 'section-block' }, [
    summaryToggle,
    el('div', { className: 'section-content' }, [
      el('div', { className: 'home-summary__top' }, [
        el('div', {}, [
          el('p', { className: 'text-muted', style: 'margin:0;' }, ['Sedes activas, planeacion base y contratacion institucional.'])
        ]),
        el('p', { id: 'homeSummaryNote', className: 'text-muted', style: 'margin:0;' }, ['Cargando datos...'])
      ]),
      el('div', { id: 'homeSummaryCards', className: 'home-summary__cards mt-2' }, [
        metricCard('Sedes activas', '...', 'green'),
        metricCard('Planeados', '...', 'blue'),
        metricCard('Contratados', '...', 'indigo'),
        metricCard('Sobran', '...', 'lime'),
        metricCard('Faltan', '...', 'red')
      ])
    ])
  ]);

  const chartToggle = sectionToggle('Tendencia mensual');
  const chartBlock = el('div', { className: 'section-block' }, [
    chartToggle,
    el('div', { className: 'section-content' }, [
      el('div', { className: 'home-chart__top' }, [
        el('div', {}, [
          el('p', { className: 'text-muted', style: 'margin:0;' }, [`Del ${monthStart} al ${today}.`])
        ])
      ]),
      el('div', { id: 'homeChartLegend', className: 'home-chart__legend mt-2' }, []),
      el('div', { id: 'homeChartMount', className: 'home-chart__mount mt-2' }, [
        el('p', { className: 'text-muted' }, ['Preparando informacion del mes actual...'])
      ])
    ])
  ]);

  const ui = el('section', { className: 'main-card home-shell' }, [hero, summaryBlock, chartBlock]);
  mount.replaceChildren(ui);
  bindSectionToggle(summaryBlock, summaryToggle);
  bindSectionToggle(chartBlock, chartToggle);

  try {
    const [sedes, employees, metrics] = await Promise.all([
      streamOnce((ok, fail) => deps.streamSedes?.(ok, fail)),
      streamOnce((ok, fail) => (deps.streamActiveBaseEmployees || deps.streamEmployees)?.(ok, fail)),
      deps.listDailyMetricsRange?.(monthStart, today) || []
    ]);

    const summary = computeOperationalSummary(sedes, employees, today);
    renderSummary(summaryBlock, summary);

    const chartData = buildMonthlySeries(monthStart, today, metrics || []);
    renderMonthlyChart(chartBlock, chartData, monthStart, today);
  } catch (error) {
    renderSummary(summaryBlock, emptySummary());
    renderChartError(chartBlock, error);
  }

  return () => {};
};

function heroPill(label, value) {
  return el('span', { className: 'home-hero__pill' }, [
    el('span', { className: 'home-hero__pill-label' }, [`${label}:`]),
    el('span', { className: 'home-hero__pill-value' }, [String(value || '-')])
  ]);
}

function sectionToggle(title) {
  return el('button', {
    className: 'section-title section-title--toggle home-section-toggle',
    type: 'button',
    'aria-expanded': 'true'
  }, [title]);
}

function bindSectionToggle(block, toggle) {
  toggle.addEventListener('click', () => {
    const collapsed = block.classList.toggle('is-collapsed');
    toggle.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  });
}

function metricCard(label, value, tone = 'blue') {
  return el('div', { className: `metric-tile metric-tile--${tone}` }, [
    el('span', { className: 'metric-tile__label' }, [label]),
    el('strong', { className: 'metric-tile__value' }, [String(value || '0')])
  ]);
}

function todayBogota() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Bogota' }).format(new Date());
}

function monthStartBogota(today) {
  const [year, month] = String(today || todayBogota()).split('-');
  return `${year}-${month}-01`;
}

function streamOnce(factory, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let unsub = () => {};
    const finish = (cb) => (value) => {
      if (settled) return;
      settled = true;
      try { unsub?.(); } catch {}
      cb(value);
    };
    try {
      unsub = factory(
        finish((rows) => resolve(Array.isArray(rows) ? rows : [])),
        finish((error) => reject(error instanceof Error ? error : new Error(String(error || 'Error al consultar datos.'))))
      ) || (() => {});
    } catch (error) {
      reject(error instanceof Error ? error : new Error(String(error || 'Error al consultar datos.')));
      return;
    }
    setTimeout(() => {
      if (settled) return;
      settled = true;
      try { unsub?.(); } catch {}
      reject(new Error('Tiempo de espera agotado al consultar el dashboard.'));
    }, timeoutMs);
  });
}

function toISODate(value) {
  if (!value) return '';
  try {
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
    const date = value?.toDate ? value.toDate() : new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return date.toISOString().slice(0, 10);
  } catch {
    return '';
  }
}

function parseOperatorCount(value) {
  if (value == null) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : 0;
  const digits = String(value).replace(/[^\d]/g, '');
  if (!digits) return 0;
  const parsed = Number(digits);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isCurrentEmployee(row = {}, todayISO) {
  const estado = String(row?.estado || 'activo').trim().toLowerCase();
  const ingreso = toISODate(row?.fechaIngreso || row?.fecha_ingreso);
  const retiro = toISODate(row?.fechaRetiro || row?.fecha_retiro);
  if (ingreso && ingreso > todayISO) return false;
  if (estado === 'eliminado') return false;
  if (estado === 'inactivo') return Boolean(retiro && retiro >= todayISO);
  if (retiro && retiro < todayISO) return false;
  return true;
}

function computeOperationalSummary(sedes = [], employees = [], todayISO) {
  const activeSedes = (sedes || []).filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
  const activeSedeCodes = new Set(activeSedes.map((row) => String(row?.codigo || '').trim()).filter(Boolean));
  const plannedBySede = new Map();
  activeSedes.forEach((row) => {
    const sedeCode = String(row?.codigo || '').trim();
    if (!sedeCode) return;
    plannedBySede.set(sedeCode, parseOperatorCount(row?.numeroOperarios ?? row?.numero_operarios));
  });

  const contractedBySede = new Map();
  (employees || []).forEach((row) => {
    if (!isCurrentEmployee(row, todayISO)) return;
    const sedeCode = String(row?.sedeCodigo || row?.sede_codigo || '').trim();
    if (!sedeCode || !activeSedeCodes.has(sedeCode)) return;
    contractedBySede.set(sedeCode, Number(contractedBySede.get(sedeCode) || 0) + 1);
  });

  const planned = Array.from(plannedBySede.values()).reduce((acc, value) => acc + Number(value || 0), 0);
  const contracted = Array.from(contractedBySede.values()).reduce((acc, value) => acc + Number(value || 0), 0);
  let surplus = 0;
  let missing = 0;

  plannedBySede.forEach((plannedCount, sedeCode) => {
    const contractedCount = Number(contractedBySede.get(sedeCode) || 0);
    surplus += Math.max(contractedCount - plannedCount, 0);
    missing += Math.max(plannedCount - contractedCount, 0);
  });

  return {
    activeSedes: activeSedes.length,
    planned,
    contracted,
    surplus,
    missing
  };
}

function emptySummary() {
  return { activeSedes: 0, planned: 0, contracted: 0, surplus: 0, missing: 0 };
}

function renderSummary(block, summary) {
  const cardsMount = block.querySelector('#homeSummaryCards');
  const note = block.querySelector('#homeSummaryNote');
  cardsMount.replaceChildren(
    metricCard('Sedes activas', summary.activeSedes, 'green'),
    metricCard('Planeados', summary.planned, 'blue'),
    metricCard('Contratados', summary.contracted, 'indigo'),
    metricCard('Sobran', summary.surplus, 'lime'),
    metricCard('Faltan', summary.missing, 'red')
  );
  note.textContent = `Planeacion institucional: ${summary.planned} cupos base y ${summary.contracted} contratados activos sin supernumerarios.`;
}

function buildMonthlySeries(from, to, metricsRows = []) {
  const rowsByDate = new Map((metricsRows || []).map((row) => [String(row?.fecha || '').trim(), row]));
  const days = listDaysInRange(from, to);
  const rows = days.map((day) => {
    const row = rowsByDate.get(day) || {};
    const planned = Number(row?.planned || 0);
    const expected = Number(row?.expected || 0);
    const attendance = Number(row?.attendanceCount || 0);
    const absenteeism = Number(row?.absenteeism || 0);
    const noContracted = Number(row?.noContracted || 0);
    const stackedTotal = attendance + absenteeism + noContracted;
    const surplus = Math.max(expected - planned, 0);
    const shortfall = Math.max(planned - stackedTotal, 0);
    const overflow = Math.max(stackedTotal - planned, 0);
    return {
      day,
      planned,
      expected,
      attendance,
      absenteeism,
      noContracted,
      stackedTotal,
      surplus,
      shortfall,
      overflow,
      closed: row?.closed === true
    };
  });
  const maxValue = Math.max(1, ...rows.flatMap((row) => [row.planned, row.stackedTotal]));
  const mismatches = rows.filter((row) => row.planned !== row.stackedTotal).length;
  const surplusDays = rows.filter((row) => row.surplus > 0).length;
  const legend = [
    { label: 'Asistencias', color: '#2563eb', kind: 'fill' },
    { label: 'Ausentismos', color: '#ea580c', kind: 'fill' },
    { label: 'No contratados', color: '#7c3aed', kind: 'fill' },
    { label: 'Planeados', color: '#0f172a', kind: 'line' }
  ];
  return { rows, maxValue, mismatches, surplusDays, legend };
}

function listDaysInRange(from, to) {
  const out = [];
  let current = String(from || '').trim();
  while (current && current <= to) {
    out.push(current);
    current = addOneDay(current);
  }
  return out;
}

function addOneDay(value) {
  const [year, month, day] = String(value || '').split('-').map(Number);
  const date = new Date(Date.UTC(year, (month || 1) - 1, day || 1));
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

function renderMonthlyChart(block, chartData, from, to) {
  const legend = block.querySelector('#homeChartLegend');
  const mount = block.querySelector('#homeChartMount');
  const rows = chartData?.rows || [];
  const maxValue = Math.max(1, Number(chartData?.maxValue || 0));

  legend.replaceChildren(...(chartData?.legend || []).map((item) => legendChip(item.label, item.color, item.kind)));

  if (!rows.length) {
    mount.replaceChildren(el('p', { className: 'text-muted' }, ['Sin datos para el mes actual.']));
    return;
  }

  const width = Math.max(760, rows.length * 34);
  const height = 340;
  const margin = { top: 24, right: 20, bottom: 52, left: 52 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const slotWidth = plotWidth / Math.max(rows.length, 1);
  const barWidth = Math.max(12, Math.min(22, slotWidth * 0.58));

  const xFor = (index) => margin.left + slotWidth * index + (slotWidth - barWidth) / 2;
  const yFor = (value) => margin.top + plotHeight - (plotHeight * value) / maxValue;
  const gridValues = Array.from(new Set([0, .25, .5, .75, 1].map((p) => Math.round(maxValue * p)))).sort((a, b) => a - b);

  const svg = [
    `<svg viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="Grafico mensual de planeados frente a coberturas operativas diarias">`
  ];

  gridValues.forEach((value) => {
    const y = yFor(value);
    svg.push(`<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="#d9e3ed" stroke-dasharray="4 4" />`);
    svg.push(`<text x="${margin.left - 10}" y="${y + 4}" text-anchor="end" font-size="11" fill="#64748b">${value}</text>`);
  });

  rows.forEach((row, index) => {
    const x = xFor(index);
    const dayLabel = row.day.slice(-2);
    const hitX = Math.max(margin.left, x - Math.max((slotWidth - barWidth) / 2, 4));
    const hitWidth = Math.min(slotWidth, width - margin.right - hitX);
    svg.push(`<g class="home-chart-day" data-day="${row.day}" data-planned="${row.planned}" data-expected="${row.expected}" data-attendance="${row.attendance}" data-absenteeism="${row.absenteeism}" data-no-contracted="${row.noContracted}" data-total="${row.stackedTotal}" data-surplus="${row.surplus}" data-closed="${row.closed ? 'true' : 'false'}" style="cursor:pointer;">`);
    svg.push(`<rect x="${hitX}" y="${margin.top}" width="${hitWidth}" height="${plotHeight}" fill="transparent" />`);
    if (index < rows.length - 1) {
      svg.push(`<line x1="${x + barWidth / 2}" y1="${margin.top}" x2="${x + barWidth / 2}" y2="${height - margin.bottom}" stroke="#f4f7fa" />`);
    }

    const segments = [
      { value: row.attendance, color: '#2563eb' },
      { value: row.absenteeism, color: '#ea580c' },
      { value: row.noContracted, color: '#7c3aed' }
    ];
    let accumulated = 0;
    segments.forEach((segment) => {
      if (!segment.value) return;
      const y = yFor(accumulated + segment.value);
      const nextY = yFor(accumulated);
      const rectHeight = Math.max(nextY - y, 0);
      svg.push(`<rect x="${x}" y="${y}" width="${barWidth}" height="${rectHeight}" rx="3" fill="${segment.color}"></rect>`);
      accumulated += segment.value;
    });

    const plannedY = yFor(row.planned);
    svg.push(`<line x1="${x - 2}" y1="${plannedY}" x2="${x + barWidth + 2}" y2="${plannedY}" stroke="#0f172a" stroke-width="2.5" stroke-linecap="round" />`);
    if (row.planned !== row.stackedTotal) {
      const markerColor = row.stackedTotal > row.planned ? '#2f9a47' : '#b91c1c';
      svg.push(`<circle cx="${x + barWidth / 2}" cy="${plannedY}" r="3.2" fill="${markerColor}" />`);
    }
    svg.push(`<text x="${x + barWidth / 2}" y="${height - 18}" text-anchor="middle" font-size="10" fill="#64748b">${dayLabel}</text>`);
    svg.push('</g>');
  });

  svg.push('</svg>');

  const chartShell = el('div', { className: 'home-chart__canvas-shell' }, []);
  const detailBox = el('div', { className: 'home-chart__detail mt-2' }, []);
  mount.replaceChildren(chartShell, detailBox);
  chartShell.innerHTML = `${svg.join('')}<div id="homeChartTooltip" style="position:absolute; display:none; pointer-events:none; z-index:4; max-width:280px; background:#0f172a; color:#fff; border-radius:12px; padding:.65rem .8rem; box-shadow:0 18px 40px rgba(15,23,42,.24); font-size:.82rem; line-height:1.35;"></div>`;

  const tooltip = chartShell.querySelector('#homeChartTooltip');
  const dayNodes = Array.from(chartShell.querySelectorAll('.home-chart-day'));
  const defaultData = rows[rows.length - 1] || rows[0] || null;
  let pinnedDay = null;

  const renderIntoDetailBox = (data, prefix = 'Ultimo dia cargado') => {
    if (!data) {
      detailBox.textContent = 'Pasa el mouse por un dia del grafico para ver el detalle.';
      return;
    }
    detailBox.innerHTML = chartDayDetailHtml(data, prefix);
  };

  const showTooltip = (data, clientX, clientY) => {
    if (!data || !tooltip) return;
    tooltip.innerHTML = chartDayDetailHtml(data, 'Detalle del dia');
    tooltip.style.display = 'block';
    positionChartTooltip(chartShell, tooltip, clientX, clientY);
  };

  const restoreDetail = () => {
    if (pinnedDay) {
      renderIntoDetailBox(pinnedDay, 'Dia fijado');
      return;
    }
    renderIntoDetailBox(defaultData, 'Ultimo dia cargado');
  };

  restoreDetail();

  dayNodes.forEach((node) => {
    const data = chartDayDataFromNode(node);
    node.addEventListener('mouseenter', (event) => {
      renderIntoDetailBox(data, 'Detalle del dia');
      showTooltip(data, event.clientX, event.clientY);
    });
    node.addEventListener('mousemove', (event) => {
      showTooltip(data, event.clientX, event.clientY);
    });
    node.addEventListener('mouseleave', () => {
      if (tooltip) tooltip.style.display = 'none';
      restoreDetail();
    });
    node.addEventListener('click', (event) => {
      const isSameDay = pinnedDay?.day === data.day;
      pinnedDay = isSameDay ? null : data;
      renderIntoDetailBox(pinnedDay || data, pinnedDay ? 'Dia fijado' : 'Detalle del dia');
      showTooltip(data, event.clientX, event.clientY);
    });
  });
}

function chartDayDataFromNode(node) {
  const data = node?.dataset || {};
  return {
    day: String(data.day || ''),
    planned: Number(data.planned || 0),
    expected: Number(data.expected || 0),
    attendance: Number(data.attendance || 0),
    absenteeism: Number(data.absenteeism || 0),
    noContracted: Number(data.noContracted || 0),
    surplus: Number(data.surplus || 0),
    stackedTotal: Number(data.total || 0),
    closed: String(data.closed || '').trim() === 'true'
  };
}

function chartDayDetailHtml(data, prefix = 'Detalle del dia') {
  const difference = Number(data?.planned || 0) - Number(data?.stackedTotal || 0);
  const balance = difference === 0 ? 'Cierra exacto' : difference > 0 ? `Faltan ${difference}` : `Sobran ${Math.abs(difference)}`;
  const contractedBase = Number(data?.expected || 0);
  const surplus = Number(data?.surplus || 0);
  const closureLabel = data?.closed ? 'Dia cerrado' : 'Dia abierto';
  const extraLine = surplus > 0
    ? `<span style="display:block; opacity:.82; margin-bottom:.35rem;">Contratados base ${contractedBase}. Sobran ${surplus} frente a la planeacion.</span>`
    : `<span style="display:block; opacity:.82; margin-bottom:.35rem;">Contratados base ${contractedBase}. ${closureLabel}.</span>`;
  return [
    `<strong style="display:block; font-size:.92rem; margin-bottom:.18rem;">${prefix}: ${data?.day || '-'}</strong>`,
    `<span style="display:block; opacity:.82; margin-bottom:.35rem;">Planeados ${Number(data?.planned || 0)}. Cobertura diaria ${Number(data?.attendance || 0)} asistencias + ${Number(data?.absenteeism || 0)} ausentismos + ${Number(data?.noContracted || 0)} no contratados = ${Number(data?.stackedTotal || 0)}</span>`,
    extraLine,
    `<span style="display:block; opacity:.82;">Balance vs planeacion: ${balance}</span>`
  ].join('');
}

function positionChartTooltip(container, tooltip, clientX, clientY) {
  if (!container || !tooltip) return;
  const containerRect = container.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  const desiredLeft = clientX - containerRect.left + 14;
  const desiredTop = clientY - containerRect.top - tooltipRect.height - 14;
  const maxLeft = Math.max(containerRect.width - tooltipRect.width - 8, 8);
  const left = Math.min(Math.max(desiredLeft, 8), maxLeft);
  const top = desiredTop < 8 ? Math.min(clientY - containerRect.top + 18, Math.max(containerRect.height - tooltipRect.height - 8, 8)) : desiredTop;
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function legendChip(label, color, kind = 'fill') {
  const swatchStyle = kind === 'line'
    ? `width:18px; height:0; border-top:3px solid ${color}; display:inline-block; border-radius:999px;`
    : `width:10px; height:10px; border-radius:999px; background:${color}; display:inline-block;`;
  return el('span', { className: 'legend-chip' }, [
    el('span', { style: swatchStyle }, []),
    label
  ]);
}

function renderChartError(block, error) {
  const mount = block.querySelector('#homeChartMount');
  const legend = block.querySelector('#homeChartLegend');
  legend.replaceChildren();
  mount.replaceChildren(el('p', { className: 'text-muted' }, [`Error: ${error?.message || error}`]));
}
