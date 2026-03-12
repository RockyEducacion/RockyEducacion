import { el, qs } from '../utils/dom.js';

export const Home = async (mount, deps = {}) => {
  const ui = el('section', { className: 'main-card' }, [
    el('h2', {}, ['Dashboard de operacion']),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [el('label', { className: 'label' }, ['Mes']), el('input', { id: 'monthPick', className: 'input', type: 'month' })]),
      el('button', { id: 'btnLoad', className: 'btn btn--primary', type: 'button' }, ['Actualizar']),
      el('span', { id: 'msg', className: 'text-muted' }, [' '])
    ]),
    el('div', { className: 'perms-grid mt-2' }, [
      statCard('Servicios planeados', 'kPlanned'),
      statCard('No contratados', 'kNotContracted'),
      statCard('Ausentismo', 'kAbsenteeism'),
      statCard('Servicios pagados', 'kPaid')
    ]),
    el('div', { className: 'section-block mt-2' }, [
      el('h3', { className: 'section-title' }, ['Servicios contratados por dia']),
      el('div', { style: 'min-height:320px;' }, [el('canvas', { id: 'chartPaid' })])
    ])
  ]);

  mount.replaceChildren(ui);

  const msg = qs('#msg', ui);
  const monthPick = qs('#monthPick', ui);
  const btnLoad = qs('#btnLoad', ui);
  let chart = null;
  let ChartMod = null;

  monthPick.value = await getDefaultMonth();
  btnLoad.addEventListener('click', () => loadMonth(monthPick.value));
  await loadMonth(monthPick.value);

  return () => {
    if (chart) chart.destroy();
  };

  async function getDefaultMonth() {
    const latest = await getLatestDashboardDate();
    return String(latest || todayBogota()).slice(0, 7);
  }

  async function getLatestDashboardDate() {
    try {
      const today = todayBogota();
      const from = shiftDay(today, -62);
      const rows = typeof deps.listAttendanceRange === 'function' ? await deps.listAttendanceRange(from, today) : [];
      const dates = (rows || [])
        .map((row) => String(row?.fecha || '').trim())
        .filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value))
        .sort();
      return dates[dates.length - 1] || today;
    } catch {
      return todayBogota();
    }
  }

  async function loadMonth(month) {
    if (!/^\d{4}-\d{2}$/.test(String(month || '').trim())) {
      msg.textContent = 'Selecciona un mes valido.';
      return;
    }

    const { from, to } = monthRange(month);
    const visibleTo = minIsoDate(to, todayBogota());
    const days = from <= visibleTo ? eachDay(from, visibleTo) : [];
    msg.textContent = 'Consultando metricas mensuales...';

    try {
      const rows = await buildMonthRows(days, from, visibleTo);
      const refDay = visibleTo || todayBogota();
      const ref = rows.find((row) => row.fecha === refDay) || emptyRow(refDay);

      qs('#kPlanned', ui).textContent = String(ref.planeados || 0);
      qs('#kNotContracted', ui).textContent = String(ref.noContratados || 0);
      qs('#kAbsenteeism', ui).textContent = String(ref.ausentismos || 0);
      qs('#kPaid', ui).textContent = String(ref.pagados || 0);

      await renderChart(rows, month);
      msg.textContent = `Dashboard actualizado. Dia de referencia: ${refDay || '-'}.`;
    } catch (error) {
      msg.textContent = `Error: ${error?.message || error}`;
    }
  }

  async function buildMonthRows(days, from, to) {
    const [attendanceRows, replacementRows, sedes, employees, supernumerarios, novedades] = await Promise.all([
      typeof deps.listAttendanceRange === 'function' ? deps.listAttendanceRange(from, to) : [],
      typeof deps.listImportReplacementsRange === 'function' ? deps.listImportReplacementsRange(from, to) : [],
      snapshotFromStream(deps.streamSedes),
      snapshotFromStream(deps.streamEmployees),
      snapshotFromStream(deps.streamSupernumerarios),
      snapshotFromStream(deps.streamNovedades)
    ]);

    const sedesRows = (sedes || []).filter((row) => String(row?.estado || 'activo').trim().toLowerCase() !== 'inactivo');
    const employeeRows = Array.isArray(employees) ? employees : [];
    const superRows = Array.isArray(supernumerarios) ? supernumerarios : [];
    const noveltyRows = Array.isArray(novedades) ? novedades : [];

    const attendanceByDay = groupByDay(attendanceRows);
    const replacementsByDay = groupByDay(replacementRows);

    return days.map((day) => {
      const attendance = attendanceByDay.get(day) || [];
      const replacementMap = new Map((replacementsByDay.get(day) || []).map((row) => [replacementRowKey(row), row]));

      const planeados = sedesRows.reduce((sum, sede) => {
        if (!isSedeScheduledForDate(sede, day)) return sum;
        return sum + parseOperatorCount(sede?.numeroOperarios);
      }, 0);

      const esperados = employeeRows.filter((emp) => {
        if (!isEmployeeActiveForDate(emp, day, sedesRows)) return false;
        return !isSupernumerarioEmployee(emp, superRows, day);
      }).length;

      const ausentismos = attendance.filter((row) => {
        if (!requiresReplacement(row, noveltyRows)) return false;
        const replacement = replacementMap.get(replacementRowKey(row)) || null;
        const decision = String(replacement?.decision || '').trim().toLowerCase();
        return !replacement || decision === 'ausentismo';
      }).length;

      const pagados = attendance.filter((row) => isPaidNovelty(row)).length;

      return {
        fecha: day,
        planeados,
        contratados: esperados,
        noContratados: Math.max(0, planeados - esperados),
        ausentismos,
        pagados
      };
    });
  }

  async function renderChart(rows, month) {
    if (!ChartMod) {
      ChartMod = await import('https://cdn.jsdelivr.net/npm/chart.js@4.4.1/+esm');
      ChartMod.Chart.register(...ChartMod.registerables);
    }
    const canvas = qs('#chartPaid', ui);
    if (chart) chart.destroy();
    const { Chart } = ChartMod;
    chart = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: rows.map((row) => row.fecha.slice(8, 10)),
        datasets: [
          { label: `Pagados (${month})`, data: rows.map((row) => row.pagados), backgroundColor: '#0ea5e9', stack: 'totales' },
          { label: `Ausentismo (${month})`, data: rows.map((row) => row.ausentismos), backgroundColor: '#ef4444', stack: 'totales' },
          { label: `No contratados (${month})`, data: rows.map((row) => row.noContratados), backgroundColor: '#f59e0b', stack: 'totales' },
          {
            type: 'line',
            label: `Planeados (${month})`,
            data: rows.map((row) => row.planeados),
            borderColor: '#1e3a8a',
            borderWidth: 2,
            borderDash: [6, 4],
            pointRadius: 2,
            pointHoverRadius: 3,
            fill: false,
            tension: 0.2
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { stacked: true },
          y: { beginAtZero: true, ticks: { precision: 0 }, stacked: true }
        }
      }
    });
  }
};

function statCard(label, id) {
  return el('div', { className: 'perm-item' }, [
    el('div', {}, [
      el('div', { className: 'text-muted' }, [label]),
      el('div', { id, style: 'font-size:1.45rem;font-weight:700;line-height:1.2;' }, ['0'])
    ])
  ]);
}

function emptyRow(fecha) {
  return { fecha, planeados: 0, contratados: 0, noContratados: 0, ausentismos: 0, pagados: 0 };
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

function groupByDay(rows) {
  const map = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const day = String(row?.fecha || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return;
    if (!map.has(day)) map.set(day, []);
    map.get(day).push(row || {});
  });
  return map;
}

function snapshotFromStream(streamFn) {
  if (typeof streamFn !== 'function') return Promise.resolve([]);
  return new Promise((resolve) => {
    let done = false;
    let unsubscribe = null;
    const finish = (rows) => {
      if (done) return;
      done = true;
      try { unsubscribe?.(); } catch {}
      resolve(Array.isArray(rows) ? rows : []);
    };
    try {
      unsubscribe = streamFn((rows) => finish(rows));
      setTimeout(() => finish([]), 4000);
    } catch {
      finish([]);
    }
  });
}

function replacementRowKey(row = {}) {
  return `${String(row?.fecha || '').trim()}_${String(row?.empleadoId || row?.employeeId || '').trim()}`;
}

function attendanceNovedadCode(row = {}) {
  const explicit = String(row?.novedadCodigo || '').trim();
  if (explicit) return explicit;
  const raw = String(row?.novedad || '').trim();
  return /^\d+$/.test(raw) ? raw : '';
}

function normalize(text) {
  return String(text || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function baseNovedadName(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return '';
  const noParens = raw.replace(/\s*\(.*\)\s*$/, '').trim();
  if (/^OTRA\s+SEDE\s*:/i.test(noParens)) return 'OTRA SEDE';
  return noParens;
}

function noveltyNeedsReplacement(row = {}, novedades = []) {
  const code = attendanceNovedadCode(row);
  if (['2', '3', '4', '5', '8'].includes(code)) return true;
  if (['1', '7'].includes(code)) return false;

  const base = normalize(baseNovedadName(row?.novedadNombre || row?.novedad || code));
  if (!base) return false;
  if (base.includes('incapacidad')) return true;
  if (base.includes('accidente laboral')) return true;
  if (base.includes('calamidad')) return true;
  if (base.includes('permiso no remunerado')) return true;
  if (base.includes('compensatorio')) return false;

  const novelty = (novedades || []).find((item) => {
    const name = normalize(item?.nombre);
    const codeValue = normalize(item?.codigoNovedad || item?.codigo || '');
    return (name && (name === base || name.includes(base) || base.includes(name))) || (codeValue && codeValue === base);
  }) || null;
  if (!novelty) return false;
  const replacementFlag = normalize(novelty?.reemplazo);
  return ['si', 'yes', 'true', '1', 'reemplazo'].includes(replacementFlag);
}

function requiresReplacement(row = {}, novedades = []) {
  return noveltyNeedsReplacement(row, novedades);
}

function isPaidNovelty(row = {}) {
  return ['1', '7'].includes(attendanceNovedadCode(row));
}

function isSedeScheduledForDate(sede, selectedDate) {
  const iso = toISODate(selectedDate);
  if (!iso || !sede) return false;
  const [year, month, day] = iso.split('-').map(Number);
  const weekday = new Date(Date.UTC(year, (month || 1) - 1, day || 1)).getUTCDay();
  const jornada = String(sede?.jornada || 'lun_vie').trim().toLowerCase();
  if (jornada === 'lun_dom') return true;
  if (jornada === 'lun_sab') return weekday >= 1 && weekday <= 6;
  return weekday >= 1 && weekday <= 5;
}

function parseOperatorCount(value) {
  if (value == null) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : 0;
  const digits = String(value).replace(/[^\d]/g, '');
  if (!digits) return 0;
  const parsed = Number(digits);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isEmployeeActiveForDate(emp, selectedDate, sedes = []) {
  const iso = toISODate(selectedDate);
  if (!iso) return false;
  const ingreso = toISODate(emp?.fechaIngreso);
  if (ingreso && ingreso > iso) return false;
  const retiro = toISODate(emp?.fechaRetiro);
  if (String(emp?.estado || '').trim().toLowerCase() === 'inactivo') {
    return Boolean(retiro && retiro >= iso);
  }
  if (retiro && retiro < iso) return false;
  const sedeCodigo = String(emp?.sedeCodigo || emp?.sede_codigo || '').trim();
  if (!sedeCodigo) return false;
  const sede = (sedes || []).find((row) => String(row?.codigo || '').trim() === sedeCodigo) || null;
  return isSedeScheduledForDate(sede, iso);
}

function isSupernumerarioEmployee(emp = {}, supernumerarios = [], selectedDate = '') {
  const doc = String(emp?.documento || '').trim();
  if (doc) {
    return (supernumerarios || []).some((row) => isPersonActiveForDate(row, selectedDate) && String(row?.documento || '').trim() === doc);
  }
  const id = String(emp?.id || '').trim();
  if (!id) return false;
  return (supernumerarios || []).some((row) => isPersonActiveForDate(row, selectedDate) && String(row?.id || '').trim() === id);
}

function isPersonActiveForDate(person, selectedDate) {
  const iso = toISODate(selectedDate);
  if (!iso) return false;
  if (String(person?.estado || '').trim().toLowerCase() === 'inactivo') return false;
  const ingreso = toISODate(person?.fechaIngreso);
  if (!ingreso || ingreso > iso) return false;
  const retiro = toISODate(person?.fechaRetiro);
  if (retiro && retiro < iso) return false;
  return true;
}

function toISODate(value) {
  if (!value) return null;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
    const date = new Date(trimmed);
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
  }
  if (typeof value?.toDate === 'function') {
    const date = value.toDate();
    return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
  }
  return null;
}
