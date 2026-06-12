import { el, qs } from './dom.js';

const DEFAULT_PAGE_SIZE = 50;
const PAGE_SIZES = [25, 50, 100, 200];

export function createTablePagination(scope, {
  id = 'table',
  after = null,
  defaultPageSize = DEFAULT_PAGE_SIZE,
  onChange = null
} = {}) {
  const safeId = String(id || 'table').replace(/[^a-zA-Z0-9_-]/g, '');
  const controls = el('div', {
    id: `${safeId}Pagination`,
    className: 'mt-2',
    style: 'display:flex;justify-content:space-between;gap:.75rem;align-items:center;flex-wrap:wrap;'
  }, [
    el('div', { id: `${safeId}PageSummary`, className: 'text-muted', style: 'font-size:.86rem;' }, ['Mostrando 0 de 0']),
    el('div', { style: 'display:flex;align-items:center;gap:.5rem;flex-wrap:wrap;' }, [
      el('label', { className: 'text-muted', for: `${safeId}PageSize`, style: 'font-size:.86rem;' }, ['Filas por pagina']),
      el('select', { id: `${safeId}PageSize`, className: 'input wa-input', style: 'width:auto;min-width:88px;' }, PAGE_SIZES.map((size) => (
        el('option', { value: String(size), selected: size === defaultPageSize }, [String(size)])
      ))),
      el('button', { id: `${safeId}ToggleAll`, className: 'btn', type: 'button' }, ['Ver todos']),
      el('button', { id: `${safeId}PrevPage`, className: 'btn', type: 'button' }, ['Anterior']),
      el('span', { id: `${safeId}PageIndicator`, className: 'text-muted', style: 'font-size:.86rem;min-width:96px;text-align:center;' }, ['Pagina 0 de 0']),
      el('button', { id: `${safeId}NextPage`, className: 'btn', type: 'button' }, ['Siguiente'])
    ])
  ]);

  const anchor = after ? qs(after, scope) : null;
  if (anchor?.insertAdjacentElement) anchor.insertAdjacentElement('afterend', controls);
  else scope.append(controls);

  const summary = qs(`#${safeId}PageSummary`, controls);
  const pageSizeSelect = qs(`#${safeId}PageSize`, controls);
  const toggleAll = qs(`#${safeId}ToggleAll`, controls);
  const prevPage = qs(`#${safeId}PrevPage`, controls);
  const nextPage = qs(`#${safeId}NextPage`, controls);
  const indicator = qs(`#${safeId}PageIndicator`, controls);
  const state = {
    currentPage: 1,
    pageSize: Number(defaultPageSize) || DEFAULT_PAGE_SIZE,
    showAll: false
  };

  const emitChange = () => {
    if (typeof onChange === 'function') onChange();
  };

  pageSizeSelect?.addEventListener('change', () => {
    state.pageSize = Number(pageSizeSelect.value || DEFAULT_PAGE_SIZE) || DEFAULT_PAGE_SIZE;
    state.currentPage = 1;
    emitChange();
  });
  toggleAll?.addEventListener('click', () => {
    state.showAll = !state.showAll;
    state.currentPage = 1;
    emitChange();
  });
  prevPage?.addEventListener('click', () => {
    if (state.currentPage <= 1) return;
    state.currentPage -= 1;
    emitChange();
  });
  nextPage?.addEventListener('click', () => {
    state.currentPage += 1;
    emitChange();
  });

  const update = (totalRows, visibleCount = 0, startIndex = 0, totalPages = 0) => {
    const visibleFrom = totalRows ? startIndex + 1 : 0;
    const visibleTo = totalRows ? startIndex + visibleCount : 0;
    if (summary) summary.textContent = totalRows ? `Mostrando ${visibleFrom}-${visibleTo} de ${totalRows}` : 'Mostrando 0 de 0';
    if (indicator) indicator.textContent = state.showAll ? 'Todos los registros' : (totalRows ? `Pagina ${state.currentPage} de ${totalPages}` : 'Pagina 0 de 0');
    if (pageSizeSelect) pageSizeSelect.value = String(state.pageSize);
    if (pageSizeSelect) pageSizeSelect.disabled = state.showAll;
    if (toggleAll) toggleAll.textContent = state.showAll ? 'Usar paginas' : 'Ver todos';
    if (prevPage) prevPage.disabled = state.showAll || totalRows === 0 || state.currentPage <= 1;
    if (nextPage) nextPage.disabled = state.showAll || totalRows === 0 || state.currentPage >= totalPages;
  };

  const slice = (rows = []) => {
    const totalRows = rows.length;
    const effectivePageSize = state.showAll ? Math.max(totalRows, 1) : state.pageSize;
    const totalPages = Math.max(1, Math.ceil(totalRows / effectivePageSize));
    state.currentPage = Math.min(Math.max(state.currentPage, 1), totalPages);
    const startIndex = totalRows ? (state.currentPage - 1) * effectivePageSize : 0;
    const pageRows = rows.slice(startIndex, startIndex + effectivePageSize);
    update(totalRows, pageRows.length, startIndex, totalPages);
    return pageRows;
  };

  return {
    controls,
    reset() { state.currentPage = 1; },
    slice,
    state
  };
}
