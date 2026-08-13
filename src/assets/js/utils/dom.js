export const qs = (sel, scope = document) => scope.querySelector(sel);
export const qsa = (sel, scope = document) => Array.from(scope.querySelectorAll(sel));

export const el = (tag, props = {}, children = []) => {
  const node = document.createElement(tag);

  for (const [key, value] of Object.entries(props || {})) {
    if (value === undefined || value === null) continue;

    if (key === 'className') {
      node.className = String(value);
      continue;
    }

    if (key === 'style' && typeof value === 'string') {
      node.setAttribute('style', value);
      continue;
    }

    if (key === 'dataset' && typeof value === 'object') {
      for (const [dk, dv] of Object.entries(value)) {
        if (dv === undefined || dv === null) continue;
        node.dataset[dk] = String(dv);
      }
      continue;
    }

    if (key in node) {
      try {
        node[key] = value;
        continue;
      } catch (_) {
        // Some properties are readonly (e.g. input.list); fallback to attribute.
      }
    }

    if (typeof value === 'boolean') {
      if (value) node.setAttribute(key, '');
      else node.removeAttribute(key);
      continue;
    }

    node.setAttribute(key, String(value));
  }

  for (const child of [].concat(children)) {
    node.append(child?.nodeType ? child : document.createTextNode(child ?? ''));
  }

  return node;
};

export const lucideInlineIcon = (iconName = 'circle', fallback = '', className = 'app-inline-icon') => {
  const node = el('span', { className: `${className} app-inline-icon`, 'aria-hidden': 'true' }, [
    el('span', { className: 'app-inline-icon__fallback' }, [fallback || '']),
    el('i', { className: 'app-inline-icon__svg', 'data-lucide': iconName || 'circle' }, [])
  ]);
  hydrateLucideIcons(node);
  return node;
};

export const infoIcon = () => lucideInlineIcon('info', 'i', 'app-info-icon');
export const viewIcon = () => lucideInlineIcon('eye', 'Ver', 'app-view-icon');
export const moreIcon = () => lucideInlineIcon('ellipsis', '...', 'app-more-icon');
export const editIcon = () => lucideInlineIcon('pencil', 'Ed', 'app-edit-icon');
export const activateIcon = () => lucideInlineIcon('rotate-ccw', 'Ac', 'app-activate-icon');
export const deactivateIcon = () => lucideInlineIcon('power', 'De', 'app-deactivate-icon');
export const cancelIcon = () => lucideInlineIcon('x', 'X', 'app-cancel-icon');

export const hydrateLucideIcons = (scope = document, attempt = 0) => {
  requestAnimationFrame(() => {
    if (globalThis.lucide?.createIcons) {
      globalThis.lucide.createIcons({
        attrs: {
          'stroke-width': 2,
          width: 18,
          height: 18
        }
      });
      inlineIconNodes(scope).forEach((icon) => {
        icon.classList.toggle('has-lucide-svg', Boolean(icon.querySelector('svg')));
      });
      return;
    }
    if (attempt < 8) setTimeout(() => hydrateLucideIcons(scope, attempt + 1), 120);
  });
};

function inlineIconNodes(scope = document) {
  const nodes = [];
  if (scope?.nodeType === 1 && scope.matches?.('.app-inline-icon')) nodes.push(scope);
  nodes.push(...qsa('.app-inline-icon', scope));
  return nodes;
}

export const enableSectionToggles = (scope = document) => {
  qsa('.section-block', scope).forEach((section) => {
    if (section.dataset.collapsibleInit === '1') return;
    const title = qs('.section-title', section);
    if (!title) return;

    const contentNodes = Array.from(section.children).filter((child) => child !== title);
    const content = document.createElement('div');
    content.className = 'section-content';
    contentNodes.forEach((node) => content.appendChild(node));
    section.appendChild(content);

    const setCollapsed = (collapsed) => {
      section.classList.toggle('is-collapsed', collapsed);
      title.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    };

    title.classList.add('section-title--toggle');
    title.setAttribute('role', 'button');
    title.setAttribute('tabindex', '0');
    title.setAttribute('aria-expanded', 'true');

    const onToggle = () => setCollapsed(!section.classList.contains('is-collapsed'));
    title.addEventListener('click', onToggle);
    title.addEventListener('keydown', (ev) => {
      if (ev.key !== 'Enter' && ev.key !== ' ') return;
      ev.preventDefault();
      onToggle();
    });

    section.dataset.collapsibleInit = '1';
  });
};
