const routes = new Map();
let renderCurrentRoute = () => {};
let currentCleanup = null;

export const addRoute = (path, renderFn) => routes.set(path, renderFn);
export const navigate = (path) => {
  window.location.hash = path.startsWith('#') ? path : `#${path}`;
};

const getPath = () => (window.location.hash || '#/login').replace('#', '').split('?')[0];

export const refreshRoute = () => {
  renderCurrentRoute();
};

export const startRouter = () => {
  const render = () => {
    if (typeof currentCleanup === 'function') {
      try { currentCleanup(); } catch {}
      currentCleanup = null;
    }
    const path = getPath();
    const view = routes.get(path) || routes.get('/login');
    const maybeCleanup = view?.();
    if (typeof maybeCleanup === 'function') currentCleanup = maybeCleanup;
  };
  renderCurrentRoute = render;
  window.addEventListener('hashchange', render);
  render();
};
