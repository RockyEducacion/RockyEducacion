import { el, lucideInlineIcon } from '../utils/dom.js';
import { navigate } from '../router.js';
import { getState, setState } from '../state.js';
import { isMobileSidebarOpen, toggleMobileSidebar } from './Sidebar.js';

const THEMES = ['light', 'dark', 'color'];
const THEME_META = {
  light: { icon: 'sun', fallback: '*', title: 'Tema claro' },
  dark: { icon: 'moon', fallback: '◐', title: 'Tema oscuro' },
  color: { icon: 'palette', fallback: '◆', title: 'Tema color' }
};

export const Header=(deps={})=>{
  const { user, theme }=getState();
  const themeBtn = el('button',{className:'btn header-btn header-theme-btn',type:'button'},[]);
  const mobileMenuBtn = user ? el('button',{
    className:'btn header-btn header-mobile-toggle',
    type:'button',
    title:'Abrir menu',
    'aria-label':'Abrir menu lateral',
    'aria-controls':'app-sidebar'
  },['☰']) : null;
  const syncThemeBtn = (currentTheme) => {
    const normalizedTheme = THEMES.includes(currentTheme) ? currentTheme : 'light';
    const next = nextTheme(normalizedTheme);
    const nextMeta = THEME_META[next];
    themeBtn.replaceChildren(lucideInlineIcon(nextMeta.icon, nextMeta.fallback, 'app-theme-icon'));
    themeBtn.title = `Cambiar a ${nextMeta.title.toLowerCase()}`;
    themeBtn.setAttribute('aria-label', themeBtn.title);
  };
  syncThemeBtn(theme);
  themeBtn.addEventListener('click', () => {
    const next = nextTheme(getState().theme);
    document.documentElement.setAttribute('data-theme', next);
    setState({ theme: next });
    syncThemeBtn(next);
  });
  const syncMobileMenuBtn = () => {
    if (!mobileMenuBtn) return;
    const open = isMobileSidebarOpen();
    mobileMenuBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
    mobileMenuBtn.textContent = open ? '✕' : '☰';
    mobileMenuBtn.title = open ? 'Cerrar menu' : 'Abrir menu';
    mobileMenuBtn.setAttribute('aria-label', mobileMenuBtn.title);
  };
  syncMobileMenuBtn();
  mobileMenuBtn?.addEventListener('click', () => {
    toggleMobileSidebar();
    syncMobileMenuBtn();
  });

  const nav=el('nav',{className:'header-nav container'},[
    mobileMenuBtn,
    el('a',{href:'#',className:'header-nav__brand',title:'Empresa','aria-label':'Empresa'},[
      el('span',{className:'header-nav__brand-mark'},[
        el('img',{className:'header-nav__brand-logo',src:'src/assets/img/tercero.png',alt:'Logo Capcol',loading:'lazy'})
      ]),
      el('span',{className:'header-nav__brand-copy'},[
        el('span',{className:'header-nav__brand-kicker'},['SERVILIMPIEZA S.A.'])
      ])
    ]),
    el('div',{className:'header-nav__menu'},[
      navLink('Inicio','/',()=> navigate('/')),
      navLink('Contacto','/contact',()=> navigate('/contact')),
      navLink('Tratamiento Datos','/data-treatment',()=> navigate('/data-treatment')),
      navLink('Acerca','/about',()=> navigate('/about'))
    ]),
    el('div',{className:'header-nav__actions'},[
      themeBtn,
      user
        ? el('button',{className:'btn header-btn',onclick:async()=>{ await deps.logout?.(); navigate('/login'); }},['Cerrar sesion'])
        : el('button',{className:'btn btn--primary header-btn',onclick:()=> navigate('/login')},['Iniciar sesion'])
    ])
  ]);
  return el('div',{className:'header'},[nav]);
};

function nextTheme(theme) {
  const currentIndex = THEMES.indexOf(theme);
  return THEMES[(currentIndex >= 0 ? currentIndex + 1 : 1) % THEMES.length];
}

function navLink(text,to,onClick){
  const a=el('a',{href:`#${to}`,className:'header-nav__link'},[text]);
  a.addEventListener('click',(e)=>{ e.preventDefault(); onClick?.(); });
  return a;
}
