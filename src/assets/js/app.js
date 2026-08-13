import { Header } from './components/Header.js';
import { Footer } from './components/Footer.js';
import { Sidebar } from './components/Sidebar.js';

import { Home } from './components/Home.js';
import { Contact } from './components/Contact.js';
import { DataTreatment } from './components/DataTreatment.js';
import { About } from './components/About.js';
import { ForgotPassword, Login, ResetPassword } from './components/Login.js';
import { Notes } from './components/Notes.js';
import { AdministracionDashboard } from './components/dashboards/AdministracionDashboard.js';
import { CargueMasivoDashboard } from './components/dashboards/CargueMasivoDashboard.js';
import { EmpleadosDashboard } from './components/dashboards/EmpleadosDashboard.js';
import { GobiernoDashboard } from './components/dashboards/GobiernoDashboard.js';
import { OperacionDashboard } from './components/dashboards/OperacionDashboard.js';
import { ReportesDashboard } from './components/dashboards/ReportesDashboard.js';

import { UsersAdmin } from './components/UsersAdmin.js';
import { ZonesAdmin } from './components/ZonesAdmin.js';
import { DependenciesAdmin } from './components/DependenciesAdmin.js';
import { SedesAdmin } from './components/SedesAdmin.js';
import { EmployeesAdmin } from './components/EmployeesAdmin.js';
import { EmployeeNovelties } from './components/EmployeeNovelties.js';
import { SupernumerariosAdmin } from './components/SupernumerariosAdmin.js';
import { SupervisorsAdmin } from './components/SupervisorsAdmin.js';
import { CargosAdmin } from './components/CargosAdmin.js';
import { NovedadesAdmin } from './components/NovedadesAdmin.js';
import { CargueMasivoAdmin } from './components/CargueMasivoAdmin.js';
import { CargueMasivoSedesAdmin } from './components/CargueMasivoSedesAdmin.js';
import { ImportHistory } from './components/ImportHistory.js';
import { Absenteeism } from './components/Absenteeism.js';
import { HistoricalDailyRegistry } from './components/HistoricalDailyRegistry.js';
import { HistoricalQrRegistry } from './components/HistoricalQrRegistry.js';
import { EmployeesReport } from './components/EmployeesReport.js';
import { HiringBySedeReport } from './components/HiringBySedeReport.js';
import { ConsolidatedNoveltiesReport } from './components/ConsolidatedNoveltiesReport.js';
import { ConsolidatedReports } from './components/ConsolidatedReports.js';
import { ImportReplacements } from './components/ImportReplacements.js';
import { CargarDatos } from './components/CargarDatos.js';
import { PermissionsCenter } from './components/PermissionsCenter.js';
import { PermissionsAudit } from './components/PermissionsAudit.js';
import { WhatsAppLive } from './components/WhatsAppLive.js';
import { RegistroSede } from './components/RegistroSede.js';
import { QrTabletScanner } from './components/QrTabletScanner.js';
import { QrDailyRegistry } from './components/QrDailyRegistry.js';
import { QrDevicesInfo } from './components/QrDevicesInfo.js';

import { addRoute, startRouter, navigate, refreshRoute } from './router.js';
import { getState, setState, subscribe } from './state.js';
import { can, PERMS, isSuperAdmin } from './permissions.js';
import { ROLES } from './roles.js';
import { installBrowserAlertReplacement } from './utils/notifications.js';
installBrowserAlertReplacement();
const sidebarMount=document.getElementById('app-sidebar');
const headerMount =document.getElementById('app-header');
const footerMount =document.getElementById('app-footer');
const root        =document.getElementById('app-root');

let deps={};
function renderSidebar(){
  const current=sidebarMount.firstElementChild;
  current?._cleanup?.();
  sidebarMount.replaceChildren(Sidebar(deps));
}
renderSidebar();
headerMount.replaceChildren(Header());
footerMount.replaceChildren(Footer());
subscribe('userProfile', () => footerMount.replaceChildren(Footer()));

let unsubRoleMatrix=null; let unsubUserOverrides=null; let unsubAudit=null;
let authSyncToken = 0;
let routerStarted = false;
const PUBLIC_AUTH_ROUTES = new Set(['/login', '/forgot-password', '/reset-password']);
const REPORT_VIEW_PERMISSIONS = [
  PERMS.VIEW_REPORTS_CLIENT,
  PERMS.VIEW_REPORTS_QR_HISTORY,
  PERMS.VIEW_REPORTS_ABSENTEEISM,
  PERMS.VIEW_REPORTS_EMPLOYEES,
  PERMS.VIEW_REPORTS_HIRING,
  PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED,
  PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED
];
const CONSOLIDATED_REPORT_VIEW_PERMISSIONS = [
  PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED,
  PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED
];
const guardWrite=(perm,fn)=> async (...args)=>{
  if(typeof fn!=='function') return undefined;
  if(!can(perm)) throw new Error('No tienes permiso de edicion para esta seccion.');
  return fn(...args);
};

(function init(){
  import('./supabase.js')
    .then((fb) => {
      deps={
        authState:fb.authState, login:fb.login, register:fb.register, logout:fb.logout,
        requestPasswordReset:fb.requestPasswordReset, updatePassword:fb.updatePassword,
        ensureUserProfile:fb.ensureUserProfile, loadUserProfile:fb.loadUserProfile, createUserProfile:fb.createUserProfile,
        addNote:fb.addNote, streamNotes:fb.streamNotes,
        streamRoleMatrix:fb.streamRoleMatrix, setRolePermissions:fb.setRolePermissions, streamUserOverrides:fb.streamUserOverrides,
        getUserOverrides:fb.getUserOverrides, setUserOverrides:fb.setUserOverrides, clearUserOverrides:fb.clearUserOverrides,
        addAuditLog:fb.addAuditLog, streamAuditLogs:(cb,max)=>{ if(unsubAudit)unsubAudit(); unsubAudit=fb.streamAuditLogs(cb,max); return unsubAudit; },
        streamUsers:fb.streamUsers, setUserRole:guardWrite(PERMS.EDIT_USERS,fb.setUserRole), syncSupervisorAccessForUser:guardWrite(PERMS.EDIT_USERS,fb.syncSupervisorAccessForUser), setUserStatus:guardWrite(PERMS.EDIT_USERS,fb.setUserStatus), findUserByEmail:fb.findUserByEmail,
        streamZones:fb.streamZones, createZone:guardWrite(PERMS.EDIT_ZONES,fb.createZone), updateZone:guardWrite(PERMS.EDIT_ZONES,fb.updateZone), setZoneStatus:guardWrite(PERMS.EDIT_ZONES,fb.setZoneStatus), findZoneByCode:fb.findZoneByCode, getNextZoneCode:fb.getNextZoneCode,
        streamDependencies:fb.streamDependencies, createDependency:guardWrite(PERMS.EDIT_DEPENDENCIES,fb.createDependency), updateDependency:guardWrite(PERMS.EDIT_DEPENDENCIES,fb.updateDependency), setDependencyStatus:guardWrite(PERMS.EDIT_DEPENDENCIES,fb.setDependencyStatus), findDependencyByCode:fb.findDependencyByCode, getNextDependencyCode:fb.getNextDependencyCode,
        streamSedes:fb.streamSedes, createSede:guardWrite(PERMS.EDIT_SEDES,fb.createSede), updateSede:guardWrite(PERMS.EDIT_SEDES,fb.updateSede), setSedeStatus:guardWrite(PERMS.EDIT_SEDES,fb.setSedeStatus), findSedeByCode:fb.findSedeByCode, getNextSedeCode:fb.getNextSedeCode,
        createQrDevice:guardWrite(PERMS.MANAGE_QR_DEVICES,fb.createQrDevice), setQrDeviceStatus:guardWrite(PERMS.MANAGE_QR_DEVICES,fb.setQrDeviceStatus), listQrDevices:fb.listQrDevices, streamQrDevices:fb.streamQrDevices, scanAttendanceQr:guardWrite(PERMS.USE_QR_SCANNER,fb.scanAttendanceQr), listDailyQrRecords:fb.listDailyQrRecords, streamDailyQrRecords:fb.streamDailyQrRecords,
        createSedesBulk:guardWrite(PERMS.BULK_UPLOAD_SEDES,fb.createSedesBulk),
        streamEmployees:fb.streamEmployees, streamActiveBaseEmployees:fb.streamActiveBaseEmployees, createEmployee:guardWrite(PERMS.EDIT_EMPLOYEES,fb.createEmployee), rehireEmployee:guardWrite(PERMS.EDIT_EMPLOYEES,fb.rehireEmployee), updateEmployee:guardWrite(PERMS.EDIT_EMPLOYEES,fb.updateEmployee), setEmployeeStatus:guardWrite(PERMS.EDIT_EMPLOYEES,fb.setEmployeeStatus), updateProgrammedEmployeeAssignment:guardWrite(PERMS.MANAGE_EMPLOYEE_SCHEDULES,fb.updateProgrammedEmployeeAssignment), cancelProgrammedEmployeeAssignment:guardWrite(PERMS.MANAGE_EMPLOYEE_SCHEDULES,fb.cancelProgrammedEmployeeAssignment), findEmployeeByCode:fb.findEmployeeByCode, findEmployeeByDocument:fb.findEmployeeByDocument, getNextEmployeeCode:fb.getNextEmployeeCode,
        generateEmployeeCertificate:fb.generateEmployeeCertificate,
        streamEmployeeCargoHistory:fb.streamEmployeeCargoHistory, streamEmployeeCargoHistoryAll:fb.streamEmployeeCargoHistoryAll,
        createEmployeesBulk:guardWrite(PERMS.BULK_UPLOAD_EMPLOYEES,fb.createEmployeesBulk),
        streamSupernumerarios:fb.streamSupernumerarios, listSupervisorAvailableSupernumerarios:fb.listSupervisorAvailableSupernumerarios, createSupernumerario:guardWrite(PERMS.EDIT_SUPERNUMERARIOS,fb.createSupernumerario), updateSupernumerario:guardWrite(PERMS.EDIT_SUPERNUMERARIOS,fb.updateSupernumerario), setSupernumerarioStatus:guardWrite(PERMS.EDIT_SUPERNUMERARIOS,fb.setSupernumerarioStatus), findSupernumerarioByCode:fb.findSupernumerarioByCode, findSupernumerarioByDocument:fb.findSupernumerarioByDocument, getNextSupernumerarioCode:fb.getNextSupernumerarioCode,
        streamCargos:fb.streamCargos, createCargo:guardWrite(PERMS.EDIT_CARGOS,fb.createCargo), updateCargo:guardWrite(PERMS.EDIT_CARGOS,fb.updateCargo), setCargoStatus:guardWrite(PERMS.EDIT_CARGOS,fb.setCargoStatus), findCargoByCode:fb.findCargoByCode, getNextCargoCode:fb.getNextCargoCode,
        streamNovedades:fb.streamNovedades, createNovedad:guardWrite(PERMS.EDIT_NOVEDADES,fb.createNovedad), updateNovedad:guardWrite(PERMS.EDIT_NOVEDADES,fb.updateNovedad), setNovedadStatus:guardWrite(PERMS.EDIT_NOVEDADES,fb.setNovedadStatus), findNovedadByCode:fb.findNovedadByCode, findNovedadByCodigoNovedad:fb.findNovedadByCodigoNovedad, getNextNovedadCode:fb.getNextNovedadCode,
        streamSupervisors:fb.streamSupervisors, createSupervisor:guardWrite(PERMS.EDIT_SUPERVISORS,fb.createSupervisor), updateSupervisor:guardWrite(PERMS.EDIT_SUPERVISORS,fb.updateSupervisor), setSupervisorStatus:guardWrite(PERMS.EDIT_SUPERVISORS,fb.setSupervisorStatus), findSupervisorByCode:fb.findSupervisorByCode, findSupervisorByDocument:fb.findSupervisorByDocument, getNextSupervisorCode:fb.getNextSupervisorCode,
        confirmImportOperation:guardWrite(PERMS.MANAGE_OPERATION_REGISTRY,fb.confirmImportOperation), saveImportReplacements:guardWrite(PERMS.MANAGE_OPERATION_REGISTRY,fb.saveImportReplacements),
        closeOperationDayManual:guardWrite(PERMS.MANAGE_OPERATION_REGISTRY,fb.closeOperationDayManual),
        isOperationDayClosed:fb.isOperationDayClosed, listClosedOperationDaysRange:fb.listClosedOperationDaysRange, listDailyClosuresRange:fb.listDailyClosuresRange,
        listDailySedeClosuresRange:fb.listDailySedeClosuresRange,
        listSedeStatusRange:fb.listSedeStatusRange, listAttendanceRange:fb.listAttendanceRange, listImportReplacementsRange:fb.listImportReplacementsRange, listSupernumerarioReplacementOccupancy:fb.listSupernumerarioReplacementOccupancy, listEmployeeDailyStatusRange:fb.listEmployeeDailyStatusRange,
        listDailyMetricsRange:fb.listDailyMetricsRange,
        streamDailyMetricsByDate:fb.streamDailyMetricsByDate,
        streamIncapacitadosByDate:fb.streamIncapacitadosByDate,
        streamIncapacidades:fb.streamIncapacidades,
        uploadIncapacidadSupport:guardWrite(PERMS.MANAGE_INCAPACITIES,fb.uploadIncapacidadSupport),
        createIncapacidad:guardWrite(PERMS.MANAGE_INCAPACITIES,fb.createIncapacidad),
        updateIncapacidad:guardWrite(PERMS.MANAGE_INCAPACITIES,fb.updateIncapacidad),
        setIncapacidadStatus:guardWrite(PERMS.MANAGE_INCAPACITIES,fb.setIncapacidadStatus),
        listIncapacidadesRange:fb.listIncapacidadesRange,
        streamImportHistory:fb.streamImportHistory, streamDailyClosures:fb.streamDailyClosures, streamWhatsAppIncoming:fb.streamWhatsAppIncoming,
        streamAttendanceByDate:fb.streamAttendanceByDate, streamAttendanceRecent:fb.streamAttendanceRecent, streamImportReplacementsByDate:fb.streamImportReplacementsByDate
      };

      fb.authState(async (user, authEvent)=>{
        const syncToken = ++authSyncToken;
        const prevUid = String(getState().user?.uid || '').trim();
        const nextUid = String(user?.uid || '').trim();
        const sameUser = Boolean(prevUid && nextUid && prevUid === nextUid);
        if(!sameUser){ if(unsubRoleMatrix){unsubRoleMatrix();unsubRoleMatrix=null;} if(unsubUserOverrides){unsubUserOverrides();unsubUserOverrides=null;} }
        if(!user){
          setState({ user:null, userProfile:null, userOverrides:{} });
          headerMount.replaceChildren(Header(deps));
          renderSidebar();
          if(authEvent==='PASSWORD_RECOVERY' || isRecoveryHash()) navigate('/reset-password');
          else if(!isPublicAuthRoute()) navigate('/login');
          else refreshRoute();
          return;
        }
        await fb.ensureUserProfile(user); const profile=await fb.loadUserProfile(user.uid);
        if(syncToken!==authSyncToken) return;
        const status=String(profile?.estado||'activo').toLowerCase();
        if(status==='inactivo' || status==='eliminado'){
          try{ sessionStorage.setItem('auth_block_msg', status==='eliminado' ? 'Tu usuario fue eliminado. Contacta al administrador.' : 'Tu usuario esta inactivo. Contacta al administrador.'); }catch{}
          await fb.logout();
          return;
        }
        if(String(profile?.role||'').trim().toLowerCase()===ROLES.TABLET_QR){
          window.location.replace('qr.html');
          return;
        }
        setState({ user, userProfile: profile });
        if(!sameUser || !unsubRoleMatrix) unsubRoleMatrix=fb.streamRoleMatrix((map)=> setState({ roleMatrix: map }));
        if(!sameUser || !unsubUserOverrides) unsubUserOverrides=fb.streamUserOverrides(user.uid,(ov)=> setState({ userOverrides: ov||{} }));
        headerMount.replaceChildren(Header(deps)); renderSidebar();
        if(authEvent==='PASSWORD_RECOVERY' || isRecoveryHash()) navigate('/reset-password');
        else if(location.hash==='' || location.hash==="#/login") navigate('/');
        else if(!sameUser) refreshRoute();
      });

      if(!routerStarted){
        startRouter();
        routerStarted = true;
      }
    })
    .catch((err) => {
      console.error('Supabase init failed:', err);
      if(!routerStarted){
        startRouter();
        routerStarted = true;
      }
    });

  addRoute('/login', ()=> Login(root, deps));
  addRoute('/forgot-password', ()=> ForgotPassword(root, deps));
  addRoute('/reset-password', ()=> ResetPassword(root, deps));
  addRoute('/', ()=> requireAuth(()=> Home(root, deps)));
  addRoute('/contact', ()=> requireAuth(()=> Contact(root)));
  addRoute('/data-treatment', ()=> requireAuth(()=> DataTreatment(root)));
  addRoute('/about', ()=> requireAuth(()=> About(root)));
  addRoute('/notes', ()=> requireAuth(()=> Notes(root)));

  // Gobierno
  addRoute('/gobierno-dashboard', ()=> requireAuth(()=> guardAny([PERMS.VIEW_USERS, PERMS.VIEW_PERMISSIONS, PERMS.VIEW_AUDIT], ()=> GobiernoDashboard(root, deps), { allowSuperAdmin: true })));
  addRoute('/permissions', ()=> requireAuth(()=> { if(!isSuperAdmin() && !can(PERMS.VIEW_PERMISSIONS)) return block('No tienes permiso para acceder a esta sección.'); return PermissionsCenter(root, deps); }));
  addRoute('/permissions-audit', ()=> requireAuth(()=> { if(!isSuperAdmin() && !can(PERMS.VIEW_AUDIT)) return block('No tienes permiso para consultar auditoria.'); return PermissionsAudit(root, deps); }));

  // Administración
  addRoute('/administracion-dashboard', ()=> requireAuth(()=> guardAny([PERMS.VIEW_ZONES, PERMS.VIEW_DEPENDENCIES, PERMS.VIEW_SEDES, PERMS.VIEW_QR_SCANNER, PERMS.VIEW_QR_DEVICES, PERMS.VIEW_CARGOS, PERMS.VIEW_NOVEDADES], ()=> AdministracionDashboard(root, deps))));
  addRoute('/users', ()=> requireAuth(()=> guard(PERMS.VIEW_USERS, ()=> UsersAdmin(root, deps))));
  addRoute('/zones', ()=> requireAuth(()=> guard(PERMS.VIEW_ZONES, ()=> ZonesAdmin(root, deps))));
  addRoute('/dependencies', ()=> requireAuth(()=> guard(PERMS.VIEW_DEPENDENCIES, ()=> DependenciesAdmin(root, deps))));
  addRoute('/sedes', ()=> requireAuth(()=> guard(PERMS.VIEW_SEDES, ()=> SedesAdmin(root, deps))));
  addRoute('/bulk-upload-sedes', ()=> requireAuth(()=> guard(PERMS.VIEW_BULK_UPLOAD_SEDES, ()=> CargueMasivoSedesAdmin(root, deps))));
  addRoute('/empleados-dashboard', ()=> requireAuth(()=> guardAny([PERMS.VIEW_EMPLOYEES, PERMS.VIEW_EMPLOYEE_NOVELTIES, PERMS.VIEW_SUPERVISORS, PERMS.VIEW_INCAPACITIES], ()=> EmpleadosDashboard(root, deps))));
  addRoute('/employees', ()=> requireAuth(()=> guard(PERMS.VIEW_EMPLOYEES, ()=> EmployeesAdmin(root, deps))));
  addRoute('/employee-novelties', ()=> requireAuth(()=> guard(PERMS.VIEW_EMPLOYEE_NOVELTIES, ()=> EmployeeNovelties(root, deps))));
  addRoute('/supernumerarios', ()=> requireAuth(()=> guard(PERMS.VIEW_SUPERNUMERARIOS, ()=> SupernumerariosAdmin(root, deps))));
  addRoute('/bulk-upload', ()=> requireAuth(()=> guard(PERMS.VIEW_BULK_UPLOAD_EMPLOYEES, ()=> CargueMasivoAdmin(root, deps))));
  addRoute('/cargos', ()=> requireAuth(()=> guard(PERMS.VIEW_CARGOS, ()=> CargosAdmin(root, deps))));
  addRoute('/novedades', ()=> requireAuth(()=> guard(PERMS.VIEW_NOVEDADES, ()=> NovedadesAdmin(root, deps))));
  addRoute('/supervisors', ()=> requireAuth(()=> guard(PERMS.VIEW_SUPERVISORS, ()=> SupervisorsAdmin(root, deps))));

  // Operación
  addRoute('/operacion-dashboard', ()=> requireAuth(()=> guardAny([PERMS.VIEW_OPERATION_REGISTRY, PERMS.VIEW_QR_DAILY_REGISTRY, PERMS.VIEW_SUPERNUMERARIOS, PERMS.VIEW_IMPORT_HISTORY], ()=> OperacionDashboard(root, deps))));
  addRoute('/imports', ()=> { navigate('/registros-vivo'); return null; });
  addRoute('/whatsapp-live', ()=> { navigate('/registros-vivo'); return null; });
  addRoute('/registros-vivo', ()=> requireAuth(()=> guard(PERMS.VIEW_OPERATION_REGISTRY, ()=> WhatsAppLive(root, deps))));
  addRoute('/registro-sede', ()=> requireAuth(()=> guard(PERMS.VIEW_OPERATION_REGISTRY, ()=> RegistroSede(root, deps))));
  addRoute('/lector-qr', ()=> requireAuth(()=> guard(PERMS.VIEW_QR_SCANNER, ()=> QrTabletScanner(root, deps))));
  addRoute('/tablets-qr', ()=> requireAuth(()=> guard(PERMS.VIEW_QR_DEVICES, ()=> QrDevicesInfo(root, deps))));
  addRoute('/registro-qr', ()=> requireAuth(()=> guard(PERMS.VIEW_QR_DAILY_REGISTRY, ()=> QrDailyRegistry(root, deps))));
  addRoute('/imports-replacements', ()=> requireAuth(()=> guard(PERMS.VIEW_OPERATION_REGISTRY, ()=> ImportReplacements(root, deps))));
  addRoute('/import-history', ()=> requireAuth(()=> guard(PERMS.VIEW_IMPORT_HISTORY, ()=> ImportHistory(root, deps))));
  addRoute('/absenteeism', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_ABSENTEEISM, ()=> Absenteeism(root, deps))));

  // Consultor
  addRoute('/reportes-dashboard', ()=> requireAuth(()=> guardAny(REPORT_VIEW_PERMISSIONS, ()=> ReportesDashboard(root, deps))));
  addRoute('/reports', ()=> requireAuth(()=> {
    if (can(PERMS.VIEW_REPORTS_CLIENT)) { navigate('/reports-daily-history'); return null; }
    if (can(PERMS.VIEW_REPORTS_QR_HISTORY)) { navigate('/reports-qr-history'); return null; }
    if (can(PERMS.VIEW_REPORTS_ABSENTEEISM)) { navigate('/absenteeism'); return null; }
    if (can(PERMS.VIEW_REPORTS_EMPLOYEES)) { navigate('/reports-employees'); return null; }
    if (can(PERMS.VIEW_REPORTS_HIRING)) { navigate('/reports-hiring'); return null; }
    if (can(PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED)) { navigate('/reports-novelties-consolidated'); return null; }
    if (can(PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED)) { navigate('/reports-services-consolidated'); return null; }
    return block('No tienes permiso para acceder a esta seccion.');
  }));
  addRoute('/reports-client', ()=> { navigate('/reports-daily-history'); return null; });
  addRoute('/reports-company', ()=> requireAuth(()=> guardAny(CONSOLIDATED_REPORT_VIEW_PERMISSIONS, ()=> {
    if (can(PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED)) { navigate('/reports-novelties-consolidated'); return null; }
    if (can(PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED)) { navigate('/reports-services-consolidated'); return null; }
    return block('No tienes permiso para acceder a esta seccion.');
  })));
  addRoute('/reports-daily', ()=> { navigate('/reports-daily-history'); return null; });
  addRoute('/reports-daily-history', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_CLIENT, ()=> HistoricalDailyRegistry(root, deps))));
  addRoute('/reports-qr-history', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_QR_HISTORY, ()=> HistoricalQrRegistry(root, deps))));
  addRoute('/reports-employees', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_EMPLOYEES, ()=> EmployeesReport(root, deps))));
  addRoute('/reports-hiring', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_HIRING, ()=> HiringBySedeReport(root, deps))));
  addRoute('/reports-novelties-consolidated', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED, ()=> ConsolidatedNoveltiesReport(root, deps))));
  addRoute('/reports-services-consolidated', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED, ()=> ConsolidatedReports(root, deps))));
  addRoute('/reports-consolidated', ()=> requireAuth(()=> guardAny(CONSOLIDATED_REPORT_VIEW_PERMISSIONS, ()=> {
    if (can(PERMS.VIEW_REPORTS_NOVELTIES_CONSOLIDATED)) { navigate('/reports-novelties-consolidated'); return null; }
    if (can(PERMS.VIEW_REPORTS_SERVICES_CONSOLIDATED)) { navigate('/reports-services-consolidated'); return null; }
    return block('No tienes permiso para acceder a esta seccion.');
  })));

  // Supervisor/Empleado
  addRoute('/cargue-masivo-dashboard', ()=> requireAuth(()=> guardAny([PERMS.VIEW_BULK_UPLOAD_SEDES, PERMS.VIEW_BULK_UPLOAD_EMPLOYEES], ()=> CargueMasivoDashboard(root, deps))));
  addRoute('/upload', ()=> requireAuth(()=> guard(PERMS.VIEW_INCAPACITIES, ()=> CargarDatos(root, deps))));
})();
function getRoutePath(){ return (window.location.hash || '#/login').replace('#', '').split('?')[0]; }
function isPublicAuthRoute(){ return PUBLIC_AUTH_ROUTES.has(getRoutePath()); }
function isRecoveryHash(){
  const urlMarkers = `${window.location.search || ''}&${window.location.hash || ''}`;
  return /(?:[?&#])reset_password=1\b|(?:[?&#])type=recovery\b|(?:[?&#])access_token=|(?:[?&#])code=/.test(urlMarkers);
}
function requireAuth(ok){ const { user }=getState(); if(!user){ navigate('/login'); return; } return ok?.(); }
function guard(perm, ok){ if(!can(perm)) return block('No tienes permiso para acceder a esta sección.'); return ok?.(); }
function guardAny(perms, ok, options = {}) {
  if (options?.allowSuperAdmin && isSuperAdmin()) return ok?.();
  if (!(perms || []).some((perm) => can(perm))) return block('No tienes permiso para acceder a esta sección.');
  return ok?.();
}
function block(text){ const div=document.createElement('div'); div.className='main-card'; div.innerHTML=`<h2 style="margin:0 0 .5rem 0;">RockyDEMO</h2><p>${text}</p>`; root.replaceChildren(div); return null; }
