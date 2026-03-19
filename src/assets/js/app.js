import { Header } from './components/Header.js';
import { Footer } from './components/Footer.js';
import { Sidebar } from './components/Sidebar.js';

import { Home } from './components/Home.js';
import { Contact } from './components/Contact.js';
import { DataTreatment } from './components/DataTreatment.js';
import { About } from './components/About.js';
import { Login } from './components/Login.js';
import { Notes } from './components/Notes.js';

import { UsersAdmin } from './components/UsersAdmin.js';
import { ZonesAdmin } from './components/ZonesAdmin.js';
import { DependenciesAdmin } from './components/DependenciesAdmin.js';
import { SedesAdmin } from './components/SedesAdmin.js';
import { EmployeesAdmin } from './components/EmployeesAdmin.js';
import { SupernumerariosAdmin } from './components/SupernumerariosAdmin.js';
import { SupervisorsAdmin } from './components/SupervisorsAdmin.js';
import { CargosAdmin } from './components/CargosAdmin.js';
import { NovedadesAdmin } from './components/NovedadesAdmin.js';
import { CargueMasivoAdmin } from './components/CargueMasivoAdmin.js';
import { CargueMasivoSedesAdmin } from './components/CargueMasivoSedesAdmin.js';
import { ImportHistory } from './components/ImportHistory.js';
import { Payroll } from './components/Payroll.js';
import { Absenteeism } from './components/Absenteeism.js';
import { AbsenteeismConsolidated } from './components/AbsenteeismConsolidated.js';
import { Reports } from './components/Reports.js';
import { ImportReplacements } from './components/ImportReplacements.js';
import { CargarDatos } from './components/CargarDatos.js';
import { PermissionsCenter } from './components/PermissionsCenter.js';
import { WhatsAppLive } from './components/WhatsAppLive.js';
import { RegistroSede } from './components/RegistroSede.js';
import { RegistroDiarioSupervisor } from './components/RegistroDiarioSupervisor.js';

import { addRoute, startRouter, navigate, refreshRoute } from './router.js';
import { getState, setState } from './state.js';
import { can, PERMS, isSuperAdmin } from './permissions.js';
const sidebarMount=document.getElementById('app-sidebar');
const headerMount =document.getElementById('app-header');
const footerMount =document.getElementById('app-footer');
const root        =document.getElementById('app-root');

let deps={};
sidebarMount.replaceChildren(Sidebar());
headerMount.replaceChildren(Header());
footerMount.replaceChildren(Footer());

let unsubRoleMatrix=null; let unsubUserOverrides=null; let unsubAudit=null;
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
        ensureUserProfile:fb.ensureUserProfile, loadUserProfile:fb.loadUserProfile, createUserProfile:fb.createUserProfile,
        addNote:fb.addNote, streamNotes:fb.streamNotes,
        streamRoleMatrix:fb.streamRoleMatrix, setRolePermissions:fb.setRolePermissions, streamUserOverrides:fb.streamUserOverrides,
        getUserOverrides:fb.getUserOverrides, setUserOverrides:fb.setUserOverrides, clearUserOverrides:fb.clearUserOverrides,
        addAuditLog:fb.addAuditLog, streamAuditLogs:(cb)=>{ if(unsubAudit)unsubAudit(); unsubAudit=fb.streamAuditLogs(cb); return unsubAudit; },
        streamUsers:fb.streamUsers, setUserRole:guardWrite(PERMS.EDIT_USERS,fb.setUserRole), setUserStatus:guardWrite(PERMS.EDIT_USERS,fb.setUserStatus), softDeleteUser:guardWrite(PERMS.EDIT_USERS,fb.softDeleteUser), findUserByEmail:fb.findUserByEmail,
        streamZones:fb.streamZones, createZone:guardWrite(PERMS.EDIT_ZONES,fb.createZone), updateZone:guardWrite(PERMS.EDIT_ZONES,fb.updateZone), setZoneStatus:guardWrite(PERMS.EDIT_ZONES,fb.setZoneStatus), findZoneByCode:fb.findZoneByCode, getNextZoneCode:fb.getNextZoneCode,
        streamDependencies:fb.streamDependencies, createDependency:guardWrite(PERMS.EDIT_DEPENDENCIES,fb.createDependency), updateDependency:guardWrite(PERMS.EDIT_DEPENDENCIES,fb.updateDependency), setDependencyStatus:guardWrite(PERMS.EDIT_DEPENDENCIES,fb.setDependencyStatus), findDependencyByCode:fb.findDependencyByCode, getNextDependencyCode:fb.getNextDependencyCode,
        streamSedes:fb.streamSedes, createSede:guardWrite(PERMS.EDIT_SEDES,fb.createSede), updateSede:guardWrite(PERMS.EDIT_SEDES,fb.updateSede), setSedeStatus:guardWrite(PERMS.EDIT_SEDES,fb.setSedeStatus), findSedeByCode:fb.findSedeByCode, getNextSedeCode:fb.getNextSedeCode,
        createSedesBulk:guardWrite(PERMS.EDIT_SEDES,fb.createSedesBulk),
        streamEmployees:fb.streamEmployees, createEmployee:guardWrite(PERMS.EDIT_EMPLOYEES,fb.createEmployee), updateEmployee:guardWrite(PERMS.EDIT_EMPLOYEES,fb.updateEmployee), setEmployeeStatus:guardWrite(PERMS.EDIT_EMPLOYEES,fb.setEmployeeStatus), findEmployeeByCode:fb.findEmployeeByCode, findEmployeeByDocument:fb.findEmployeeByDocument, getNextEmployeeCode:fb.getNextEmployeeCode,
        streamEmployeeCargoHistory:fb.streamEmployeeCargoHistory,
        createEmployeesBulk:guardWrite(PERMS.EDIT_EMPLOYEES,fb.createEmployeesBulk),
        streamSupernumerarios:fb.streamSupernumerarios, createSupernumerario:guardWrite(PERMS.EDIT_SUPERNUMERARIOS,fb.createSupernumerario), updateSupernumerario:guardWrite(PERMS.EDIT_SUPERNUMERARIOS,fb.updateSupernumerario), setSupernumerarioStatus:guardWrite(PERMS.EDIT_SUPERNUMERARIOS,fb.setSupernumerarioStatus), findSupernumerarioByCode:fb.findSupernumerarioByCode, findSupernumerarioByDocument:fb.findSupernumerarioByDocument, getNextSupernumerarioCode:fb.getNextSupernumerarioCode,
        streamCargos:fb.streamCargos, createCargo:guardWrite(PERMS.EDIT_CARGOS,fb.createCargo), updateCargo:guardWrite(PERMS.EDIT_CARGOS,fb.updateCargo), setCargoStatus:guardWrite(PERMS.EDIT_CARGOS,fb.setCargoStatus), findCargoByCode:fb.findCargoByCode, getNextCargoCode:fb.getNextCargoCode,
        streamNovedades:fb.streamNovedades, createNovedad:guardWrite(PERMS.EDIT_NOVEDADES,fb.createNovedad), updateNovedad:guardWrite(PERMS.EDIT_NOVEDADES,fb.updateNovedad), setNovedadStatus:guardWrite(PERMS.EDIT_NOVEDADES,fb.setNovedadStatus), findNovedadByCode:fb.findNovedadByCode, findNovedadByCodigoNovedad:fb.findNovedadByCodigoNovedad, getNextNovedadCode:fb.getNextNovedadCode,
        streamSupervisors:fb.streamSupervisors, createSupervisor:guardWrite(PERMS.EDIT_SUPERVISORS,fb.createSupervisor), updateSupervisor:guardWrite(PERMS.EDIT_SUPERVISORS,fb.updateSupervisor), setSupervisorStatus:guardWrite(PERMS.EDIT_SUPERVISORS,fb.setSupervisorStatus), findSupervisorByCode:fb.findSupervisorByCode, findSupervisorByDocument:fb.findSupervisorByDocument, getNextSupervisorCode:fb.getNextSupervisorCode,
        confirmImportOperation:fb.confirmImportOperation, saveImportReplacements:fb.saveImportReplacements,
        closeOperationDayManual:fb.closeOperationDayManual,
        isOperationDayClosed:fb.isOperationDayClosed, listClosedOperationDaysRange:fb.listClosedOperationDaysRange, listDailyClosuresRange:fb.listDailyClosuresRange,
        listDailySedeClosuresRange:fb.listDailySedeClosuresRange,
        listSedeStatusRange:fb.listSedeStatusRange, listAttendanceRange:fb.listAttendanceRange, listImportReplacementsRange:fb.listImportReplacementsRange,
        listDailyMetricsRange:fb.listDailyMetricsRange,
        streamDailyMetricsByDate:fb.streamDailyMetricsByDate,
        streamIncapacitadosByDate:fb.streamIncapacitadosByDate,
        streamImportHistory:fb.streamImportHistory, streamDailyClosures:fb.streamDailyClosures, streamWhatsAppIncoming:fb.streamWhatsAppIncoming,
        streamAttendanceByDate:fb.streamAttendanceByDate, streamAttendanceRecent:fb.streamAttendanceRecent, streamImportReplacementsByDate:fb.streamImportReplacementsByDate
      };

      refreshRoute();

      fb.authState(async (user)=>{
        if(unsubRoleMatrix){unsubRoleMatrix();unsubRoleMatrix=null;} if(unsubUserOverrides){unsubUserOverrides();unsubUserOverrides=null;}
        if(!user){ setState({ user:null, userProfile:null, userOverrides:{} }); headerMount.replaceChildren(Header(deps)); sidebarMount.replaceChildren(Sidebar()); if(location.hash!=="#/login") navigate('/login'); else refreshRoute(); return; }
        await fb.ensureUserProfile(user); const profile=await fb.loadUserProfile(user.uid);
        const status=String(profile?.estado||'activo').toLowerCase();
        if(status==='inactivo' || status==='eliminado'){
          try{ sessionStorage.setItem('auth_block_msg', status==='eliminado' ? 'Tu usuario fue eliminado. Contacta al administrador.' : 'Tu usuario esta inactivo. Contacta al administrador.'); }catch{}
          await fb.logout();
          return;
        }
        setState({ user, userProfile: profile });
        unsubRoleMatrix=fb.streamRoleMatrix((map)=> setState({ roleMatrix: map }));
        unsubUserOverrides=fb.streamUserOverrides(user.uid,(ov)=> setState({ userOverrides: ov||{} }));
        headerMount.replaceChildren(Header(deps)); sidebarMount.replaceChildren(Sidebar());
        if(location.hash==='' || location.hash==="#/login") navigate('/');
      });
    })
    .catch((err) => {
      console.error('Supabase init failed:', err);
    });

  addRoute('/login', ()=> Login(root, deps));
  addRoute('/', ()=> requireAuth(()=> Home(root, deps)));
  addRoute('/contact', ()=> requireAuth(()=> Contact(root)));
  addRoute('/data-treatment', ()=> requireAuth(()=> DataTreatment(root)));
  addRoute('/about', ()=> requireAuth(()=> About(root)));
  addRoute('/notes', ()=> requireAuth(()=> Notes(root)));

  // Gobierno
  addRoute('/permissions', ()=> requireAuth(()=> { if(!isSuperAdmin()) return block('Solo SuperAdmin puede ver esto.'); return PermissionsCenter(root, deps); }));

  // Administración
  addRoute('/users', ()=> requireAuth(()=> guard(PERMS.VIEW_USERS, ()=> UsersAdmin(root, deps))));
  addRoute('/zones', ()=> requireAuth(()=> guard(PERMS.VIEW_ZONES, ()=> ZonesAdmin(root, deps))));
  addRoute('/dependencies', ()=> requireAuth(()=> guard(PERMS.VIEW_DEPENDENCIES, ()=> DependenciesAdmin(root, deps))));
  addRoute('/sedes', ()=> requireAuth(()=> guard(PERMS.VIEW_SEDES, ()=> SedesAdmin(root, deps))));
  addRoute('/bulk-upload-sedes', ()=> requireAuth(()=> guard(PERMS.EDIT_SEDES, ()=> CargueMasivoSedesAdmin(root, deps))));
  addRoute('/employees', ()=> requireAuth(()=> guard(PERMS.VIEW_EMPLOYEES, ()=> EmployeesAdmin(root, deps))));
  addRoute('/supernumerarios', ()=> requireAuth(()=> guard(PERMS.VIEW_SUPERNUMERARIOS, ()=> SupernumerariosAdmin(root, deps))));
  addRoute('/bulk-upload', ()=> requireAuth(()=> guard(PERMS.EDIT_EMPLOYEES, ()=> CargueMasivoAdmin(root, deps))));
  addRoute('/cargos', ()=> requireAuth(()=> guard(PERMS.VIEW_CARGOS, ()=> CargosAdmin(root, deps))));
  addRoute('/novedades', ()=> requireAuth(()=> guard(PERMS.VIEW_NOVEDADES, ()=> NovedadesAdmin(root, deps))));
  addRoute('/supervisors', ()=> requireAuth(()=> guard(PERMS.VIEW_SUPERVISORS, ()=> SupervisorsAdmin(root, deps))));

  // Operación
  addRoute('/imports', ()=> { navigate('/registros-vivo'); return null; });
  addRoute('/whatsapp-live', ()=> { navigate('/registros-vivo'); return null; });
  addRoute('/registros-vivo', ()=> requireAuth(()=> {
    const role=String(getState().userProfile?.role||'').trim().toLowerCase();
    if(role==='supervisor'){
      if(!can(PERMS.UPLOAD_DATA)) return block('No tienes permiso para acceder a esta sección.');
      return RegistroDiarioSupervisor(root, deps);
    }
    return guard(PERMS.IMPORT_DATA, ()=> WhatsAppLive(root, deps));
  }));
  addRoute('/registro-sede', ()=> requireAuth(()=> guard(PERMS.IMPORT_DATA, ()=> RegistroSede(root, deps))));
  addRoute('/imports-replacements', ()=> requireAuth(()=> guard(PERMS.IMPORT_DATA, ()=> ImportReplacements(root, deps))));
  addRoute('/import-history', ()=> requireAuth(()=> guard(PERMS.VIEW_IMPORT_HISTORY, ()=> ImportHistory(root, deps))));
  addRoute('/payroll', ()=> requireAuth(()=> guard(PERMS.RUN_PAYROLL, ()=> Payroll(root))));
  addRoute('/absenteeism', ()=> requireAuth(()=> guard(PERMS.MANAGE_ABSENTEEISM, ()=> Absenteeism(root, deps))));
  addRoute('/absenteeism-consolidated', ()=> requireAuth(()=> guard(PERMS.MANAGE_ABSENTEEISM, ()=> AbsenteeismConsolidated(root, deps))));

  // Consultor
  addRoute('/reports', ()=> requireAuth(()=> guard(PERMS.VIEW_REPORTS, ()=> Reports(root, deps))));

  // Supervisor/Empleado
  addRoute('/upload', ()=> requireAuth(()=> guard(PERMS.UPLOAD_DATA, ()=> CargarDatos(root))));

  startRouter();
})();
function requireAuth(ok){ const { user }=getState(); if(!user){ navigate('/login'); return; } return ok?.(); }
function guard(perm, ok){ if(!can(perm)) return block('No tienes permiso para acceder a esta sección.'); return ok?.(); }
function block(text){ const div=document.createElement('div'); div.className='main-card'; div.innerHTML=`<h2 style="margin:0 0 .5rem 0;">RockyEDU</h2><p>${text}</p>`; root.replaceChildren(div); return null; }
