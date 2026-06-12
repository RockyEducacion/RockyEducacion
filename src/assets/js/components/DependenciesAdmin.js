import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
export const DependenciesAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Dependencias']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'dCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Nombre']), el('input',{id:'dName',className:'input',placeholder:'Nombre de la dependencia'}) ]),
        el('button',{id:'btnCreate',className:'btn btn--primary'},['Crear dependencia']),
        el('span',{id:'msgCreate',className:'text-muted'},[' '])
      ])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo o nombre...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Estado']), el('select',{id:'selStatus',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'activo'},['Activos']), el('option',{value:'inactivo'},['Inactivos']) ]) ]),
      ]),
      el('div',{className:'mt-2 table-wrap'},[
        el('table',{className:'table',id:'tbl'},[
          el('thead',{},[ el('tr',{},[ el('th',{'data-sort':'codigo',style:'cursor:pointer'},['Codigo']), el('th',{'data-sort':'nombre',style:'cursor:pointer'},['Nombre']), el('th',{'data-sort':'estado',style:'cursor:pointer'},['Estado']), el('th',{},['Acciones']) ]) ]),
          el('tbody',{})
        ])
      ]),
      el('p',{id:'msg',className:'text-muted mt-2'},[' '])
    ])
  ]);

  const tabCreateBtn=qs('#tabCreateBtn',ui);
  const tabListBtn=qs('#tabListBtn',ui);
  const tabCreate=qs('#tabCreate',ui);
  const tabList=qs('#tabList',ui);
  qs('.tabs', ui)?.classList.add('hidden');
  tabCreate.classList.add('hidden');
  tabList.classList.remove('hidden');
  function setTab(which){
    const isCreate=which==='create';
    tabCreateBtn.classList.toggle('is-active',isCreate);
    tabListBtn.classList.toggle('is-active',!isCreate);
    tabCreate.classList.toggle('hidden',!isCreate);
    tabList.classList.toggle('hidden',isCreate);
  }
  tabCreateBtn.addEventListener('click',()=> setTab('create'));
  tabListBtn.addEventListener('click',()=> setTab('list'));

  async function openCreateModal(){
    const modal=await showActionModal({
      title:'Crear dependencia',
      message:'Completa la informacion para crear una dependencia.',
      confirmText:'Crear dependencia',
      fields:[{ id:'name', label:'Nombre', type:'text', required:true, placeholder:'Nombre de la dependencia' }]
    });
    if(!modal.confirmed) return;
    const name=String(modal.values.name||'').trim();
    if(!name){ alert('Escribe el nombre de la dependencia.'); return; }
    try{
      const code=await deps.getNextDependencyCode?.();
      const id=await deps.createDependency?.({ codigo:code, nombre:name });
      await deps.addAuditLog?.({ targetType:'dependency', targetId:id, action:'create_dependency', after:{ codigo:code, nombre:name, estado:'activo' } });
      alert('Dependencia creada OK');
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  const btnOpenCreate=el('button',{id:'btnOpenCreate',className:'btn btn--primary right',type:'button'},['Crear dependencia']);
  qs('#tabList .form-row',ui)?.append(btnOpenCreate);
  btnOpenCreate.addEventListener('click',openCreateModal);

  qs('#btnCreate',ui).addEventListener('click',async()=>{
    const name=qs('#dName',ui).value.trim(); const msg=qs('#msgCreate',ui); msg.textContent=' ';
    if(!name){ msg.textContent='Escribe el nombre de la dependencia.'; return; }
    try{
      const code=await deps.getNextDependencyCode?.();
      const id=await deps.createDependency?.({ codigo:code, nombre:name });
      await deps.addAuditLog?.({ targetType:'dependency', targetId:id, action:'create_dependency', after:{ codigo:code, nombre:name, estado:'activo' } });
      qs('#dName',ui).value=''; msg.textContent='Dependencia creada OK'; setTab('list'); setTimeout(()=> msg.textContent=' ',1200);
    }catch(e){ msg.textContent='Error: '+(e?.message||e); }
  });
  let snapshot=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'dependencies',after:'#tabList .table-wrap',onChange:render});
  const search=()=> qs('#txtSearch',ui).value.trim().toLowerCase();
  const filterStatus=()=> qs('#selStatus',ui).value;
  function sortVal(d,key){ if(key==='createdAt'){ try{ const x=d.createdAt?.toDate?d.createdAt.toDate(): (d.createdAt?new Date(d.createdAt):null); return x?x.getTime():0; }catch{return 0;} } return String(d[key]??'').toLowerCase(); }
  function sortData(data){ if(!sortKey) return data; const out=[...data]; out.sort((a,b)=>{ const va=sortVal(a,sortKey); const vb=sortVal(b,sortKey); if(va===vb) return 0; return va>vb?sortDir:-sortDir; }); return out; }
  function updateSortIndicators(){ ui.querySelectorAll('th[data-sort]').forEach((th)=>{ const base=th.dataset.baseLabel||th.textContent.replace(/\s[\^v▲▼]$/,''); th.dataset.baseLabel=base; const key=th.getAttribute('data-sort'); th.textContent=(sortKey===key)?`${base} ${sortDir===1?'▲':'▼'}`:base; }); }
  function initSorting(){ ui.querySelectorAll('th[data-sort]').forEach((th)=> th.addEventListener('click',()=>{ const key=th.getAttribute('data-sort'); if(sortKey===key) sortDir=sortDir*-1; else { sortKey=key; sortDir=1; } paginator.reset(); render(); })); }
  function render(){ const term=search(); const st=filterStatus(); const data=snapshot.filter(d=> ((!term||(d.codigo||'').toLowerCase().includes(term)||(d.nombre||'').toLowerCase().includes(term)) && (!st || d.estado===st))); const sorted=sortData(data); const pageRows=paginator.slice(sorted); tbody.replaceChildren(...pageRows.map(d=> row(d))); const msg=qs('#msg',ui); if(msg) msg.textContent=`Total registros filtrados: ${data.length}`; updateSortIndicators(); }
  function row(d){ const tr=el('tr',{'data-id':d.id}); const tdCodigo=el('td',{},[d.codigo||'-']); const tdNombre=el('td',{},[d.nombre||'-']); const tdEstado=el('td',{},[ statusBadge(d.estado) ]); const tdAcc=el('td',{},[ actionsCell(d) ]); tr.append(tdCodigo,tdNombre,tdEstado,tdAcc); return tr; }
  function statusBadge(st){ return el('span',{className:'badge '+(st==='activo'?'badge--ok':'badge--off')},[st||'-']); }
  function formatDate(ts){ try{ const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null); return d? new Date(d).toLocaleString(): '-'; }catch{ return '-'; } }
  function auditInfoData(d){
    const hasMod = Boolean(d.lastModifiedAt || d.lastModifiedByEmail || d.lastModifiedByUid);
    return {
      action: hasMod ? 'Ultima modificacion' : 'Creacion',
      user: hasMod ? (d.lastModifiedByEmail||d.lastModifiedByUid||'-') : (d.createdByEmail||d.createdByUid||'-'),
      date: hasMod ? formatDate(d.lastModifiedAt) : formatDate(d.createdAt)
    };
  }
  function actionsCell(d){ const box=el('div',{className:'row-actions'},[]); const btnEdit=el('button',{className:'btn btn--icon',title:'Editar'},['\u270E']); btnEdit.addEventListener('click',()=>{ const tr=tbody.querySelector(`tr[data-id="${d.id}"]`); if(tr) startEdit(tr,d); }); const btnToggle=el('button',{className:'btn btn--icon '+(d.estado==='activo'?'btn--danger':'' ),title:d.estado==='activo'?'Desactivar':'Activar','aria-label':d.estado==='activo'?'Desactivar':'Activar'},[ d.estado==='activo'?'\u23FB':'\u21BA' ]); btnToggle.addEventListener('click',async()=>{ const target=d.estado==='activo'?'inactivo':'activo'; const modal=await showActionModal({ title:`${target==='inactivo'?'Desactivar':'Activar'} dependencia`, message:`Dependencia: ${d.nombre||'-'}`, confirmText:target==='inactivo'?'Desactivar':'Activar', fields:[{ id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Escribe el motivo o detalle de esta accion' }] }); if(!modal.confirmed) return; try{ await deps.setDependencyStatus?.(d.id,target); await deps.addAuditLog?.({ targetType:'dependency', targetId:d.id, action: target==='activo'?'activate_dependency':'deactivate_dependency', before:{estado:d.estado}, after:{estado:target}, note: modal.values.detail||null }); }catch(e){ alert('Error: '+(e?.message||e)); } }); const btnInfo=el('button',{className:'btn btn--icon',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']); btnInfo.addEventListener('click',()=>{ const info=auditInfoData(d); showInfoModal('Informacion del registro',[`Evento: ${info.action}`,`Usuario: ${info.user}`,`Fecha: ${info.date}`]); }); box.append(btnEdit,btnToggle,btnInfo); return box; }
  function startEdit(tr,d){ const cur={ codigo:d.codigo||'', nombre:d.nombre||'' }; const tds=tr.querySelectorAll('td'); tds[0].replaceChildren(el('input',{className:'input',value:cur.codigo,style:'max-width:160px'})); tds[1].replaceChildren(el('input',{className:'input',value:cur.nombre,style:'max-width:260px'})); tds[2].replaceChildren(statusBadge(d.estado));
    const box=el('div',{className:'row-actions'},[]); const btnSave=el('button',{className:'btn btn--primary'},['Guardar']); const btnCancel=el('button',{className:'btn'},['Cancelar']); btnSave.addEventListener('click',async()=>{ const newCode=tds[0].querySelector('input').value.trim(); const newName=tds[1].querySelector('input').value.trim(); if(!newCode||!newName) return alert('Completa codigo y nombre.'); const modal=await showActionModal({ title:'Confirmar modificacion', message:`Dependencia: ${d.nombre||'-'}`, confirmText:'Guardar cambios', fields:[{ id:'detail', label:'Detalle de la modificacion', type:'textarea', required:true, placeholder:'Describe brevemente el cambio realizado' }] }); if(!modal.confirmed) return; try{ if(newCode!==d.codigo){ const dup=await deps.findDependencyByCode?.(newCode); if(dup && dup.id!==d.id) return alert('Ya existe una dependencia con ese codigo.'); } await deps.updateDependency?.(d.id,{ codigo:newCode, nombre:newName }); await deps.addAuditLog?.({ targetType:'dependency', targetId:d.id, action:'update_dependency', before:{ codigo:d.codigo, nombre:d.nombre }, after:{ codigo:newCode, nombre:newName }, note: modal.values.detail||null }); }catch(e){ alert('Error: '+(e?.message||e)); } }); btnCancel.addEventListener('click',()=> render()); box.append(btnSave,btnCancel); tds[3].replaceChildren(box); }
  const un=deps.streamDependencies?.((arr)=>{ snapshot=arr||[]; render(); });
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); }); qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui); return ()=> un?.();
};
