import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
export const NovedadesAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Novedades']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'nCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Codigo novedad']), el('input',{id:'nCodeRef',className:'input',placeholder:'Codigo de la novedad'}) ]),
        el('div',{},[ el('label',{className:'label'},['Nombre']), el('input',{id:'nName',className:'input',placeholder:'Nombre de la novedad'}) ]),
        el('div',{},[ el('label',{className:'label'},['Reemplazo']), el('select',{id:'nReemplazo',className:'select'},[ el('option',{value:''},['Seleccione...']), el('option',{value:'si'},['SI']), el('option',{value:'no'},['NO']) ]) ]),
        el('div',{},[ el('label',{className:'label'},['Nomina']), el('select',{id:'nNomina',className:'select'},[ el('option',{value:''},['Seleccione...']), el('option',{value:'si'},['SI']), el('option',{value:'no'},['NO']) ]) ]),
        el('button',{id:'btnCreate',className:'btn btn--primary'},['Crear novedad']),
        el('span',{id:'msgCreate',className:'text-muted'},[' '])
      ])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo o nombre...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Estado']), el('select',{id:'selStatus',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'activo'},['Activos']), el('option',{value:'inactivo'},['Inactivos']) ]) ]),
        el('div',{},[ el('label',{className:'label'},['Reemplazo']), el('select',{id:'selReemp',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'si'},['SI']), el('option',{value:'no'},['NO']) ]) ]),
        el('div',{},[ el('label',{className:'label'},['Nomina']), el('select',{id:'selNomina',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'si'},['SI']), el('option',{value:'no'},['NO']) ]) ]),
      ]),
      el('div',{className:'mt-2 table-wrap'},[
        el('table',{className:'table',id:'tbl'},[
          el('thead',{},[ el('tr',{},[
            el('th',{'data-sort':'codigo',style:'cursor:pointer'},['Codigo']),
            el('th',{'data-sort':'codigoNovedad',style:'cursor:pointer'},['Codigo novedad']),
            el('th',{'data-sort':'nombre',style:'cursor:pointer'},['Nombre']),
            el('th',{'data-sort':'reemplazo',style:'cursor:pointer'},['Reemplazo']),
            el('th',{'data-sort':'nomina',style:'cursor:pointer'},['Nomina']),
            el('th',{'data-sort':'estado',style:'cursor:pointer'},['Estado']),
            el('th',{},['Acciones'])
          ]) ]),
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
      title:'Crear novedad',
      message:'Completa la informacion para crear una novedad.',
      confirmText:'Crear novedad',
      fields:[
        { id:'codeRef', label:'Codigo novedad', type:'text', required:true, placeholder:'Codigo de la novedad' },
        { id:'name', label:'Nombre', type:'text', required:true, placeholder:'Nombre de la novedad' },
        { id:'reemplazo', label:'Reemplazo', type:'select', required:true, options:[{value:'',label:'Seleccione...'},{value:'si',label:'SI'},{value:'no',label:'NO'}] },
        { id:'nomina', label:'Nomina', type:'select', required:true, options:[{value:'',label:'Seleccione...'},{value:'si',label:'SI'},{value:'no',label:'NO'}] }
      ]
    });
    if(!modal.confirmed) return;
    const codeRef=String(modal.values.codeRef||'').trim();
    const name=String(modal.values.name||'').trim();
    const reemplazo=String(modal.values.reemplazo||'').trim();
    const nomina=String(modal.values.nomina||'').trim();
    if(!codeRef){ alert('Escribe el codigo de novedad.'); return; }
    if(!name){ alert('Escribe el nombre de la novedad.'); return; }
    if(!reemplazo){ alert('Selecciona reemplazo.'); return; }
    if(!nomina){ alert('Selecciona nomina.'); return; }
    try{
      const dupRef=await deps.findNovedadByCodigoNovedad?.(codeRef);
      if(dupRef){ alert('Ya existe una novedad con ese codigo de novedad.'); return; }
      const code=await deps.getNextNovedadCode?.();
      const id=await deps.createNovedad?.({ codigo:code, codigoNovedad:codeRef, nombre:name, reemplazo, nomina });
      await deps.addAuditLog?.({ targetType:'novedad', targetId:id, action:'create_novedad', after:{ codigo:code, codigoNovedad:codeRef, nombre:name, reemplazo, nomina, estado:'activo' } });
      alert('Novedad creada OK');
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  const btnOpenCreate=el('button',{id:'btnOpenCreate',className:'btn btn--primary right',type:'button'},['Crear novedad']);
  qs('#tabList .form-row',ui)?.append(btnOpenCreate);
  btnOpenCreate.addEventListener('click',openCreateModal);

  qs('#btnCreate',ui).addEventListener('click',async()=>{
    const codeRef=qs('#nCodeRef',ui).value.trim();
    const name=qs('#nName',ui).value.trim();
    const reemplazo=qs('#nReemplazo',ui).value;
    const nomina=qs('#nNomina',ui).value;
    const msg=qs('#msgCreate',ui); msg.textContent=' ';
    if(!codeRef){ msg.textContent='Escribe el codigo de novedad.'; return; }
    if(!name){ msg.textContent='Escribe el nombre de la novedad.'; return; }
    if(!reemplazo){ msg.textContent='Selecciona reemplazo.'; return; }
    if(!nomina){ msg.textContent='Selecciona nomina.'; return; }
    try{
      const dupRef=await deps.findNovedadByCodigoNovedad?.(codeRef);
      if(dupRef) { msg.textContent='Ya existe una novedad con ese codigo de novedad.'; return; }
      const code=await deps.getNextNovedadCode?.();
      const id=await deps.createNovedad?.({ codigo:code, codigoNovedad:codeRef, nombre:name, reemplazo, nomina });
      await deps.addAuditLog?.({ targetType:'novedad', targetId:id, action:'create_novedad', after:{ codigo:code, codigoNovedad:codeRef, nombre:name, reemplazo, nomina, estado:'activo' } });
      qs('#nCodeRef',ui).value=''; qs('#nName',ui).value=''; qs('#nReemplazo',ui).value=''; qs('#nNomina',ui).value=''; msg.textContent='Novedad creada OK'; setTab('list'); setTimeout(()=> msg.textContent=' ',1200);
    }catch(e){ msg.textContent='Error: '+(e?.message||e); }
  });

  let snapshot=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'novedades',after:'#tabList .table-wrap',onChange:render});
  const search=()=> qs('#txtSearch',ui).value.trim().toLowerCase();
  const filterStatus=()=> qs('#selStatus',ui).value;
  const filterReemp=()=> qs('#selReemp',ui).value;
  const filterNomina=()=> qs('#selNomina',ui).value;
  function sortVal(n,key){ if(key==='createdAt'){ try{ const x=n.createdAt?.toDate?n.createdAt.toDate(): (n.createdAt?new Date(n.createdAt):null); return x?x.getTime():0; }catch{return 0;} } return String(n[key]??'').toLowerCase(); }
  function sortData(data){ if(!sortKey) return data; const out=[...data]; out.sort((a,b)=>{ const va=sortVal(a,sortKey); const vb=sortVal(b,sortKey); if(va===vb) return 0; return va>vb?sortDir:-sortDir; }); return out; }
  function updateSortIndicators(){ ui.querySelectorAll('th[data-sort]').forEach((th)=>{ const base=th.dataset.baseLabel||th.textContent.replace(/\s[\^v▲▼]$/,''); th.dataset.baseLabel=base; const key=th.getAttribute('data-sort'); th.textContent=(sortKey===key)?`${base} ${sortDir===1?'▲':'▼'}`:base; }); }
  function initSorting(){ ui.querySelectorAll('th[data-sort]').forEach((th)=> th.addEventListener('click',()=>{ const key=th.getAttribute('data-sort'); if(sortKey===key) sortDir=sortDir*-1; else { sortKey=key; sortDir=1; } paginator.reset(); render(); })); }
  function render(){
    const term=search(); const st=filterStatus(); const re=filterReemp(); const no=filterNomina();
    const data=snapshot.filter(n=>{
      const matchesText=!term || (n.codigo||'').toLowerCase().includes(term) || (n.codigoNovedad||'').toLowerCase().includes(term) || (n.nombre||'').toLowerCase().includes(term);
      const matchesStatus=!st || n.estado===st;
      const matchesReemp=!re || n.reemplazo===re;
      const matchesNomina=!no || n.nomina===no;
      return matchesText && matchesStatus && matchesReemp && matchesNomina;
    });
    const sorted=sortData(data);
    const pageRows=paginator.slice(sorted);
    tbody.replaceChildren(...pageRows.map(n=> row(n)));
    const msg=qs('#msg',ui); if(msg) msg.textContent=`Total registros filtrados: ${data.length}`;
    updateSortIndicators();
  }
  function row(n){
    const tr=el('tr',{'data-id':n.id});
    const tdCodigo=el('td',{},[n.codigo||'-']);
    const tdCodeRef=el('td',{},[n.codigoNovedad||'-']);
    const tdNombre=el('td',{},[n.nombre||'-']);
    const tdReemp=el('td',{},[ (n.reemplazo||'').toUpperCase() || '-' ]);
    const tdNomina=el('td',{},[ (n.nomina||'').toUpperCase() || '-' ]);
    const tdEstado=el('td',{},[ statusBadge(n.estado) ]);
    const tdAcc=el('td',{},[ actionsCell(n) ]);
    tr.append(tdCodigo,tdCodeRef,tdNombre,tdReemp,tdNomina,tdEstado,tdAcc);
    return tr;
  }
  function statusBadge(st){ return el('span',{className:'badge '+(st==='activo'?'badge--ok':'badge--off')},[st||'-']); }
  function formatDate(ts){ try{ const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null); return d? new Date(d).toLocaleString(): '-'; }catch{ return '-'; } }
  function auditInfoData(n){
    const hasMod = Boolean(n.lastModifiedAt || n.lastModifiedByEmail || n.lastModifiedByUid);
    return {
      action: hasMod ? 'Ultima modificacion' : 'Creacion',
      user: hasMod ? (n.lastModifiedByEmail||n.lastModifiedByUid||'-') : (n.createdByEmail||n.createdByUid||'-'),
      date: hasMod ? formatDate(n.lastModifiedAt) : formatDate(n.createdAt)
    };
  }
  function actionsCell(n){
    const box=el('div',{className:'row-actions'},[]);
    const btnEdit=el('button',{className:'btn btn--icon',title:'Editar'},['\u270E']);
    btnEdit.addEventListener('click',()=>{ const tr=tbody.querySelector(`tr[data-id="${n.id}"]`); if(tr) startEdit(tr,n); });
    const btnToggle=el('button',{className:'btn btn--icon '+(n.estado==='activo'?'btn--danger':'' ),title:n.estado==='activo'?'Desactivar':'Activar','aria-label':n.estado==='activo'?'Desactivar':'Activar'},[ n.estado==='activo'?'\u23FB':'\u21BA' ]);
    btnToggle.addEventListener('click',async()=>{
      const target=n.estado==='activo'?'inactivo':'activo';
      const modal=await showActionModal({
        title:`${target==='inactivo'?'Desactivar':'Activar'} novedad`,
        message:`Novedad: ${n.nombre||'-'}`,
        confirmText:target==='inactivo'?'Desactivar':'Activar',
        fields:[{ id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Escribe el motivo o detalle de esta accion' }]
      });
      if(!modal.confirmed) return;
      try{ await deps.setNovedadStatus?.(n.id,target); await deps.addAuditLog?.({ targetType:'novedad', targetId:n.id, action: target==='activo'?'activate_novedad':'deactivate_novedad', before:{estado:n.estado}, after:{estado:target}, note: modal.values.detail||null }); }catch(e){ alert('Error: '+(e?.message||e)); }
    });
    const btnInfo=el('button',{className:'btn btn--icon',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']);
    btnInfo.addEventListener('click',()=>{ const info=auditInfoData(n); showInfoModal('Informacion del registro',[`Evento: ${info.action}`,`Usuario: ${info.user}`,`Fecha: ${info.date}`]); });
    box.append(btnEdit,btnToggle,btnInfo); return box;
  }
  function startEdit(tr,n){
    const cur={ codigo:n.codigo||'', codigoNovedad:n.codigoNovedad||'', nombre:n.nombre||'', reemplazo:n.reemplazo||'', nomina:n.nomina||'' };
    const tds=tr.querySelectorAll('td');
    tds[0].replaceChildren(el('input',{className:'input',value:cur.codigo,style:'max-width:140px'}));
    tds[1].replaceChildren(el('input',{className:'input',value:cur.codigoNovedad,style:'max-width:160px'}));
    tds[2].replaceChildren(el('input',{className:'input',value:cur.nombre,style:'max-width:260px'}));
    tds[3].replaceChildren(el('select',{className:'select'},[
      el('option',{value:'si', selected:cur.reemplazo==='si'},['SI']),
      el('option',{value:'no', selected:cur.reemplazo==='no'},['NO'])
    ]));
    tds[4].replaceChildren(el('select',{className:'select'},[
      el('option',{value:'si', selected:cur.nomina==='si'},['SI']),
      el('option',{value:'no', selected:cur.nomina==='no'},['NO'])
    ]));
    tds[5].replaceChildren(statusBadge(n.estado));
    const box=el('div',{className:'row-actions'},[]);
    const btnSave=el('button',{className:'btn btn--primary'},['Guardar']);
    const btnCancel=el('button',{className:'btn'},['Cancelar']);
    btnSave.addEventListener('click',async()=>{
      const newCode=tds[0].querySelector('input').value.trim();
      const newCodeRef=tds[1].querySelector('input').value.trim();
      const newName=tds[2].querySelector('input').value.trim();
      const newReemp=tds[3].querySelector('select').value;
      const newNomina=tds[4].querySelector('select').value;
      if(!newCode||!newCodeRef||!newName) return alert('Completa codigo, codigo novedad y nombre.');
      if(!newReemp) return alert('Selecciona reemplazo.');
      if(!newNomina) return alert('Selecciona nomina.');
      const modal=await showActionModal({
        title:'Confirmar modificacion',
        message:`Novedad: ${n.nombre||'-'}`,
        confirmText:'Guardar cambios',
        fields:[{ id:'detail', label:'Detalle de la modificacion', type:'textarea', required:true, placeholder:'Describe brevemente el cambio realizado' }]
      });
      if(!modal.confirmed) return;
      try{
        if(newCode!==n.codigo){ const dup=await deps.findNovedadByCode?.(newCode); if(dup && dup.id!==n.id) return alert('Ya existe una novedad con ese codigo.'); }
        if(newCodeRef!==n.codigoNovedad){ const dupRef=await deps.findNovedadByCodigoNovedad?.(newCodeRef); if(dupRef && dupRef.id!==n.id) return alert('Ya existe una novedad con ese codigo de novedad.'); }
        await deps.updateNovedad?.(n.id,{ codigo:newCode, codigoNovedad:newCodeRef, nombre:newName, reemplazo:newReemp, nomina:newNomina });
        await deps.addAuditLog?.({ targetType:'novedad', targetId:n.id, action:'update_novedad', before:{ codigo:n.codigo||null, codigoNovedad:n.codigoNovedad||null, nombre:n.nombre||null, reemplazo:n.reemplazo||null, nomina:n.nomina||null }, after:{ codigo:newCode||null, codigoNovedad:newCodeRef||null, nombre:newName||null, reemplazo:newReemp||null, nomina:newNomina||null }, note: modal.values.detail||null });
      }catch(e){ alert('Error: '+(e?.message||e)); }
    });
    btnCancel.addEventListener('click',()=> render());
    box.append(btnSave,btnCancel); tds[6].replaceChildren(box);
  }
  const un=deps.streamNovedades?.((arr)=>{ snapshot=arr||[]; render(); });
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); });
  qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  qs('#selReemp',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  qs('#selNomina',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui);
  return ()=> un?.();
};
