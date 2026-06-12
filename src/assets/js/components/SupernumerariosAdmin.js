import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
export const SupernumerariosAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Supernumerarios']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'eCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Documento']), el('input',{id:'eDoc',className:'input',placeholder:'Documento del supernumerario'}) ]),
        el('div',{},[ el('label',{className:'label'},['Nombre completo']), el('input',{id:'eName',className:'input',placeholder:'Nombre completo'}) ]),
        el('div',{},[ el('label',{className:'label'},['Telefono']), el('input',{id:'ePhone',className:'input',placeholder:'Telefono'}) ]),
        el('div',{},[ el('label',{className:'label'},['Cargo']), el('select',{id:'eCargo',className:'select'},[]) ]),
        el('div',{},[ el('label',{className:'label'},['Sede (buscar)']), el('input',{id:'eSedeSearch',className:'input',list:'eSedeList',placeholder:'Nombre o codigo de sede'}) ]),
        el('div',{},[ el('label',{className:'label'},['Fecha ingreso']), el('input',{id:'eIngreso',className:'input',type:'date'}) ])
      ]),
      el('datalist',{id:'eSedeList'},[])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo, documento, nombre o sede...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Estado']), el('select',{id:'selStatus',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'activo'},['Activos']), el('option',{value:'inactivo'},['Inactivos']) ]) ])
      ]),
      el('div',{className:'mt-2 table-wrap'},[
        el('table',{className:'table',id:'tbl'},[
          el('thead',{},[ el('tr',{},[
            el('th',{'data-sort':'codigo',style:'cursor:pointer'},['Codigo']),
            el('th',{'data-sort':'documento',style:'cursor:pointer'},['Documento']),
            el('th',{'data-sort':'nombre',style:'cursor:pointer'},['Nombre']),
            el('th',{'data-sort':'telefono',style:'cursor:pointer'},['Telefono']),
            el('th',{'data-sort':'cargoNombre',style:'cursor:pointer'},['Cargo']),
            el('th',{'data-sort':'sedeNombre',style:'cursor:pointer'},['Sede']),
            el('th',{'data-sort':'estado',style:'cursor:pointer'},['Estado']),
            el('th',{'data-sort':'fechaIngreso',style:'cursor:pointer'},['Ingreso']),
            el('th',{'data-sort':'fechaRetiro',style:'cursor:pointer'},['Retiro']),
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
  setTab('list');
  tabListBtn.addEventListener('click',()=> setTab('list'));

  let sedeList=[]; let cargoList=[];
  const sedeInput=qs('#eSedeSearch',ui); const sedeListNode=qs('#eSedeList',ui); const cargoSelect=qs('#eCargo',ui);
  function buildOptions(items, selected){
    const opts=[ el('option',{value:''},['Seleccione...']) ];
    items.forEach((item)=>{
      const code=item.codigo||''; const label=item.nombre||code||'-';
      opts.push(el('option',{value:code, selected: code && code===selected},[ `${label} (${code||'-'})` ]));
    });
    return opts;
  }
  function sedeLabelByCode(code){
    const sede=sedeList.find(s=>s.codigo===code);
    return sede ? `${sede.nombre||sede.codigo} (${sede.codigo||'-'})` : '';
  }
  function renderSedeSelect(){
    const opts=sedeList
      .map((s)=> sedeLabelByCode(s.codigo))
      .filter((v, i, arr)=> v && arr.indexOf(v)===i)
      .map((value)=> el('option',{value}));
    sedeListNode.replaceChildren(...opts);
  }
  function resolveSedeCode(inputValue){
    const raw=String(inputValue||'').trim();
    if(!raw) return '';
    const byCode=sedeList.find(s=> String(s.codigo||'').toLowerCase()===raw.toLowerCase());
    if(byCode) return byCode.codigo;
    const match=raw.match(/\(([^)]+)\)\s*$/);
    if(match){
      const code=match[1].trim();
      const byLabelCode=sedeList.find(s=> String(s.codigo||'').toLowerCase()===code.toLowerCase());
      if(byLabelCode) return byLabelCode.codigo;
    }
    const byName=sedeList.find(s=> String(s.nombre||'').toLowerCase()===raw.toLowerCase());
    return byName?.codigo||'';
  }
  function renderCargoSelect(){
    const cur=cargoSelect.value;
    cargoSelect.replaceChildren(...buildOptions(cargoList,cur));
  }
  let snapshot=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'supernumerarios',after:'#tabList .table-wrap',onChange:render});
  let unSedes=()=>{};
  let unCargos=()=>{};
  let unEmp=()=>{};
  let employees=[];
  const sedeNameByCode=(code)=> sedeList.find(s=>s.codigo===code)?.nombre || '-';
  const cargoNameByCode=(code)=> cargoList.find(c=>c.codigo===code)?.nombre || '-';
  const isLinkedByDoc=(doc)=>{
    const d=String(doc||'').trim();
    if(!d) return false;
    return employees.some((e)=> e.estado!=='inactivo' && String(e.documento||'').trim()===d);
  };
  const linkedEmployeeByDoc=(doc)=>{
    const d=String(doc||'').trim();
    if(!d) return null;
    return employees.find((e)=> String(e.documento||'').trim()===d) || null;
  };
  const shouldHideInComplementaryView=(row)=>{
    const linked=linkedEmployeeByDoc(row?.documento);
    if(!linked) return false;
    if(String(linked.estado||'').trim().toLowerCase()==='inactivo') return true;
    return row?.estado==='inactivo' && isLinkedByDoc(row?.documento);
  };

  const search=()=> qs('#txtSearch',ui).value.trim().toLowerCase();
  const filterStatus=()=> qs('#selStatus',ui).value;
  function toSortableDate(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      return d? d.getTime(): 0;
    }catch{ return 0; }
  }
  function getSortValue(e,key){
    if(key==='cargoNombre') return (e.cargoNombre||cargoNameByCode(e.cargoCodigo)||'').toLowerCase();
    if(key==='sedeNombre') return (e.sedeNombre||sedeNameByCode(e.sedeCodigo)||'').toLowerCase();
    if(key==='fechaIngreso' || key==='fechaRetiro') return toSortableDate(e[key]);
    return String(e[key]??'').toLowerCase();
  }
  function sortData(data){
    if(!sortKey) return data;
    const out=[...data];
    out.sort((a,b)=>{
      const va=getSortValue(a,sortKey); const vb=getSortValue(b,sortKey);
      if(va===vb) return 0;
      return va>vb ? sortDir : -sortDir;
    });
    return out;
  }
  function updateSortIndicators(){
    ui.querySelectorAll('th[data-sort]').forEach((th)=>{
      const base=th.dataset.baseLabel||th.textContent.replace(/\s[\^v▲▼]$/,'');
      th.dataset.baseLabel=base;
      const key=th.getAttribute('data-sort');
      th.textContent=(sortKey===key)?`${base} ${sortDir===1?'▲':'▼'}`:base;
    });
  }
  function initSorting(){
    ui.querySelectorAll('th[data-sort]').forEach((th)=>{
      th.addEventListener('click',()=>{
        const key=th.getAttribute('data-sort');
        if(sortKey===key) sortDir=sortDir*-1; else { sortKey=key; sortDir=1; }
        paginator.reset();
        render();
      });
    });
  }
  function render(){
    const term=search(); const st=filterStatus();
    const data=snapshot.filter(e=>{
      if(shouldHideInComplementaryView(e)) return false;
      const text=[e.codigo,e.documento,e.nombre,e.cargoNombre,cargoNameByCode(e.cargoCodigo),e.sedeNombre,sedeNameByCode(e.sedeCodigo)].join(' ').toLowerCase();
      return (!term || text.includes(term)) && (!st || e.estado===st);
    });
    const sorted=sortData(data);
    const pageRows=paginator.slice(sorted);
    tbody.replaceChildren(...pageRows.map(e=> row(e)));
    const msg=qs('#msg',ui); if(msg) msg.textContent=`Total registros filtrados: ${data.length}`;
    updateSortIndicators();
  }
  function row(e){
    const tr=el('tr',{'data-id':e.id});
    const tdCodigo=el('td',{},[e.codigo||'-']);
    const linked=isLinkedByDoc(e.documento);
    const tdDoc=el('td',{}, linked ? [e.documento||'-',' ',el('span',{className:'badge'},['Vinculado'])] : [e.documento||'-']);
    const tdNombre=el('td',{},[e.nombre||'-']);
    const tdTel=el('td',{},[e.telefono||'-']);
    const tdCargo=el('td',{},[ e.cargoNombre||cargoNameByCode(e.cargoCodigo) ]);
    const tdSede=el('td',{},[ e.sedeNombre||sedeNameByCode(e.sedeCodigo) ]);
    const tdEstado=el('td',{},[ statusBadge(e.estado) ]);
    const tdIngreso=el('td',{},[ formatDate(e.fechaIngreso) ]);
    const tdRetiro=el('td',{},[ formatDate(e.fechaRetiro) ]);
    const tdAcc=el('td',{},[ actionsCell(e) ]);
    tr.append(tdCodigo,tdDoc,tdNombre,tdTel,tdCargo,tdSede,tdEstado,tdIngreso,tdRetiro,tdAcc);
    return tr;
  }
  function statusBadge(st){ return el('span',{className:'badge '+(st==='activo'?'badge--ok':'badge--off')},[st||'-']); }
  function formatDate(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      return d? new Date(d).toLocaleDateString(): '-';
    }catch{ return '-'; }
  }
  function formatDateTime(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      return d? new Date(d).toLocaleString(): '-';
    }catch{ return '-'; }
  }
  function auditInfoData(e){
    const hasMod = Boolean(e.lastModifiedAt || e.lastModifiedByEmail || e.lastModifiedByUid);
    return {
      action: hasMod ? 'Ultima modificacion' : 'Creacion',
      user: hasMod ? (e.lastModifiedByEmail||e.lastModifiedByUid||'-') : (e.createdByEmail||e.createdByUid||'-'),
      date: hasMod ? formatDateTime(e.lastModifiedAt) : formatDateTime(e.createdAt)
    };
  }
  function actionsCell(e){
    const box=el('div',{className:'row-actions'},[]);
    const btnInfo=el('button',{className:'btn btn--icon',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']);
    btnInfo.addEventListener('click',()=>{ const info=auditInfoData(e); showInfoModal('Informacion del registro',[`Evento: ${info.action}`,`Usuario: ${info.user}`,`Fecha: ${info.date}`]); });
    box.append(btnInfo); return box;
  }
  function startEdit(tr,e){
    const cur={
      codigo:e.codigo||'',
      documento:e.documento||'',
      nombre:e.nombre||'',
      telefono:e.telefono||'',
      cargoCodigo:e.cargoCodigo||'',
      sedeCodigo:e.sedeCodigo||'',
      fechaIngreso: toInputDate(e.fechaIngreso),
      fechaRetiro: toInputDate(e.fechaRetiro)
    };
    const tds=tr.querySelectorAll('td');
    tds[0].replaceChildren(el('input',{className:'input',value:cur.codigo,style:'max-width:140px'}));
    tds[1].replaceChildren(el('input',{className:'input',value:cur.documento,style:'max-width:160px'}));
    tds[2].replaceChildren(el('input',{className:'input',value:cur.nombre,style:'max-width:220px'}));
    tds[3].replaceChildren(el('input',{className:'input',value:cur.telefono,style:'max-width:140px'}));
    tds[4].replaceChildren(el('select',{className:'select'},buildOptions(cargoList,cur.cargoCodigo)));
    tds[5].replaceChildren(el('input',{className:'input',list:'eSedeList',value:sedeLabelByCode(cur.sedeCodigo),style:'max-width:240px'}));
    tds[6].replaceChildren(statusBadge(e.estado));
    tds[7].replaceChildren(el('input',{className:'input',type:'date',value:cur.fechaIngreso||''}));
    tds[8].replaceChildren(el('input',{className:'input',type:'date',value:cur.fechaRetiro||''}));
    const box=el('div',{className:'row-actions'},[]);
    const btnSave=el('button',{className:'btn btn--primary'},['Guardar']);
    const btnCancel=el('button',{className:'btn'},['Cancelar']);
    btnSave.addEventListener('click',async()=>{
      const newCode=tds[0].querySelector('input').value.trim();
      const newDoc=tds[1].querySelector('input').value.trim();
      const newName=tds[2].querySelector('input').value.trim();
      const newPhone=tds[3].querySelector('input').value.trim();
      const newCargoCode=tds[4].querySelector('select').value;
      const newSedeCode=resolveSedeCode(tds[5].querySelector('input').value);
      const newIngreso=tds[7].querySelector('input').value.trim();
      const newRetiro=tds[8].querySelector('input').value.trim();
      if(!newCode||!newDoc||!newName||!newPhone) return alert('Completa codigo, documento, nombre y telefono.');
      if(!newCargoCode) return alert('Selecciona un cargo.');
      if(!newSedeCode) return alert('Selecciona una sede.');
      if(!newIngreso) return alert('Selecciona la fecha de ingreso.');
      if(e.estado==='inactivo' && !newRetiro) return alert('Para supernumerarios inactivos, la fecha de retiro es obligatoria.');
      const modal=await showActionModal({
        title:'Confirmar modificacion',
        message:`Supernumerario: ${e.nombre||'-'}`,
        confirmText:'Guardar cambios',
        fields:[{ id:'detail', label:'Detalle de la modificacion', type:'textarea', required:true, placeholder:'Describe brevemente el cambio realizado' }]
      });
      if(!modal.confirmed) return;
      try{
        if(newCode!==e.codigo){ const dup=await deps.findSupernumerarioByCode?.(newCode); if(dup && dup.id!==e.id) return alert('Ya existe un supernumerario con ese codigo.'); }
        if(newDoc!==e.documento){ const dupDoc=await deps.findSupernumerarioByDocument?.(newDoc); if(dupDoc && dupDoc.id!==e.id) return alert('Ya existe un supernumerario con ese documento.'); }
        const newCargo=cargoList.find(c=>c.codigo===newCargoCode);
        const newSede=sedeList.find(s=>s.codigo===newSedeCode);
        await deps.updateSupernumerario?.(e.id,{
          codigo:newCode,
          documento:newDoc,
          nombre:newName,
          telefono:newPhone,
          cargoCodigo:newCargoCode,
          cargoNombre:newCargo?.nombre||null,
          sedeCodigo:newSedeCode,
          sedeNombre:newSede?.nombre||null,
          fechaIngreso: new Date(`${newIngreso}T00:00:00`),
          fechaRetiro: newRetiro ? new Date(`${newRetiro}T00:00:00`) : null
        });
        await deps.addAuditLog?.({ targetType:'supernumerario', targetId:e.id, action:'update_supernumerario', before:{ codigo:e.codigo, documento:e.documento, nombre:e.nombre, sedeCodigo:e.sedeCodigo, fechaRetiro:e.fechaRetiro||null }, after:{ codigo:newCode, documento:newDoc, nombre:newName, sedeCodigo:newSedeCode, fechaRetiro:newRetiro||null }, note: modal.values.detail||null });
      }catch(err){ alert('Error: '+(err?.message||err)); }
    });
    btnCancel.addEventListener('click',()=> render());
    box.append(btnSave,btnCancel); tds[9].replaceChildren(box);
  }
  function toInputDate(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      if(!d) return '';
      const pad=(n)=> String(n).padStart(2,'0');
      return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
    }catch{ return ''; }
  }
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); });
  qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui);
  let un=()=>{};
  try{
    unSedes=deps.streamSedes?.((arr)=>{ sedeList=(arr||[]).filter(s=>s.estado!=='inactivo'); renderSedeSelect(); render(); }) || (()=>{});
    unCargos=deps.streamCargos?.((arr)=>{ cargoList=(arr||[]).filter(c=>c.estado!=='inactivo'); renderCargoSelect(); render(); }) || (()=>{});
    unEmp=deps.streamEmployees?.((arr)=>{ employees=arr||[]; render(); }) || (()=>{});
    un=deps.streamSupernumerarios?.((arr)=>{ snapshot=arr||[]; render(); }) || (()=>{});
  }catch(e){
    const msg=qs('#msg',ui); if(msg) msg.textContent='Error cargando supernumerarios: '+(e?.message||e);
  }
  return ()=>{ un?.(); unSedes?.(); unCargos?.(); unEmp?.(); };
};

