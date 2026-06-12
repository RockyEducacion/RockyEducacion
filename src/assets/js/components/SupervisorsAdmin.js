import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
export const SupervisorsAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Supervisores']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'sCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Documento']), el('input',{id:'sDoc',className:'input',placeholder:'Documento del supervisor'}) ]),
        el('div',{},[ el('label',{className:'label'},['Nombre']), el('input',{id:'sName',className:'input',placeholder:'Nombre del supervisor'}) ]),
        el('div',{},[ el('label',{className:'label'},['Zona']), el('select',{id:'sZone',className:'select'},[]) ]),
        el('div',{},[ el('label',{className:'label'},['Fecha ingreso']), el('input',{id:'sIngreso',className:'input',type:'date'}) ])
      ])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo, documento, nombre o zona...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Estado']), el('select',{id:'selStatus',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'activo'},['Activos']), el('option',{value:'inactivo'},['Inactivos']) ]) ])
      ]),
      el('div',{className:'mt-2 table-wrap'},[
        el('table',{className:'table',id:'tbl'},[
          el('thead',{},[ el('tr',{},[
            el('th',{'data-sort':'codigo',style:'cursor:pointer'},['Codigo']),
            el('th',{'data-sort':'documento',style:'cursor:pointer'},['Documento']),
            el('th',{'data-sort':'nombre',style:'cursor:pointer'},['Nombre']),
            el('th',{'data-sort':'zonaNombre',style:'cursor:pointer'},['Zona']),
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

  let zoneList=[];
  const zoneSelect=qs('#sZone',ui);
  function buildOptions(items, selected){
    const opts=[ el('option',{value:''},['Seleccione...']) ];
    items.forEach((item)=>{
      const code=item.codigo||''; const label=item.nombre||code||'-';
      opts.push(el('option',{value:code, selected: code && code===selected},[ `${label} (${code||'-'})` ]));
    });
    return opts;
  }
  function renderZoneSelect(){
    const cur=zoneSelect.value;
    zoneSelect.replaceChildren(...buildOptions(zoneList,cur));
  }
  let snapshot=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'supervisors',after:'#tabList .table-wrap',onChange:render});
  let unZones=()=>{};
  let unEmp=()=>{};
  let employees=[];
  const zoneNameByCode=(code)=> zoneList.find(z=>z.codigo===code)?.nombre || '-';
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
  function toDate(ts){ try{ const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null); return d? d.getTime():0; }catch{ return 0; } }
  function sortVal(s,key){ if(key==='zonaNombre') return (s.zonaNombre||zoneNameByCode(s.zonaCodigo)||'').toLowerCase(); if(key==='fechaIngreso'||key==='fechaRetiro') return toDate(s[key]); return String(s[key]??'').toLowerCase(); }
  function sortData(data){ if(!sortKey) return data; const out=[...data]; out.sort((a,b)=>{ const va=sortVal(a,sortKey); const vb=sortVal(b,sortKey); if(va===vb) return 0; return va>vb?sortDir:-sortDir; }); return out; }
  function updateSortIndicators(){ ui.querySelectorAll('th[data-sort]').forEach((th)=>{ const base=th.dataset.baseLabel||th.textContent.replace(/\s[\^v▲▼]$/,''); th.dataset.baseLabel=base; const key=th.getAttribute('data-sort'); th.textContent=(sortKey===key)?`${base} ${sortDir===1?'▲':'▼'}`:base; }); }
  function initSorting(){ ui.querySelectorAll('th[data-sort]').forEach((th)=> th.addEventListener('click',()=>{ const key=th.getAttribute('data-sort'); if(sortKey===key) sortDir=sortDir*-1; else { sortKey=key; sortDir=1; } paginator.reset(); render(); })); }
  function render(){
    const term=search(); const st=filterStatus();
    const data=snapshot.filter(s=>{
      if(shouldHideInComplementaryView(s)) return false;
      const text=[s.codigo,s.documento,s.nombre,s.zonaNombre,zoneNameByCode(s.zonaCodigo)].join(' ').toLowerCase();
      return (!term || text.includes(term)) && (!st || s.estado===st);
    });
    const sorted=sortData(data);
    const pageRows=paginator.slice(sorted);
    tbody.replaceChildren(...pageRows.map(s=> row(s)));
    const msg=qs('#msg',ui); if(msg) msg.textContent=`Total registros filtrados: ${data.length}`;
    updateSortIndicators();
  }
  function row(s){
    const tr=el('tr',{'data-id':s.id});
    const tdCodigo=el('td',{},[s.codigo||'-']);
    const linked=isLinkedByDoc(s.documento);
    const tdDoc=el('td',{}, linked ? [s.documento||'-',' ',el('span',{className:'badge'},['Vinculado'])] : [s.documento||'-']);
    const tdNombre=el('td',{},[s.nombre||'-']);
    const tdZona=el('td',{},[ s.zonaNombre||zoneNameByCode(s.zonaCodigo) ]);
    const tdEstado=el('td',{},[ statusBadge(s.estado) ]);
    const tdIngreso=el('td',{},[ formatDate(s.fechaIngreso) ]);
    const tdRetiro=el('td',{},[ formatDate(s.fechaRetiro) ]);
    const tdAcc=el('td',{},[ actionsCell(s) ]);
    tr.append(tdCodigo,tdDoc,tdNombre,tdZona,tdEstado,tdIngreso,tdRetiro,tdAcc);
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
  function auditInfoData(s){
    const hasMod = Boolean(s.lastModifiedAt || s.lastModifiedByEmail || s.lastModifiedByUid);
    return {
      action: hasMod ? 'Ultima modificacion' : 'Creacion',
      user: hasMod ? (s.lastModifiedByEmail||s.lastModifiedByUid||'-') : (s.createdByEmail||s.createdByUid||'-'),
      date: hasMod ? formatDateTime(s.lastModifiedAt) : formatDateTime(s.createdAt)
    };
  }
  function actionsCell(s){
    const box=el('div',{className:'row-actions'},[]);
    const btnEditZone=el('button',{className:'btn btn--icon',type:'button',title:'Editar zona','aria-label':'Editar zona'},['\u270E']);
    btnEditZone.addEventListener('click',()=> startEditZone(s));
    const btnInfo=el('button',{className:'btn btn--icon',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']);
    btnInfo.addEventListener('click',()=>{ const info=auditInfoData(s); showInfoModal('Informacion del registro',[`Evento: ${info.action}`,`Usuario: ${info.user}`,`Fecha: ${info.date}`]); });
    box.append(btnEditZone,btnInfo); return box;
  }
  async function startEditZone(s){
    const zoneCode=String(s.zonaCodigo||'').trim();
    const zoneName=String(s.zonaNombre||zoneNameByCode(zoneCode)||'-').trim()||'-';
    const editable=(zoneList||[]).map((z)=> ({ id:String(z.codigo||''), name:String(z.nombre||z.codigo||'').trim()||String(z.codigo||'') }))
      .filter((z)=> z.id);
    if(!editable.length){ alert('No hay zonas activas disponibles.'); return; }
    const modal=await showActionModal({
      title:'Editar zona del supervisor',
      message:`Supervisor: ${s.nombre||'-'}\nZona actual: ${zoneName} (${zoneCode||'-'})`,
      confirmText:'Guardar zona',
      fields:[
        { id:'zonaCodigo', label:'Nueva zona', type:'select', required:true, options:editable.map((z)=> ({ value:z.id, label:`${z.name} (${z.id})` })), value:zoneCode },
        { id:'detail', label:'Detalle del cambio', type:'textarea', required:true, placeholder:'Describe por que cambia la zona' }
      ]
    });
    if(!modal.confirmed) return;
    const newZoneCode=String(modal.values?.zonaCodigo||'').trim();
    if(!newZoneCode) return;
    if(newZoneCode===zoneCode) return;
    const newZone=zoneList.find((z)=> String(z.codigo||'')===newZoneCode);
    if(!newZone){ alert('La zona seleccionada no es valida.'); return; }
    try{
      await deps.updateSupervisor?.(s.id,{
        zonaCodigo:newZoneCode,
        zonaNombre:newZone?.nombre||null
      });
      await deps.addAuditLog?.({
        targetType:'supervisor',
        targetId:s.id,
        action:'update_supervisor_zone',
        before:{ zonaCodigo:zoneCode||null, zonaNombre:s.zonaNombre||zoneName||null },
        after:{ zonaCodigo:newZoneCode, zonaNombre:newZone?.nombre||null },
        note: modal.values?.detail||null
      });
    }catch(err){
      alert('Error: '+(err?.message||err));
    }
  }
  unZones=deps.streamZones?.((arr)=>{ zoneList=(arr||[]).filter(z=>z.estado!=='inactivo'); renderZoneSelect(); render(); }) || (()=>{});
  unEmp=deps.streamEmployees?.((arr)=>{ employees=arr||[]; render(); }) || (()=>{});
  const un=deps.streamSupervisors?.((arr)=>{ snapshot=arr||[]; render(); });
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); });
  qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui);
  return ()=>{ un?.(); unZones?.(); unEmp?.(); };
};
