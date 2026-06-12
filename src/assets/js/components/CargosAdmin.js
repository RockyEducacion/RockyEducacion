import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
export const CargosAdmin=(mount,deps={})=>{
  const crudOptions=[
    { value:'empleado', label:'Solo Empleados' },
    { value:'supervisor', label:'Supervisor' },
    { value:'supernumerario', label:'Supernumerario' }
  ];
  const crudLabel=(value)=> crudOptions.find((o)=>o.value===value)?.label || 'Solo Empleados';
  const crudSelect=(selected='empleado')=> el('select',{className:'select'},[
    ...crudOptions.map((o)=> el('option',{ value:o.value, selected:o.value===selected },[o.label]))
  ]);
  const parseSalary=(value)=>{
    const raw=String(value??'').trim();
    if(!raw) return null;
    const normalized=raw.replace(/[^\d.,-]/g,'').replace(/\./g,'').replace(',','.');
    const salary=Number(normalized);
    return Number.isFinite(salary)&&salary>=0?salary:NaN;
  };
  const salaryInputValue=(value)=>{
    if(value==null||value==='') return '';
    const salary=Number(value);
    return Number.isFinite(salary)?String(salary):'';
  };
  const formatSalary=(value)=>{
    if(value==null||value==='') return '-';
    const salary=Number(value);
    return Number.isFinite(salary)?salary.toLocaleString('es-CO',{style:'currency',currency:'COP',maximumFractionDigits:0}):'-';
  };
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Cargos']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'cCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Cargo']), el('input',{id:'cName',className:'input',placeholder:'Nombre del cargo'}) ]),
        el('div',{},[ el('label',{className:'label'},['Salario']), el('input',{id:'cSalary',className:'input',type:'number',min:'0',step:'1',inputMode:'numeric',placeholder:'0'}) ]),
        el('div',{},[ el('label',{className:'label'},['Vincular en CRUD']), el('select',{id:'cCrud',className:'select'},[
          ...crudOptions.map((o)=> el('option',{ value:o.value },[o.label]))
        ]) ]),
        el('button',{id:'btnCreate',className:'btn btn--primary'},['Crear cargo']),
        el('span',{id:'msgCreate',className:'text-muted'},[' '])
      ])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo o cargo...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Estado']), el('select',{id:'selStatus',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'activo'},['Activos']), el('option',{value:'inactivo'},['Inactivos']) ]) ]),
      ]),
      el('div',{className:'mt-2 table-wrap'},[
        el('table',{className:'table',id:'tbl'},[
          el('thead',{},[ el('tr',{},[ el('th',{'data-sort':'codigo',style:'cursor:pointer'},['Codigo']), el('th',{'data-sort':'nombre',style:'cursor:pointer'},['Cargo']), el('th',{'data-sort':'salario',style:'cursor:pointer'},['Salario']), el('th',{'data-sort':'alineacionCrud',style:'cursor:pointer'},['Vinculacion']), el('th',{'data-sort':'estado',style:'cursor:pointer'},['Estado']), el('th',{},['Acciones']) ]) ]),
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
      title:'Crear cargo',
      message:'Completa la informacion para crear un cargo.',
      confirmText:'Crear cargo',
      fields:[
        { id:'name', label:'Cargo', type:'text', required:true, placeholder:'Nombre del cargo' },
        { id:'salary', label:'Salario', type:'number', min:'0', step:'1', placeholder:'0' },
        {
          id:'crud',
          label:'Vincular en CRUD',
          type:'select',
          value:'empleado',
          options:crudOptions.map((o)=> ({ value:o.value, label:o.label }))
        }
      ]
    });
    if(!modal.confirmed) return;
    const name=String(modal.values.name||'').trim();
    const salario=parseSalary(modal.values.salary);
    const alineacionCrud=String(modal.values.crud||'empleado').trim() || 'empleado';
    if(!name){ alert('Escribe el cargo.'); return; }
    if(Number.isNaN(salario)){ alert('El salario debe ser un numero valido.'); return; }
    try{
      const code=await deps.getNextCargoCode?.();
      const id=await deps.createCargo?.({ codigo:code, nombre:name, salario, alineacionCrud });
      await deps.addAuditLog?.({ targetType:'cargo', targetId:id, action:'create_cargo', after:{ codigo:code, nombre:name, salario, alineacionCrud, estado:'activo' } });
      alert('Cargo creado OK');
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  const btnOpenCreate=el('button',{id:'btnOpenCreate',className:'btn btn--primary right',type:'button'},['Crear cargo']);
  qs('#tabList .form-row',ui)?.append(btnOpenCreate);
  btnOpenCreate.addEventListener('click',openCreateModal);

  qs('#btnCreate',ui).addEventListener('click',async()=>{
    const name=qs('#cName',ui).value.trim();
    const salario=parseSalary(qs('#cSalary',ui).value);
    const alineacionCrud=qs('#cCrud',ui).value || 'empleado';
    const msg=qs('#msgCreate',ui); msg.textContent=' ';
    if(!name){ msg.textContent='Escribe el cargo.'; return; }
    if(Number.isNaN(salario)){ msg.textContent='El salario debe ser un numero valido.'; return; }
    try{
      const code=await deps.getNextCargoCode?.();
      const id=await deps.createCargo?.({ codigo:code, nombre:name, salario, alineacionCrud });
      await deps.addAuditLog?.({ targetType:'cargo', targetId:id, action:'create_cargo', after:{ codigo:code, nombre:name, salario, alineacionCrud, estado:'activo' } });
      qs('#cName',ui).value=''; qs('#cSalary',ui).value=''; qs('#cCrud',ui).value='empleado';
      msg.textContent='Cargo creado OK'; setTab('list'); setTimeout(()=> msg.textContent=' ',1200);
    }catch(e){ msg.textContent='Error: '+(e?.message||e); }
  });
  let snapshot=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'cargos',after:'#tabList .table-wrap',onChange:render});
  const search=()=> qs('#txtSearch',ui).value.trim().toLowerCase();
  const filterStatus=()=> qs('#selStatus',ui).value;
  function sortVal(c,key){ if(key==='createdAt'){ try{ const x=c.createdAt?.toDate?c.createdAt.toDate(): (c.createdAt?new Date(c.createdAt):null); return x?x.getTime():0; }catch{return 0;} } if(key==='salario') return Number(c.salario)||0; return String(c[key]??'').toLowerCase(); }
  function sortData(data){ if(!sortKey) return data; const out=[...data]; out.sort((a,b)=>{ const va=sortVal(a,sortKey); const vb=sortVal(b,sortKey); if(va===vb) return 0; return va>vb?sortDir:-sortDir; }); return out; }
  function updateSortIndicators(){ ui.querySelectorAll('th[data-sort]').forEach((th)=>{ const base=th.dataset.baseLabel||th.textContent.replace(/\s[\^v▲▼]$/,''); th.dataset.baseLabel=base; const key=th.getAttribute('data-sort'); th.textContent=(sortKey===key)?`${base} ${sortDir===1?'▲':'▼'}`:base; }); }
  function initSorting(){ ui.querySelectorAll('th[data-sort]').forEach((th)=> th.addEventListener('click',()=>{ const key=th.getAttribute('data-sort'); if(sortKey===key) sortDir=sortDir*-1; else { sortKey=key; sortDir=1; } paginator.reset(); render(); })); }
  function render(){ const term=search(); const st=filterStatus(); const data=snapshot.filter(c=> ((!term||(c.codigo||'').toLowerCase().includes(term)||(c.nombre||'').toLowerCase().includes(term)||String(c.salario??'').toLowerCase().includes(term)||formatSalary(c.salario).toLowerCase().includes(term)||crudLabel(c.alineacionCrud||'empleado').toLowerCase().includes(term)) && (!st || c.estado===st))); const sorted=sortData(data); const pageRows=paginator.slice(sorted); tbody.replaceChildren(...pageRows.map(c=> row(c))); const msg=qs('#msg',ui); if(msg) msg.textContent=`Total registros filtrados: ${data.length}`; updateSortIndicators(); }
  function row(c){ const tr=el('tr',{'data-id':c.id}); const tdCodigo=el('td',{},[c.codigo||'-']); const tdNombre=el('td',{},[c.nombre||'-']); const tdSalario=el('td',{},[formatSalary(c.salario)]); const tdCrud=el('td',{},[crudLabel(c.alineacionCrud||'empleado')]); const tdEstado=el('td',{},[ statusBadge(c.estado) ]); const tdAcc=el('td',{},[ actionsCell(c) ]); tr.append(tdCodigo,tdNombre,tdSalario,tdCrud,tdEstado,tdAcc); return tr; }
  function statusBadge(st){ return el('span',{className:'badge '+(st==='activo'?'badge--ok':'badge--off')},[st||'-']); }
  function formatDate(ts){ try{ const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null); return d? new Date(d).toLocaleString(): '-'; }catch{ return '-'; } }
  function auditInfoData(c){
    const hasMod = Boolean(c.lastModifiedAt || c.lastModifiedByEmail || c.lastModifiedByUid);
    return {
      action: hasMod ? 'Ultima modificacion' : 'Creacion',
      user: hasMod ? (c.lastModifiedByEmail||c.lastModifiedByUid||'-') : (c.createdByEmail||c.createdByUid||'-'),
      date: hasMod ? formatDate(c.lastModifiedAt) : formatDate(c.createdAt)
    };
  }
  function actionsCell(c){ const box=el('div',{className:'row-actions'},[]); const btnEdit=el('button',{className:'btn btn--icon',title:'Editar'},['\u270E']); btnEdit.addEventListener('click',()=>{ const tr=tbody.querySelector(`tr[data-id="${c.id}"]`); if(tr) startEdit(tr,c); }); const btnToggle=el('button',{className:'btn btn--icon '+(c.estado==='activo'?'btn--danger':'' ),title:c.estado==='activo'?'Desactivar':'Activar','aria-label':c.estado==='activo'?'Desactivar':'Activar'},[ c.estado==='activo'?'\u23FB':'\u21BA' ]); btnToggle.addEventListener('click',async()=>{ const target=c.estado==='activo'?'inactivo':'activo'; const modal=await showActionModal({ title:`${target==='inactivo'?'Desactivar':'Activar'} cargo`, message:`Cargo: ${c.nombre||'-'}`, confirmText:target==='inactivo'?'Desactivar':'Activar', fields:[{ id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Escribe el motivo o detalle de esta accion' }] }); if(!modal.confirmed) return; try{ await deps.setCargoStatus?.(c.id,target); await deps.addAuditLog?.({ targetType:'cargo', targetId:c.id, action: target==='activo'?'activate_cargo':'deactivate_cargo', before:{estado:c.estado}, after:{estado:target}, note: modal.values.detail||null }); }catch(e){ alert('Error: '+(e?.message||e)); } }); const btnInfo=el('button',{className:'btn btn--icon',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']); btnInfo.addEventListener('click',()=>{ const info=auditInfoData(c); showInfoModal('Informacion del registro',[`Evento: ${info.action}`,`Usuario: ${info.user}`,`Fecha: ${info.date}`]); }); box.append(btnEdit,btnToggle,btnInfo); return box; }
  function startEdit(tr,c){
    const cur={ codigo:c.codigo||'', nombre:c.nombre||'', salario:c.salario, alineacionCrud:c.alineacionCrud||'empleado' };
    const tds=tr.querySelectorAll('td');
    tds[0].replaceChildren(el('input',{className:'input',value:cur.codigo,style:'max-width:160px'}));
    tds[1].replaceChildren(el('input',{className:'input',value:cur.nombre,style:'max-width:260px'}));
    tds[2].replaceChildren(el('input',{className:'input',value:salaryInputValue(cur.salario),style:'max-width:140px',type:'number',min:'0',step:'1',inputMode:'numeric'}));
    tds[3].replaceChildren(crudSelect(cur.alineacionCrud));
    tds[4].replaceChildren(statusBadge(c.estado));
    const box=el('div',{className:'row-actions'},[]);
    const btnSave=el('button',{className:'btn btn--primary'},['Guardar']);
    const btnCancel=el('button',{className:'btn'},['Cancelar']);
    btnSave.addEventListener('click',async()=>{
      const newCode=tds[0].querySelector('input').value.trim();
      const newName=tds[1].querySelector('input').value.trim();
      const newSalary=parseSalary(tds[2].querySelector('input').value);
      const newCrud=tds[3].querySelector('select').value||'empleado';
      if(!newCode||!newName) return alert('Completa codigo y cargo.');
      if(Number.isNaN(newSalary)) return alert('El salario debe ser un numero valido.');
      const modal=await showActionModal({
        title:'Confirmar modificacion',
        message:`Cargo: ${c.nombre||'-'}`,
        confirmText:'Guardar cambios',
        fields:[{ id:'detail', label:'Detalle de la modificacion', type:'textarea', required:true, placeholder:'Describe brevemente el cambio realizado' }]
      });
      if(!modal.confirmed) return;
      try{
        if(newCode!==c.codigo){
          const dup=await deps.findCargoByCode?.(newCode);
          if(dup && dup.id!==c.id) return alert('Ya existe un cargo con ese codigo.');
        }
        await deps.updateCargo?.(c.id,{ codigo:newCode, nombre:newName, salario:newSalary, alineacionCrud:newCrud });
        await deps.addAuditLog?.({
          targetType:'cargo',
          targetId:c.id,
          action:'update_cargo',
          before:{ codigo:c.codigo, nombre:c.nombre, salario:c.salario??null, alineacionCrud:c.alineacionCrud||'empleado' },
          after:{ codigo:newCode, nombre:newName, salario:newSalary, alineacionCrud:newCrud },
          note: modal.values.detail||null
        });
      }catch(e){ alert('Error: '+(e?.message||e)); }
    });
    btnCancel.addEventListener('click',()=> render());
    box.append(btnSave,btnCancel);
    tds[5].replaceChildren(box);
  }
  const un=deps.streamCargos?.((arr)=>{ snapshot=arr||[]; render(); });
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); }); qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui); return ()=> un?.();
};
