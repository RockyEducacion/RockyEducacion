import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
export const EmployeesAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Empleados']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'eCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Documento']), el('input',{id:'eDoc',className:'input',placeholder:'Documento del empleado'}) ]),
        el('div',{},[ el('label',{className:'label'},['Nombre completo']), el('input',{id:'eName',className:'input',placeholder:'Nombre completo'}) ]),
        el('div',{},[ el('label',{className:'label'},['Telefono']), el('input',{id:'ePhone',className:'input',placeholder:'Telefono'}) ]),
        el('div',{},[ el('label',{className:'label'},['Cargo']), el('select',{id:'eCargo',className:'select'},[]) ]),
        el('div',{},[ el('label',{className:'label'},['Sede (buscar)']), el('input',{id:'eSedeSearch',className:'input',list:'eSedeList',placeholder:'Nombre o codigo de sede'}) ]),
        el('div',{},[ el('label',{className:'label'},['Fecha ingreso']), el('input',{id:'eIngreso',className:'input',type:'date'}) ]),
        el('button',{id:'btnCreate',className:'btn btn--primary'},['Crear empleado']),
        el('span',{id:'msgCreate',className:'text-muted'},[' '])
      ]),
      el('datalist',{id:'eSedeList'},[])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo, documento, nombre o sede...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Sede']), el('select',{id:'selSede',className:'select'},[ el('option',{value:''},['Todas']) ]) ]),
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
  tabCreateBtn.addEventListener('click',()=> setTab('create'));
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
  function renderSedeFilter(){
    const select=qs('#selSede',ui);
    if(!select) return;
    const cur=select.value;
    const opts=[
      el('option',{value:''},['Todas']),
      ...sedeList
        .filter((s)=> String(s.codigo||'').trim())
        .sort((a,b)=> String(a.nombre||a.codigo||'').localeCompare(String(b.nombre||b.codigo||'')))
        .map((s)=>{
          const code=String(s.codigo||'').trim();
          return el('option',{value:code, selected:code===cur},[`${s.nombre||code} (${code})`]);
        })
    ];
    select.replaceChildren(...opts);
    if(cur && !sedeList.some((s)=> String(s.codigo||'').trim()===cur)) select.value='';
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
  function sedeOptions(){
    return sedeList
      .map((s)=> sedeLabelByCode(s.codigo))
      .filter((value, index, arr)=> value && arr.indexOf(value)===index);
  }
  function cargoOptions(){
    return [
      { value:'', label:'Seleccione...' },
      ...cargoList.map((cargo)=>({
        value:cargo.codigo||'',
        label:`${cargo.nombre||cargo.codigo||'-'} (${cargo.codigo||'-'})`
      }))
    ];
  }
  async function openCreateModal(){
    const modal=await showActionModal({
      title:'Crear empleado',
      message:'Completa la informacion para crear un empleado.',
      confirmText:'Crear empleado',
      fields:[
        { id:'doc', label:'Documento', type:'text', required:true, placeholder:'Documento del empleado' },
        { id:'name', label:'Nombre completo', type:'text', required:true, placeholder:'Nombre completo' },
        { id:'phone', label:'Telefono', type:'text', required:true, placeholder:'Telefono' },
        { id:'cargo', label:'Cargo', type:'select', required:true, options:cargoOptions() },
        { id:'sede', label:'Sede', type:'datalist', required:true, placeholder:'Selecciona o escribe sede', options:sedeOptions() },
        { id:'ingreso', label:'Fecha ingreso', type:'date', required:true }
      ]
    });
    if(!modal.confirmed) return;
    const doc=String(modal.values.doc||'').trim();
    const name=String(modal.values.name||'').trim();
    const phone=String(modal.values.phone||'').trim();
    const cargoCode=String(modal.values.cargo||'').trim();
    const sedeCode=resolveSedeCode(modal.values.sede);
    const ingreso=String(modal.values.ingreso||'').trim();
    if(!doc){ alert('Escribe el documento.'); return; }
    if(!name){ alert('Escribe el nombre completo.'); return; }
    if(!phone){ alert('Escribe el telefono.'); return; }
    if(!cargoCode){ alert('Selecciona un cargo.'); return; }
    if(!sedeCode){ alert('Selecciona una sede valida.'); return; }
    if(!/^\d{4}-\d{2}-\d{2}$/.test(ingreso)){ alert('Selecciona la fecha de ingreso.'); return; }
    try{
      const dupDoc=await deps.findEmployeeByDocument?.(doc);
      if(dupDoc) {
        if(String(dupDoc.estado||'').trim().toLowerCase()==='inactivo') {
          alert('Ya existe un empleado inactivo con ese documento. Se abrira el reingreso.');
          return openRehireEmployeeModal(dupDoc,{ nombre:name, telefono:phone, cargoCodigo:cargoCode, sedeCodigo:sedeCode, fechaIngreso:ingreso });
        }
        alert('Ya existe un empleado activo con ese documento.');
        return;
      }
      const code=await deps.getNextEmployeeCode?.();
      const cargo=cargoList.find(c=>c.codigo===cargoCode);
      const sede=sedeList.find(s=>s.codigo===sedeCode);
      const id=await deps.createEmployee?.({
        codigo:code,
        documento:doc,
        nombre:name,
        telefono:phone,
        cargoCodigo:cargoCode,
        cargoNombre:cargo?.nombre||null,
        sedeCodigo:sedeCode,
        sedeNombre:sede?.nombre||null,
        fechaIngreso: new Date(`${ingreso}T00:00:00`)
      });
      await deps.addAuditLog?.({ targetType:'employee', targetId:id, action:'create_employee', after:{ codigo:code, documento:doc, nombre:name, sedeCodigo:sedeCode, estado:'activo' } });
      alert('Empleado creado OK');
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  const btnOpenCreate=el('button',{id:'btnOpenCreate',className:'btn btn--primary right',type:'button'},['Crear empleado']);
  qs('#tabList .form-row',ui)?.append(btnOpenCreate);
  btnOpenCreate.addEventListener('click',openCreateModal);
  let snapshot=[]; let historyRows=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'employees',after:'#tabList .table-wrap',onChange:render});
  let unSedes=()=>{};
  let unCargos=()=>{};
  let unSup=()=>{};
  let unSupn=()=>{};
  let unHistory=()=>{};
  let supervisors=[]; let supernumerarios=[];
  const sedeNameByCode=(code)=> sedeList.find(s=>s.codigo===code)?.nombre || '-';
  const cargoNameByCode=(code)=> cargoList.find(c=>c.codigo===code)?.nombre || '-';
  const isLinkedByDoc=(doc)=>{
    const d=String(doc||'').trim();
    if(!d) return false;
    const inSup=supervisors.some((s)=> s.estado!=='inactivo' && String(s.documento||'').trim()===d);
    const inSupn=supernumerarios.some((s)=> s.estado!=='inactivo' && String(s.documento||'').trim()===d);
    return inSup || inSupn;
  };

  qs('#btnCreate',ui).addEventListener('click',async()=>{
    const doc=qs('#eDoc',ui).value.trim();
    const name=qs('#eName',ui).value.trim();
    const phone=qs('#ePhone',ui).value.trim();
    const cargoCode=qs('#eCargo',ui).value;
    const sedeCode=resolveSedeCode(sedeInput.value);
    const ingreso=qs('#eIngreso',ui).value;
    const msg=qs('#msgCreate',ui); msg.textContent=' ';
    if(!doc){ msg.textContent='Escribe el documento.'; return; }
    if(!name){ msg.textContent='Escribe el nombre completo.'; return; }
    if(!phone){ msg.textContent='Escribe el telefono.'; return; }
    if(!cargoCode){ msg.textContent='Selecciona un cargo.'; return; }
    if(!sedeCode){ msg.textContent='Selecciona una sede.'; return; }
    if(!ingreso){ msg.textContent='Selecciona la fecha de ingreso.'; return; }
    try{
      const dupDoc=await deps.findEmployeeByDocument?.(doc);
      if(dupDoc) {
        if(String(dupDoc.estado||'').trim().toLowerCase()==='inactivo') {
          msg.textContent='Ya existe un empleado inactivo con ese documento. Se abrira el reingreso.';
          await openRehireEmployeeModal(dupDoc,{ nombre:name, telefono:phone, cargoCodigo:cargoCode, sedeCodigo:sedeCode, fechaIngreso:ingreso });
          return;
        }
        msg.textContent='Ya existe un empleado activo con ese documento.';
        return;
      }
      const code=await deps.getNextEmployeeCode?.();
      const cargo=cargoList.find(c=>c.codigo===cargoCode);
      const sede=sedeList.find(s=>s.codigo===sedeCode);
      const id=await deps.createEmployee?.({
        codigo:code,
        documento:doc,
        nombre:name,
        telefono:phone,
        cargoCodigo:cargoCode,
        cargoNombre:cargo?.nombre||null,
        sedeCodigo:sedeCode,
        sedeNombre:sede?.nombre||null,
        fechaIngreso: new Date(`${ingreso}T00:00:00`)
      });
      await deps.addAuditLog?.({ targetType:'employee', targetId:id, action:'create_employee', after:{ codigo:code, documento:doc, nombre:name, sedeCodigo:sedeCode, estado:'activo' } });
      qs('#eDoc',ui).value=''; qs('#eName',ui).value=''; qs('#ePhone',ui).value=''; qs('#eIngreso',ui).value=''; sedeInput.value=''; renderCargoSelect(); renderSedeSelect();
      msg.textContent='Empleado creado OK'; setTab('list'); setTimeout(()=> msg.textContent=' ',1200);
    }catch(e){ msg.textContent='Error: '+(e?.message||e); }
  });

  const search=()=> qs('#txtSearch',ui).value.trim().toLowerCase();
  const filterSede=()=> qs('#selSede',ui)?.value||'';
  const filterStatus=()=> qs('#selStatus',ui).value;
  function toSortableDate(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      return d? d.getTime(): 0;
    }catch{ return 0; }
  }
  function getSortValue(e,key){
    const view=employeeAssignmentView(e);
    if(key==='cargoNombre') return (view.current?.cargoNombre||cargoNameByCode(view.current?.cargoCodigo)||'').toLowerCase();
    if(key==='sedeNombre') return (view.current?.sedeNombre||sedeNameByCode(view.current?.sedeCodigo)||'').toLowerCase();
    if(key==='fechaIngreso') return toSortableDate(view.current?.fechaIngreso||e.fechaIngreso);
    if(key==='fechaRetiro') return toSortableDate(e[key]);
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
    const term=search(); const sedeCode=filterSede(); const st=filterStatus();
    const data=snapshot.filter(e=>{
      const view=employeeAssignmentView(e);
      const currentSedeCode=String(view.current?.sedeCodigo||'').trim();
      const text=[
        e.codigo,
        e.documento,
        e.nombre,
        view.current?.cargoNombre,
        cargoNameByCode(view.current?.cargoCodigo),
        view.current?.sedeNombre,
        sedeNameByCode(view.current?.sedeCodigo),
        view.programmed?.cargoNombre,
        cargoNameByCode(view.programmed?.cargoCodigo),
        view.programmed?.sedeNombre,
        sedeNameByCode(view.programmed?.sedeCodigo)
      ].join(' ').toLowerCase();
      return (!term || text.includes(term)) && (!sedeCode || currentSedeCode===sedeCode) && (!st || e.estado===st);
    });
    const sorted=sortData(data);
    const pageRows=paginator.slice(sorted);
    tbody.replaceChildren(...pageRows.map(e=> row(e)));
    const msg=qs('#msg',ui); if(msg) msg.textContent=`Total registros filtrados: ${data.length}`;
    updateSortIndicators();
  }
  function row(e){
    const view=employeeAssignmentView(e);
    const tr=el('tr',{'data-id':e.id});
    const tdCodigo=el('td',{},[e.codigo||'-']);
    const linked=isLinkedByDoc(e.documento);
    const tdDoc=el('td',{}, linked ? [e.documento||'-',' ',el('span',{className:'badge'},['Vinculado'])] : [e.documento||'-']);
    const tdNombre=el('td',{},[e.nombre||'-']);
    const tdTel=el('td',{},[e.telefono||'-']);
    const tdCargo=el('td',{},[ assignmentCellText(view.current,'cargo'), programmedBadge(view.programmed,'cargo') ].filter(Boolean));
    const tdSede=el('td',{},[ assignmentCellText(view.current,'sede'), programmedBadge(view.programmed,'sede') ].filter(Boolean));
    const tdEstado=el('td',{},[ statusBadge(e.estado) ]);
    const tdIngreso=el('td',{},[ formatDate(view.current?.fechaIngreso||e.fechaIngreso) ]);
    const tdRetiro=el('td',{},[ formatDate(e.fechaRetiro) ]);
    const tdAcc=el('td',{},[ actionsCell(e) ]);
    tr.append(tdCodigo,tdDoc,tdNombre,tdTel,tdCargo,tdSede,tdEstado,tdIngreso,tdRetiro,tdAcc);
    return tr;
  }
  function statusBadge(st){ return el('span',{className:'badge '+(st==='activo'?'badge--ok':'badge--off')},[st||'-']); }
  function assignmentCellText(assignment={},kind='sede'){
    if(kind==='cargo') return assignment?.cargoNombre||cargoNameByCode(assignment?.cargoCodigo)||assignment?.cargoCodigo||'-';
    return assignment?.sedeNombre||sedeNameByCode(assignment?.sedeCodigo)||assignment?.sedeCodigo||'-';
  }
  function programmedBadge(assignment=null,kind='sede'){
    if(!assignment) return null;
    const target=assignmentCellText(assignment,kind);
    const label=`Programado desde ${formatInputDate(assignment.fechaIngreso)}: ${target}`;
    return el('span',{className:'badge',title:label,'aria-label':label,style:'margin-left:.35rem;cursor:help;'},['Programado']);
  }
  function employeeAssignmentView(e={}){
    const today=todayInputDate();
    const rows=historyRowsByEmployee(e);
    const current=resolveAssignmentOnDate(e,today,rows) || employeeAssignmentData(e);
    const programmed=nextProgrammedAssignment(today,rows);
    return { current, programmed };
  }
  function historyRowsByEmployee(e={}){
    const employeeId=String(e?.id||'').trim();
    const doc=String(e?.documento||'').trim();
    return (historyRows||[]).filter((row)=>{
      const rowEmployeeId=String(row?.employeeId||'').trim();
      const rowDoc=String(row?.documento||'').trim();
      return (employeeId && rowEmployeeId===employeeId) || (doc && rowDoc===doc);
    });
  }
  function employeeAssignmentData(e={}){
    return {
      cargoCodigo:e.cargoCodigo||null,
      cargoNombre:e.cargoNombre||null,
      sedeCodigo:e.sedeCodigo||null,
      sedeNombre:e.sedeNombre||null,
      fechaIngreso:e.fechaIngreso||null,
      fechaRetiro:e.fechaRetiro||null
    };
  }
  function resolveAssignmentOnDate(e={},day,rows=[]){
    const matching=(rows||[]).filter((row)=>{
      const ingreso=toISODateValue(row?.fechaIngreso);
      if(!ingreso || ingreso>day) return false;
      const retiro=toISODateValue(row?.fechaRetiro);
      return !retiro || retiro>=day;
    });
    if(!matching.length) return null;
    matching.sort((a,b)=>{
      const ai=toISODateValue(a.fechaIngreso)||'';
      const bi=toISODateValue(b.fechaIngreso)||'';
      if(ai!==bi) return bi.localeCompare(ai);
      return String(b.createdAt||'').localeCompare(String(a.createdAt||''));
    });
    return matching[0];
  }
  function nextProgrammedAssignment(day,rows=[]){
    const future=(rows||[]).filter((row)=>{
      const ingreso=toISODateValue(row?.fechaIngreso);
      return ingreso && ingreso>day && !row?.fechaRetiro;
    });
    if(!future.length) return null;
    future.sort((a,b)=>{
      const ai=toISODateValue(a.fechaIngreso)||'';
      const bi=toISODateValue(b.fechaIngreso)||'';
      if(ai!==bi) return ai.localeCompare(bi);
      return String(a.createdAt||'').localeCompare(String(b.createdAt||''));
    });
    return future[0];
  }
  function formatDate(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      return d? new Date(d).toLocaleDateString(): '-';
    }catch{ return '-'; }
  }
  async function openCargoHistoryModal(e){
    const employeeId=String(e?.id||'').trim();
    if(!employeeId || typeof deps.streamEmployeeCargoHistory!=='function'){
      showInfoModal('Historial del empleado',['No hay historial disponible para este empleado.']);
      return;
    }
    showInfoModal(`Historial del empleado - ${e?.nombre||'-'}`,['Cargando...']);
    let done=false;
    const un=deps.streamEmployeeCargoHistory(employeeId,(rows)=>{
      if(done) return;
      done=true;
      const list=Array.isArray(rows)? rows:[];
      if(!list.length){
        showInfoModal(`Historial del empleado - ${e?.nombre||'-'}`,['Sin registros.']);
        un?.();
        return;
      }
      const lines=list.map((row,idx)=>{
        const ingreso=formatDate(row.fechaIngreso);
        const retiro=row.fechaRetiro ? formatDate(row.fechaRetiro) : 'Activo';
        const cargo=row.cargoNombre||row.cargoCodigo||'-';
        const sede=row.sedeNombre||row.sedeCodigo||'-';
        return `${idx+1}. Cargo: ${cargo} | Sede: ${sede} | Ingreso: ${ingreso} | Retiro: ${retiro}`;
      });
      showInfoModal(`Historial del empleado - ${e?.nombre||'-'}`,lines);
      un?.();
    });
    setTimeout(()=>{
      if(done) return;
      done=true;
      showInfoModal(`Historial del empleado - ${e?.nombre||'-'}`,['No se pudo cargar el historial. Intenta de nuevo.']);
      un?.();
    },5000);
  }
  async function openMoreOptionsModal(e){
    const inactive=String(e?.estado||'').trim().toLowerCase()==='inactivo';
    const modal=await showActionModal({
      title:'Mas opciones',
      message:`Empleado: ${e.nombre||'-'}`,
      confirmText:'Continuar',
      fields:[{
        id:'action',
        label:'Accion',
        type:'select',
        required:true,
        options:[
          { value:'', label:'Seleccione...' },
          { value:'edit', label:'Editar' },
          { value:'transfer', label:'Trasladar empleado' },
          { value:'cargo', label:'Cambiar cargo' },
          { value:'certificate', label:'Generar certificado' },
          inactive ? { value:'rehire', label:'Reingresar empleado' } : null,
          { value:'retire', label:'Retirar empleado' }
        ].filter(Boolean)
      }]
    });
    if(!modal.confirmed) return;
    if(modal.values.action==='edit') return openEditEmployeeModal(e);
    if(modal.values.action==='transfer') return openTransferEmployeeModal(e);
    if(modal.values.action==='cargo') return openChangeCargoModal(e);
    if(modal.values.action==='certificate') return openCertificateModal(e);
    if(modal.values.action==='rehire') return openRehireEmployeeModal(e);
    if(modal.values.action==='retire') return openRetireEmployeeModal(e);
  }
  async function openCertificateModal(e){
    if(e.estado!=='activo') return alert('Solo puedes generar certificados de empleados activos.');
    const modal=await showActionModal({
      title:'Generar certificado',
      message:`Empleado: ${e.nombre||'-'}`,
      confirmText:'Descargar PDF',
      fields:[{
        id:'type',
        label:'Tipo de certificado',
        type:'select',
        required:true,
        options:[
          { value:'basic', label:'Laboral basico' },
          { value:'with_salary', label:'Laboral con salario' }
        ]
      }]
    });
    if(!modal.confirmed) return;
    try{
      await deps.generateEmployeeCertificate?.(e.id, modal.values.type||'basic');
      await deps.addAuditLog?.({ targetType:'employee', targetId:e.id, action:'generate_employee_certificate', after:{ documento:e.documento||null, type:modal.values.type||'basic' } });
    }catch(err){ alert('Error: '+(err?.message||err)); }
  }
  async function openEditEmployeeModal(e){
    const modal=await showActionModal({
      title:'Editar empleado',
      message:`Empleado: ${e.nombre||'-'}`,
      confirmText:'Guardar cambios',
      fields:[
        { id:'codigo', label:'Codigo', type:'text', required:true, value:e.codigo||'' },
        { id:'documento', label:'Documento', type:'text', required:true, value:e.documento||'' },
        { id:'nombre', label:'Nombre completo', type:'text', required:true, value:e.nombre||'' },
        { id:'telefono', label:'Telefono', type:'text', required:true, value:e.telefono||'' },
        { id:'fechaIngreso', label:'Fecha ingreso', type:'date', required:true, value:toInputDate(e.fechaIngreso) },
        { id:'detail', label:'Detalle de la modificacion', type:'textarea', required:true, placeholder:'Describe brevemente el cambio realizado' }
      ]
    });
    if(!modal.confirmed) return;
    const newCode=String(modal.values.codigo||'').trim();
    const newDoc=String(modal.values.documento||'').trim();
    const newName=String(modal.values.nombre||'').trim();
    const newPhone=String(modal.values.telefono||'').trim();
    const newIngreso=String(modal.values.fechaIngreso||'').trim();
    if(!/^\d{4}-\d{2}-\d{2}$/.test(newIngreso)) return alert('Selecciona la fecha de ingreso.');
    const currentRetiro=toInputDate(e.fechaRetiro);
    if(currentRetiro && newIngreso>currentRetiro) return alert('La fecha de ingreso no puede ser posterior a la fecha de retiro.');
    try{
      if(newCode!==String(e.codigo||'')){ const dup=await deps.findEmployeeByCode?.(newCode); if(dup && dup.id!==e.id) return alert('Ya existe un empleado con ese codigo.'); }
      if(newDoc!==String(e.documento||'')){ const dupDoc=await deps.findEmployeeByDocument?.(newDoc); if(dupDoc && dupDoc.id!==e.id) return alert('Ya existe un empleado con ese documento.'); }
      await deps.updateEmployee?.(e.id,{
        codigo:newCode,
        documento:newDoc,
        nombre:newName,
        telefono:newPhone,
        fechaIngreso:new Date(`${newIngreso}T00:00:00`)
      });
      await deps.addAuditLog?.({ targetType:'employee', targetId:e.id, action:'update_employee', before:{ codigo:e.codigo, documento:e.documento, nombre:e.nombre, telefono:e.telefono, fechaIngreso:e.fechaIngreso||null }, after:{ codigo:newCode, documento:newDoc, nombre:newName, telefono:newPhone, fechaIngreso:newIngreso }, note:modal.values.detail||null });
    }catch(err){ alert('Error: '+(err?.message||err)); }
  }
  async function openTransferEmployeeModal(e){
    if(e.estado!=='activo') return alert('Solo puedes trasladar empleados activos.');
    const suggestedEnd=toInputDate(new Date()) || '';
    const suggestedStart=addOneDayToInputDate(suggestedEnd);
    const todayBogota=todayInputDate();
    const modal=await showActionModal({
      title:'Trasladar empleado',
      message:`Empleado: ${e.nombre||'-'}`,
      confirmText:'Trasladar',
      fields:[
        { id:'currentSede', label:'Sede actual', type:'text', value:sedeLabelByCode(e.sedeCodigo)||e.sedeNombre||e.sedeCodigo||'-', readonly:true },
        { id:'sede', label:'Nueva sede', type:'datalist', required:true, placeholder:'Selecciona o escribe sede', options:sedeOptions() },
        { id:'historyRetiroDate', label:'Fecha de retiro en sede anterior', type:'date', required:true, value:suggestedEnd },
        { id:'historyIngresoDate', label:'Fecha de ingreso en nueva sede', type:'date', required:true, value:suggestedStart, min:todayBogota },
        { id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Describe brevemente el traslado' }
      ]
    });
    if(!modal.confirmed) return;
    const newSedeCode=resolveSedeCode(modal.values.sede);
    const historyRetiroDate=String(modal.values.historyRetiroDate||'').trim();
    const historyIngresoDate=String(modal.values.historyIngresoDate||'').trim();
    if(!newSedeCode) return alert('Selecciona una sede valida.');
    if(newSedeCode===String(e.sedeCodigo||'')) return alert('Selecciona una sede diferente.');
    if(!validInputDate(historyRetiroDate) || !validInputDate(historyIngresoDate)) return alert('Fechas invalidas.');
    if(historyIngresoDate<todayBogota) return alert(`La fecha de inicio en nueva sede no puede ser anterior a hoy (${todayBogota}).`);
    if(addOneDayToInputDate(historyRetiroDate)!==historyIngresoDate) return alert('La nueva asignacion debe iniciar el dia siguiente al fin del tramo anterior.');
    try{
      const newSede=sedeList.find(s=>s.codigo===newSedeCode);
      await deps.updateEmployee?.(e.id,{
        sedeCodigo:newSedeCode,
        sedeNombre:newSede?.nombre||null,
        assignmentFechaIngreso:new Date(`${historyIngresoDate}T00:00:00`),
        assignmentFechaRetiro:new Date(`${historyRetiroDate}T00:00:00`),
        historialFechaRetiro:new Date(`${historyRetiroDate}T00:00:00`)
      });
      await deps.addAuditLog?.({ targetType:'employee', targetId:e.id, action:'transfer_employee', before:{ sedeCodigo:e.sedeCodigo, sedeNombre:e.sedeNombre||null }, after:{ sedeCodigo:newSedeCode, sedeNombre:newSede?.nombre||null, assignmentFechaIngreso:historyIngresoDate, assignmentFechaRetiro:historyRetiroDate }, note:modal.values.detail||null });
    }catch(err){ alert('Error: '+(err?.message||err)); }
  }
  async function openChangeCargoModal(e){
    if(e.estado!=='activo') return alert('Solo puedes cambiar el cargo de empleados activos.');
    const suggestedEnd=toInputDate(new Date()) || '';
    const suggestedStart=addOneDayToInputDate(suggestedEnd);
    const modal=await showActionModal({
      title:'Cambiar cargo',
      message:`Empleado: ${e.nombre||'-'}`,
      confirmText:'Cambiar cargo',
      fields:[
        { id:'currentCargo', label:'Cargo actual', type:'text', value:e.cargoNombre||cargoNameByCode(e.cargoCodigo)||e.cargoCodigo||'-', readonly:true },
        { id:'cargo', label:'Nuevo cargo', type:'select', required:true, options:cargoOptions() },
        { id:'historyRetiroDate', label:'Fecha fin de cargo anterior', type:'date', required:true, value:suggestedEnd },
        { id:'historyIngresoDate', label:'Fecha inicio de nuevo cargo', type:'date', required:true, value:suggestedStart },
        { id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Describe brevemente el cambio de cargo' }
      ]
    });
    if(!modal.confirmed) return;
    const newCargoCode=String(modal.values.cargo||'').trim();
    const historyRetiroDate=String(modal.values.historyRetiroDate||'').trim();
    const historyIngresoDate=String(modal.values.historyIngresoDate||'').trim();
    if(!newCargoCode) return alert('Selecciona un cargo.');
    if(newCargoCode===String(e.cargoCodigo||'')) return alert('Selecciona un cargo diferente.');
    if(!validInputDate(historyRetiroDate) || !validInputDate(historyIngresoDate)) return alert('Fechas invalidas.');
    if(addOneDayToInputDate(historyRetiroDate)!==historyIngresoDate) return alert('La nueva asignacion debe iniciar el dia siguiente al fin del tramo anterior.');
    try{
      const newCargo=cargoList.find(c=>c.codigo===newCargoCode);
      await deps.updateEmployee?.(e.id,{
        cargoCodigo:newCargoCode,
        cargoNombre:newCargo?.nombre||null,
        fechaIngreso:new Date(`${historyIngresoDate}T00:00:00`),
        assignmentFechaIngreso:new Date(`${historyIngresoDate}T00:00:00`),
        assignmentFechaRetiro:new Date(`${historyRetiroDate}T00:00:00`),
        historialFechaRetiro:new Date(`${historyRetiroDate}T00:00:00`)
      });
      await deps.addAuditLog?.({ targetType:'employee', targetId:e.id, action:'change_employee_cargo', before:{ cargoCodigo:e.cargoCodigo, cargoNombre:e.cargoNombre||null }, after:{ cargoCodigo:newCargoCode, cargoNombre:newCargo?.nombre||null, assignmentFechaIngreso:historyIngresoDate, assignmentFechaRetiro:historyRetiroDate }, note:modal.values.detail||null });
    }catch(err){ alert('Error: '+(err?.message||err)); }
  }
  async function openRetireEmployeeModal(e){
    if(e.estado!=='activo') return alert('Este empleado ya esta retirado.');
    const suggested=toInputDate(new Date()) || '';
    const modal=await showActionModal({
      title:'Retirar empleado',
      message:`Empleado: ${e.nombre||'-'}`,
      confirmText:'Retirar',
      fields:[
        { id:'retiroDate', label:'Fecha de retiro', type:'date', required:true, value:suggested },
        { id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Escribe el motivo o detalle de esta accion' }
      ]
    });
    if(!modal.confirmed) return;
    const retiro=String(modal.values.retiroDate||'').trim();
    if(!validInputDate(retiro)) return alert('Fecha invalida. Usa formato AAAA-MM-DD.');
    const ingreso=toInputDate(e.fechaIngreso);
    if(ingreso && retiro<ingreso) return alert('La fecha de retiro no puede ser anterior a la fecha de ingreso.');
    try{
      const retiroDate=new Date(`${retiro}T00:00:00`);
      await deps.setEmployeeStatus?.(e.id,'inactivo',{ fechaRetiro:retiroDate });
      await deps.addAuditLog?.({ targetType:'employee', targetId:e.id, action:'retire_employee', before:{estado:e.estado, fechaRetiro:e.fechaRetiro||null}, after:{estado:'inactivo', fechaRetiro:retiro}, note:modal.values.detail||null });
    }catch(err){ alert('Error: '+(err?.message||err)); }
  }
  async function openRehireEmployeeModal(e, defaults={}){
    if(String(e?.estado||'').trim().toLowerCase()!=='inactivo') return alert('Solo puedes reingresar empleados inactivos.');
    const lastRetiro=lastRetiroDateForEmployee(e);
    const minIngreso=lastRetiro||'';
    const modal=await showActionModal({
      title:'Reingresar empleado',
      message:`Empleado: ${e.nombre||'-'}${lastRetiro ? `\nUltimo retiro: ${formatInputDate(lastRetiro)}` : ''}`,
      confirmText:'Reingresar',
      fields:[
        { id:'documento', label:'Documento', type:'text', value:e.documento||'', readonly:true },
        { id:'nombre', label:'Nombre completo', type:'text', required:true, value:defaults.nombre||e.nombre||'' },
        { id:'telefono', label:'Telefono', type:'text', required:true, value:defaults.telefono||e.telefono||'' },
        { id:'cargo', label:'Cargo', type:'select', required:true, value:defaults.cargoCodigo||e.cargoCodigo||'', options:cargoOptions() },
        { id:'sede', label:'Sede', type:'datalist', required:true, placeholder:'Selecciona o escribe sede', value:sedeLabelByCode(defaults.sedeCodigo||e.sedeCodigo)||'', options:sedeOptions() },
        { id:'fechaIngreso', label:'Nueva fecha ingreso', type:'date', required:true, value:defaults.fechaIngreso||minIngreso, min:minIngreso },
        { id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Describe brevemente el reingreso' }
      ]
    });
    if(!modal.confirmed) return;
    const newName=String(modal.values.nombre||'').trim();
    const newPhone=String(modal.values.telefono||'').trim();
    const newCargoCode=String(modal.values.cargo||'').trim();
    const newSedeCode=resolveSedeCode(modal.values.sede);
    const newIngreso=String(modal.values.fechaIngreso||'').trim();
    if(!newName) return alert('Escribe el nombre completo.');
    if(!newPhone) return alert('Escribe el telefono.');
    if(!newCargoCode) return alert('Selecciona un cargo.');
    if(!newSedeCode) return alert('Selecciona una sede valida.');
    if(!validInputDate(newIngreso)) return alert('Selecciona una fecha de ingreso valida.');
    if(lastRetiro && newIngreso<lastRetiro) return alert(`La nueva fecha de ingreso no puede ser anterior al ultimo retiro (${lastRetiro}).`);
    try{
      const cargo=cargoList.find(c=>c.codigo===newCargoCode);
      const sede=sedeList.find(s=>s.codigo===newSedeCode);
      const updated=await deps.rehireEmployee?.(e.id,{
        nombre:newName,
        telefono:newPhone,
        cargoCodigo:newCargoCode,
        cargoNombre:cargo?.nombre||null,
        sedeCodigo:newSedeCode,
        sedeNombre:sede?.nombre||null,
        fechaIngreso:new Date(`${newIngreso}T00:00:00`)
      });
      await deps.addAuditLog?.({ targetType:'employee', targetId:e.id, action:'rehire_employee', before:{ estado:e.estado, fechaRetiro:e.fechaRetiro||null }, after:{ estado:'activo', fechaIngreso:newIngreso, cargoCodigo:newCargoCode, sedeCodigo:newSedeCode }, note:modal.values.detail||null });
      alert(`Empleado reingresado OK: ${updated?.nombre||newName}`);
    }catch(err){ alert('Error: '+(err?.message||err)); }
  }
  function actionsCell(e){
    const box=el('div',{className:'row-actions'},[]);
    const btnMore=el('button',{className:'btn btn--icon',type:'button',title:'Mas opciones','aria-label':'Mas opciones'},['\u22EF']);
    btnMore.addEventListener('click',()=> openMoreOptionsModal(e));
    const btnInfo=el('button',{className:'btn btn--icon',type:'button',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']);
    btnInfo.addEventListener('click',()=>{ openCargoHistoryModal(e); });
    box.append(btnMore,btnInfo);
    return box;
  }
  function toInputDate(ts){
    try{
      const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null);
      if(!d) return '';
      const pad=(n)=> String(n).padStart(2,'0');
      return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
    }catch{ return ''; }
  }
  function todayInputDate(){
    return new Intl.DateTimeFormat('en-CA',{ timeZone:'America/Bogota' }).format(new Date());
  }
  function toISODateValue(value){
    if(!value) return '';
    if(typeof value==='string'){
      const raw=value.trim();
      if(/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
      const parsed=new Date(raw);
      return Number.isNaN(parsed.getTime())?'':parsed.toISOString().slice(0,10);
    }
    const parsed=value?.toDate?value.toDate():(value instanceof Date?value:null);
    return parsed && !Number.isNaN(parsed.getTime())?parsed.toISOString().slice(0,10):'';
  }
  function formatInputDate(value){
    const iso=toISODateValue(value);
    if(!iso) return '-';
    const [year,month,day]=iso.split('-').map((part)=>Number(part));
    const date=new Date(year,(month||1)-1,day||1);
    return date.toLocaleDateString();
  }
  function validInputDate(value){
    const raw=String(value||'').trim();
    if(!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return false;
    return !Number.isNaN(new Date(`${raw}T00:00:00`).getTime());
  }
  function addOneDayToInputDate(value){
    if(!/^\d{4}-\d{2}-\d{2}$/.test(String(value||'').trim())) return '';
    const dt=new Date(`${value}T00:00:00`);
    if(Number.isNaN(dt.getTime())) return '';
    dt.setDate(dt.getDate()+1);
    return toInputDate(dt);
  }
  function lastRetiroDateForEmployee(e={}){
    const dates=[
      toISODateValue(e?.fechaRetiro),
      ...historyRowsByEmployee(e).map((row)=>toISODateValue(row?.fechaRetiro))
    ].filter(Boolean).sort();
    return dates.length ? dates[dates.length-1] : '';
  }
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); });
  qs('#selSede',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui);
  let un=()=>{};
  try{
    unSedes=deps.streamSedes?.((arr)=>{ sedeList=(arr||[]).filter(s=>s.estado!=='inactivo'); renderSedeSelect(); renderSedeFilter(); render(); }) || (()=>{});
    unCargos=deps.streamCargos?.((arr)=>{ cargoList=(arr||[]).filter(c=>c.estado!=='inactivo'); renderCargoSelect(); render(); }) || (()=>{});
    unSup=deps.streamSupervisors?.((arr)=>{ supervisors=arr||[]; render(); }) || (()=>{});
    unSupn=deps.streamSupernumerarios?.((arr)=>{ supernumerarios=arr||[]; render(); }) || (()=>{});
    unHistory=deps.streamEmployeeCargoHistoryAll?.((arr)=>{ historyRows=arr||[]; render(); }) || (()=>{});
    un=deps.streamEmployees?.((arr)=>{ snapshot=arr||[]; render(); }) || (()=>{});
  }catch(e){
    const msg=qs('#msg',ui); if(msg) msg.textContent='Error cargando empleados: '+(e?.message||e);
  }
  return ()=>{ un?.(); unSedes?.(); unCargos?.(); unSup?.(); unSupn?.(); unHistory?.(); };
};
