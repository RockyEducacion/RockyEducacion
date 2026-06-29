import { el, qs } from '../utils/dom.js';
import { showInfoModal } from '../utils/infoModal.js';
import { showActionModal } from '../utils/actionModal.js';
import { createTablePagination } from '../utils/pagination.js';
import { can, PERMS } from '../permissions.js';
export const SedesAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Sedes']),
    el('div',{className:'tabs mt-2'},[
      el('button',{id:'tabCreateBtn',className:'tab',type:'button'},['Crear']),
      el('button',{id:'tabListBtn',className:'tab is-active',type:'button'},['Consultar'])
    ]),
    el('div',{id:'tabCreate',className:'hidden'},[
      el('div',{className:'form-row mt-2'},[
        el('div',{},[ el('label',{className:'label'},['Codigo (automatico)']), el('input',{id:'sCode',className:'input',placeholder:'Se generara al crear',disabled:true}) ]),
        el('div',{},[ el('label',{className:'label'},['Nombre']), el('input',{id:'sName',className:'input',placeholder:'Nombre de la sede'}) ]),
        el('div',{},[ el('label',{className:'label'},['Dependencia (buscar)']), el('input',{id:'sDepSearch',className:'input',list:'sDepList',placeholder:'Nombre o codigo de dependencia'}) ]),
        el('div',{},[ el('label',{className:'label'},['Zona (buscar)']), el('input',{id:'sZoneSearch',className:'input',list:'sZoneList',placeholder:'Nombre o codigo de zona'}) ]),
        el('div',{},[ el('label',{className:'label'},['Nro de operarios']), el('input',{id:'sOps',className:'input',type:'number',min:'0',step:'1',inputMode:'numeric',placeholder:'0'}) ]),
        el('div',{},[ el('label',{className:'label'},['Jornada']), el('select',{id:'sJornada',className:'select'},[
          el('option',{value:'lun_vie'},['Lunes a viernes']),
          el('option',{value:'lun_sab'},['Lunes a sabado']),
          el('option',{value:'lun_dom'},['Lunes a domingo'])
        ]) ]),
        el('div',{},[ el('label',{className:'label'},['QR']), el('select',{id:'sQrEnabled',className:'select'},[
          el('option',{value:'false'},['Inactivo']),
          el('option',{value:'true'},['Activo'])
        ]) ]),
        el('div',{},[ el('label',{className:'label'},['Latitud QR']), el('input',{id:'sQrLat',className:'input',type:'number',step:'0.000001',placeholder:'Ej: 6.244203'}) ]),
        el('div',{},[ el('label',{className:'label'},['Longitud QR']), el('input',{id:'sQrLng',className:'input',type:'number',step:'0.000001',placeholder:'Ej: -75.581212'}) ]),
        el('div',{},[ el('label',{className:'label'},['Radio QR (m)']), el('input',{id:'sQrRadius',className:'input',type:'number',min:'1',step:'1',value:'500'}) ]),
        el('button',{id:'btnCreate',className:'btn btn--primary'},['Crear sede']),
        el('span',{id:'msgCreate',className:'text-muted'},[' '])
      ]),
      el('datalist',{id:'sDepList'},[]),
      el('datalist',{id:'sZoneList'},[])
    ]),
    el('div',{id:'tabList'},[
      el('div',{className:'form-row'},[
        el('div',{},[ el('label',{className:'label'},['Buscar']), el('input',{id:'txtSearch',className:'input',placeholder:'Codigo, nombre, dependencia o zona...'}) ]),
        el('div',{},[ el('label',{className:'label'},['Estado']), el('select',{id:'selStatus',className:'select'},[ el('option',{value:''},['Todos']), el('option',{value:'activo'},['Activos']), el('option',{value:'inactivo'},['Inactivos']) ]) ]),
      ]),
      el('div',{className:'mt-2 table-wrap'},[
        el('table',{className:'table',id:'tbl'},[
          el('thead',{},[ el('tr',{},[
            el('th',{'data-sort':'codigo',style:'cursor:pointer'},['Codigo']),
            el('th',{'data-sort':'nombre',style:'cursor:pointer'},['Nombre']),
            el('th',{'data-sort':'dependenciaNombre',style:'cursor:pointer'},['Dependencia']),
            el('th',{'data-sort':'zonaNombre',style:'cursor:pointer'},['Zona']),
            el('th',{'data-sort':'numeroOperarios',style:'cursor:pointer'},['Operarios']),
            el('th',{'data-sort':'jornada',style:'cursor:pointer'},['Jornada']),
            el('th',{'data-sort':'qrEnabled',style:'cursor:pointer'},['QR']),
            el('th',{},['Ubicacion QR']),
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

  let depList=[]; let zoneList=[];
  const depInput=qs('#sDepSearch',ui); const zoneInput=qs('#sZoneSearch',ui);
  const depDatalist=qs('#sDepList',ui); const zoneDatalist=qs('#sZoneList',ui);

  function buildOptions(items, selected){
    const opts=[ el('option',{value:''},['Seleccione...']) ];
    items.forEach((item)=>{
      const code=item.codigo||''; const label=item.nombre||code||'-';
      opts.push(el('option',{value:code, selected: code && code===selected},[ `${label} (${code||'-'})` ]));
    });
    return opts;
  }
  function labelByCode(list, code){
    const it=list.find(x=>x.codigo===code);
    return it ? `${it.nombre||it.codigo} (${it.codigo||'-'})` : '';
  }
  function resolveCode(list, rawValue){
    const raw=String(rawValue||'').trim();
    if(!raw) return '';
    const byCode=list.find(x=> String(x.codigo||'').toLowerCase()===raw.toLowerCase());
    if(byCode) return byCode.codigo;
    const m=raw.match(/\(([^)]+)\)\s*$/);
    if(m){
      const code=m[1].trim();
      const byLabel=list.find(x=> String(x.codigo||'').toLowerCase()===code.toLowerCase());
      if(byLabel) return byLabel.codigo;
    }
    const byName=list.find(x=> String(x.nombre||'').toLowerCase()===raw.toLowerCase());
    return byName?.codigo||'';
  }
  function renderSelects(){
    const depOpts=depList
      .map((d)=> labelByCode(depList,d.codigo))
      .filter((v,i,arr)=> v && arr.indexOf(v)===i)
      .map((value)=> el('option',{value}));
    depDatalist.replaceChildren(...depOpts);
    const zoneOpts=zoneList
      .map((z)=> labelByCode(zoneList,z.codigo))
      .filter((v,i,arr)=> v && arr.indexOf(v)===i)
      .map((value)=> el('option',{value}));
    zoneDatalist.replaceChildren(...zoneOpts);
  }
  function catalogLabelByCode(list, code){
    const item=list.find((x)=>x.codigo===code);
    if(!item) return '';
    return `${item.nombre||item.codigo||'-'} (${item.codigo||'-'})`;
  }
  function catalogOptions(list){
    return list
      .map((item)=> catalogLabelByCode(list, item.codigo))
      .filter((value, index, arr)=> value && arr.indexOf(value)===index);
  }
  async function openCreateModal(){
    const modal=await showActionModal({
      title:'Crear sede',
      message:'Completa la informacion para crear una sede.',
      confirmText:'Crear sede',
      fields:[
        { id:'name', label:'Nombre', type:'text', required:true, placeholder:'Nombre de la sede' },
        { id:'dep', label:'Dependencia', type:'datalist', required:true, placeholder:'Selecciona o escribe dependencia', options:catalogOptions(depList) },
        { id:'zone', label:'Zona', type:'datalist', required:true, placeholder:'Selecciona o escribe zona', options:catalogOptions(zoneList) },
        { id:'ops', label:'Nro de operarios', type:'number', required:true, min:'0', step:'1', value:'0' },
        {
          id:'jornada',
          label:'Jornada',
          type:'select',
          value:'lun_vie',
          options:[
            { value:'lun_vie', label:'Lunes a viernes' },
            { value:'lun_sab', label:'Lunes a sabado' },
            { value:'lun_dom', label:'Lunes a domingo' }
          ]
        },
        {
          id:'qrEnabled',
          label:'QR',
          type:'select',
          value:'false',
          options:[
            { value:'false', label:'Inactivo' },
            { value:'true', label:'Activo' }
          ]
        },
        { id:'qrLatitude', label:'Latitud QR', type:'number', step:'0.000001', placeholder:'Ej: 6.244203' },
        { id:'qrLongitude', label:'Longitud QR', type:'number', step:'0.000001', placeholder:'Ej: -75.581212' },
        { id:'qrRadiusMeters', label:'Radio QR (m)', type:'number', min:'1', step:'1', value:'500' }
      ]
    });
    if(!modal.confirmed) return;
    const name=String(modal.values.name||'').trim();
    const depCode=resolveCode(depList, modal.values.dep);
    const zoneCode=resolveCode(zoneList, modal.values.zone);
    const opsRaw=String(modal.values.ops||'').trim();
    const jornada=String(modal.values.jornada||'lun_vie').trim() || 'lun_vie';
    if(!name){ alert('Escribe el nombre de la sede.'); return; }
    if(!depCode){ alert('Selecciona una dependencia valida.'); return; }
    if(!zoneCode){ alert('Selecciona una zona valida.'); return; }
    const ops=Number(opsRaw);
    if(!Number.isFinite(ops) || ops<0 || !Number.isInteger(ops)){ alert('Ingresa un numero entero de operarios valido.'); return; }
    const qrEnabled=String(modal.values.qrEnabled||'false')==='true';
    const qrLatitude=parseOptionalNumber(modal.values.qrLatitude);
    const qrLongitude=parseOptionalNumber(modal.values.qrLongitude);
    const qrRadiusMeters=parsePositiveInteger(modal.values.qrRadiusMeters,500);
    if(qrEnabled && (!Number.isFinite(qrLatitude) || !Number.isFinite(qrLongitude))){ alert('Para activar QR debes configurar latitud y longitud de la sede.'); return; }
    try{
      const code=await deps.getNextSedeCode?.();
      const dep=depList.find(d=>d.codigo===depCode);
      const zone=zoneList.find(z=>z.codigo===zoneCode);
      const id=await deps.createSede?.({
        codigo:code,
        nombre:name,
        dependenciaCodigo:depCode,
        dependenciaNombre:dep?.nombre||null,
        zonaCodigo:zoneCode,
        zonaNombre:zone?.nombre||null,
        numeroOperarios:ops,
        jornada,
        qrEnabled,
        qrLatitude,
        qrLongitude,
        qrRadiusMeters
      });
      await deps.addAuditLog?.({ targetType:'sede', targetId:id, action:'create_sede', after:{ codigo:code, nombre:name, estado:'activo', dependenciaCodigo:depCode, zonaCodigo:zoneCode, numeroOperarios:ops, jornada, qrEnabled, qrLatitude, qrLongitude, qrRadiusMeters } });
      alert('Sede creada OK');
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  const btnOpenCreate=el('button',{id:'btnOpenCreate',className:'btn btn--primary right',type:'button'},['Crear sede']);
  qs('#tabList .form-row',ui)?.append(btnOpenCreate);
  btnOpenCreate.addEventListener('click',openCreateModal);
  let snapshot=[]; const tbody=ui.querySelector('tbody');
  let sortKey=''; let sortDir=1;
  const paginator=createTablePagination(ui,{id:'sedes',after:'#tabList .table-wrap',onChange:render});
  let unDeps=()=>{};
  let unZones=()=>{};

  qs('#btnCreate',ui).addEventListener('click',async()=>{
    const name=qs('#sName',ui).value.trim();
    const depCode=resolveCode(depList, depInput.value);
    const zoneCode=resolveCode(zoneList, zoneInput.value);
    const opsRaw=qs('#sOps',ui).value.trim();
    const jornada=qs('#sJornada',ui).value;
    const qrEnabled=qs('#sQrEnabled',ui).value==='true';
    const qrLatitude=parseOptionalNumber(qs('#sQrLat',ui).value);
    const qrLongitude=parseOptionalNumber(qs('#sQrLng',ui).value);
    const qrRadiusMeters=parsePositiveInteger(qs('#sQrRadius',ui).value,500);
    const msg=qs('#msgCreate',ui); msg.textContent=' ';
    if(!name){ msg.textContent='Escribe el nombre de la sede.'; return; }
    if(!depCode){ msg.textContent='Selecciona una dependencia.'; return; }
    if(!zoneCode){ msg.textContent='Selecciona una zona.'; return; }
    const ops=Number(opsRaw);
    if(!Number.isFinite(ops) || ops<0 || !Number.isInteger(ops)){ msg.textContent='Ingresa un numero entero de operarios valido.'; return; }
    if(qrEnabled && (!Number.isFinite(qrLatitude) || !Number.isFinite(qrLongitude))){ msg.textContent='Para activar QR debes configurar latitud y longitud.'; return; }
    try{
      const code=await deps.getNextSedeCode?.();
      const dep=depList.find(d=>d.codigo===depCode);
      const zone=zoneList.find(z=>z.codigo===zoneCode);
      const id=await deps.createSede?.({
        codigo:code,
        nombre:name,
        dependenciaCodigo:depCode,
        dependenciaNombre:dep?.nombre||null,
        zonaCodigo:zoneCode,
        zonaNombre:zone?.nombre||null,
        numeroOperarios:ops,
        jornada:jornada||'lun_vie',
        qrEnabled,
        qrLatitude,
        qrLongitude,
        qrRadiusMeters
      });
      await deps.addAuditLog?.({ targetType:'sede', targetId:id, action:'create_sede', after:{ codigo:code, nombre:name, estado:'activo', dependenciaCodigo:depCode, zonaCodigo:zoneCode, numeroOperarios:ops, jornada:jornada||'lun_vie', qrEnabled, qrLatitude, qrLongitude, qrRadiusMeters } });
      qs('#sName',ui).value=''; qs('#sOps',ui).value=''; qs('#sQrEnabled',ui).value='false'; qs('#sQrLat',ui).value=''; qs('#sQrLng',ui).value=''; qs('#sQrRadius',ui).value='500'; depInput.value=''; zoneInput.value=''; renderSelects();
      msg.textContent='Sede creada OK'; setTab('list'); setTimeout(()=> msg.textContent=' ',1200);
    }catch(e){ msg.textContent='Error: '+(e?.message||e); }
  });

  const search=()=> qs('#txtSearch',ui).value.trim().toLowerCase();
  const filterStatus=()=> qs('#selStatus',ui).value;
  const depNameByCode=(code)=> depList.find(d=>d.codigo===code)?.nombre || '-';
  const zoneNameByCode=(code)=> zoneList.find(z=>z.codigo===code)?.nombre || '-';
  function toDate(ts){ try{ const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null); return d? d.getTime():0; }catch{ return 0; } }
  function parseOptionalNumber(value){
    const raw=String(value||'').trim();
    if(!raw) return null;
    const n=Number(raw);
    return Number.isFinite(n) ? n : null;
  }
  function parsePositiveInteger(value,fallback){
    const n=Number(String(value||'').trim());
    return Number.isFinite(n) && Number.isInteger(n) && n>0 ? n : fallback;
  }
  function sortValue(s,key){
    if(key==='dependenciaNombre') return (s.dependenciaNombre||depNameByCode(s.dependenciaCodigo)||'').toLowerCase();
    if(key==='zonaNombre') return (s.zonaNombre||zoneNameByCode(s.zonaCodigo)||'').toLowerCase();
    if(key==='numeroOperarios') return Number(s.numeroOperarios||0);
    if(key==='jornada') return String(s.jornada||'lun_vie').toLowerCase();
    if(key==='qrEnabled') return s.qrEnabled===true ? 1 : 0;
    if(key==='createdAt') return toDate(s.createdAt);
    return String(s[key]??'').toLowerCase();
  }
  function sortData(data){
    if(!sortKey) return data;
    const out=[...data];
    out.sort((a,b)=>{
      const va=sortValue(a,sortKey); const vb=sortValue(b,sortKey);
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
    const data=snapshot.filter(s=>{
      const text=[s.codigo,s.nombre,s.dependenciaNombre,depNameByCode(s.dependenciaCodigo),s.zonaNombre,zoneNameByCode(s.zonaCodigo)].join(' ').toLowerCase();
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
    const tdNombre=el('td',{},[s.nombre||'-']);
    const tdDep=el('td',{},[ s.dependenciaNombre||depNameByCode(s.dependenciaCodigo) ]);
    const tdZone=el('td',{},[ s.zonaNombre||zoneNameByCode(s.zonaCodigo) ]);
    const tdOps=el('td',{},[ String(s.numeroOperarios ?? '-') ]);
    const tdJornada=el('td',{},[ labelJornada(s.jornada) ]);
    const tdQr=el('td',{},[ qrBadge(s.qrEnabled) ]);
    const tdQrLocation=el('td',{},[ qrLocationLabel(s) ]);
    const tdEstado=el('td',{},[ statusBadge(s.estado) ]);
    const tdAcc=el('td',{},[ actionsCell(s) ]);
    tr.append(tdCodigo,tdNombre,tdDep,tdZone,tdOps,tdJornada,tdQr,tdQrLocation,tdEstado,tdAcc);
    return tr;
  }
  function labelJornada(v){
    if(v==='lun_sab') return 'Lunes a sabado';
    if(v==='lun_dom') return 'Lunes a domingo';
    return 'Lunes a viernes';
  }
  function statusBadge(st){ return el('span',{className:'badge '+(st==='activo'?'badge--ok':'badge--off')},[st||'-']); }
  function qrBadge(enabled){ return el('span',{className:'badge '+(enabled===true?'badge--ok':'badge--off')},[enabled===true?'Activo':'Inactivo']); }
  function qrLocationLabel(s){
    if(!Number.isFinite(Number(s.qrLatitude)) || !Number.isFinite(Number(s.qrLongitude))) return '-';
    return `${Number(s.qrLatitude).toFixed(6)}, ${Number(s.qrLongitude).toFixed(6)} (${Number(s.qrRadiusMeters||500)} m)`;
  }
  function formatDate(ts){ try{ const d=ts?.toDate? ts.toDate(): (ts? new Date(ts): null); return d? new Date(d).toLocaleString(): '-'; }catch{ return '-'; } }
  function auditInfoData(s){
    const hasMod = Boolean(s.lastModifiedAt || s.lastModifiedByEmail || s.lastModifiedByUid);
    return {
      action: hasMod ? 'Ultima modificacion' : 'Creacion',
      user: hasMod ? (s.lastModifiedByEmail||s.lastModifiedByUid||'-') : (s.createdByEmail||s.createdByUid||'-'),
      date: hasMod ? formatDate(s.lastModifiedAt) : formatDate(s.createdAt)
    };
  }
  function activeQrSedeOptions(){
    return [...snapshot]
      .filter((row)=> row?.qrEnabled===true && String(row?.estado||'activo').trim().toLowerCase()==='activo' && String(row?.codigo||'').trim())
      .sort((a,b)=> String(a.nombre||a.codigo||'').localeCompare(String(b.nombre||b.codigo||'')))
      .map((row)=> ({ value:String(row.codigo||'').trim(), label:`${row.nombre||row.codigo||'-'} (${row.codigo||'-'})` }));
  }
  async function openMoreOptionsModal(s){
    const target=s.estado==='activo'?'inactivo':'activo';
    const modal=await showActionModal({
      title:'Mas opciones',
      message:`Sede: ${s.nombre||'-'}`,
      confirmText:'Continuar',
      fields:[{
        id:'action',
        label:'Accion',
        type:'select',
        required:true,
        options:[
          { value:'', label:'Seleccione...' },
          { value:'edit', label:'Editar' },
          { value:'toggle', label:target==='inactivo'?'Desactivar':'Activar' }
        ]
      }]
    });
    if(!modal.confirmed) return;
    if(modal.values.action==='edit') return openEditSedeModal(s);
    if(modal.values.action==='toggle') return openToggleSedeModal(s);
  }
  async function openRegisterQrDeviceModal(s){
    const canManageQrDevices=can(PERMS.MANAGE_QR_DEVICES);
    if(s.qrEnabled!==true) return alert('Activa QR en la sede para registrar tablets.');
    if(!canManageQrDevices) return alert('No tienes permiso para administrar tablets QR.');
    const sedeOptions=activeQrSedeOptions();
    const modal=await showActionModal({
      title:'Registrar tablet QR',
      message:`Sede: ${s.nombre||s.codigo||'-'}`,
      confirmText:'Generar token',
      fields:[
        { id:'deviceName', label:'Nombre de la tablet', type:'text', required:true, placeholder:'Ej: Tablet recepcion principal' },
        { id:'sedeCodigos', label:'Sedes autorizadas', type:'checkboxes', required:true, value:[s.codigo], options:sedeOptions }
      ]
    });
    if(!modal.confirmed) return;
    const sedeCodigos=Array.isArray(modal.values.sedeCodigos) ? modal.values.sedeCodigos : [modal.values.sedeCodigos].filter(Boolean);
    if(!sedeCodigos.length) return alert('Selecciona al menos una sede.');
    const sedeLabels=sedeCodigos.map((codigo)=>{
      const sede=snapshot.find((row)=> String(row?.codigo||'').trim()===String(codigo||'').trim());
      return sede ? `${sede.nombre||sede.codigo||'-'} (${sede.codigo||'-'})` : String(codigo||'').trim();
    });
    try{
      const result=await deps.createQrDevice?.({ sedeCodigo:s.codigo, sedeCodigos, deviceName:modal.values.deviceName });
      const token=String(result?.deviceToken||'').trim();
      showInfoModal('Tablet QR registrada',[
        `Sedes: ${sedeLabels.join(', ')}`,
        `Tablet: ${modal.values.deviceName}`,
        'Abre Lector QR en la tablet y pega este token de dispositivo:',
        token
      ]);
      await deps.addAuditLog?.({ targetType:'sede', targetId:s.id, action:'create_qr_device', after:{ sedeCodigo:s.codigo, sedeCodigos, deviceName:modal.values.deviceName } });
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  async function openToggleSedeModal(s){
    const target=s.estado==='activo'?'inactivo':'activo';
    const modal=await showActionModal({
      title:`${target==='inactivo'?'Desactivar':'Activar'} sede`,
      message:`Sede: ${s.nombre||'-'}`,
      confirmText:target==='inactivo'?'Desactivar':'Activar',
      fields:[{ id:'detail', label:'Detalle', type:'textarea', required:true, placeholder:'Escribe el motivo o detalle de esta accion' }]
    });
    if(!modal.confirmed) return;
    try{ await deps.setSedeStatus?.(s.id,target); await deps.addAuditLog?.({ targetType:'sede', targetId:s.id, action: target==='activo'?'activate_sede':'deactivate_sede', before:{estado:s.estado}, after:{estado:target}, note: modal.values.detail||null }); }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  async function openEditSedeModal(s){
    const modal=await showActionModal({
      title:'Editar sede',
      message:`Sede: ${s.nombre||'-'}`,
      confirmText:'Guardar cambios',
      fields:[
        { id:'codigo', label:'Codigo', type:'text', required:true, value:s.codigo||'' },
        { id:'nombre', label:'Nombre', type:'text', required:true, value:s.nombre||'' },
        { id:'dependencia', label:'Dependencia', type:'datalist', required:true, placeholder:'Selecciona o escribe dependencia', value:labelByCode(depList,s.dependenciaCodigo||''), options:catalogOptions(depList) },
        { id:'zona', label:'Zona', type:'datalist', required:true, placeholder:'Selecciona o escribe zona', value:labelByCode(zoneList,s.zonaCodigo||''), options:catalogOptions(zoneList) },
        { id:'numeroOperarios', label:'Nro de operarios', type:'number', required:true, min:'0', step:'1', value:String(s.numeroOperarios ?? '') },
        {
          id:'jornada',
          label:'Jornada',
          type:'select',
          value:s.jornada||'lun_vie',
          options:[
            { value:'lun_vie', label:'Lunes a viernes' },
            { value:'lun_sab', label:'Lunes a sabado' },
            { value:'lun_dom', label:'Lunes a domingo' }
          ]
        },
        {
          id:'qrEnabled',
          label:'QR',
          type:'select',
          value:s.qrEnabled===true?'true':'false',
          options:[
            { value:'false', label:'Inactivo' },
            { value:'true', label:'Activo' }
          ]
        },
        { id:'qrLatitude', label:'Latitud QR', type:'number', step:'0.000001', value:s.qrLatitude ?? '', placeholder:'Ej: 6.244203' },
        { id:'qrLongitude', label:'Longitud QR', type:'number', step:'0.000001', value:s.qrLongitude ?? '', placeholder:'Ej: -75.581212' },
        { id:'qrRadiusMeters', label:'Radio QR (m)', type:'number', min:'1', step:'1', value:String(s.qrRadiusMeters || 500) },
        { id:'detail', label:'Detalle de la modificacion', type:'textarea', required:true, placeholder:'Describe brevemente el cambio realizado' }
      ]
    });
    if(!modal.confirmed) return;
    const newCode=String(modal.values.codigo||'').trim();
    const newName=String(modal.values.nombre||'').trim();
    const newDepCode=resolveCode(depList, modal.values.dependencia);
    const newZoneCode=resolveCode(zoneList, modal.values.zona);
    const newOpsRaw=String(modal.values.numeroOperarios||'').trim();
    const newJornada=String(modal.values.jornada||'lun_vie').trim() || 'lun_vie';
    const newQrEnabled=String(modal.values.qrEnabled||'false')==='true';
    const newQrLatitude=parseOptionalNumber(modal.values.qrLatitude);
    const newQrLongitude=parseOptionalNumber(modal.values.qrLongitude);
    const newQrRadiusMeters=parsePositiveInteger(modal.values.qrRadiusMeters,500);
    if(!newCode||!newName) return alert('Completa codigo y nombre.');
    if(!newDepCode||!newZoneCode) return alert('Selecciona dependencia y zona.');
    const newOps=Number(newOpsRaw);
    if(!Number.isFinite(newOps) || newOps<0 || !Number.isInteger(newOps)) return alert('Ingresa un numero entero de operarios valido.');
    if(newQrEnabled && (!Number.isFinite(newQrLatitude) || !Number.isFinite(newQrLongitude))) return alert('Para activar QR debes configurar latitud y longitud.');
    try{
      if(newCode!==s.codigo){ const dup=await deps.findSedeByCode?.(newCode); if(dup && dup.id!==s.id) return alert('Ya existe una sede con ese codigo.'); }
      const newDep=depList.find(d=>d.codigo===newDepCode);
      const newZone=zoneList.find(z=>z.codigo===newZoneCode);
      await deps.updateSede?.(s.id,{
        codigo:newCode,
        nombre:newName,
        dependenciaCodigo:newDepCode,
        dependenciaNombre:newDep?.nombre||null,
        zonaCodigo:newZoneCode,
        zonaNombre:newZone?.nombre||null,
        numeroOperarios:newOps,
        jornada:newJornada,
        qrEnabled:newQrEnabled,
        qrLatitude:newQrLatitude,
        qrLongitude:newQrLongitude,
        qrRadiusMeters:newQrRadiusMeters
      });
      await deps.addAuditLog?.({ targetType:'sede', targetId:s.id, action:'update_sede', before:{ codigo:s.codigo, nombre:s.nombre, dependenciaCodigo:s.dependenciaCodigo, zonaCodigo:s.zonaCodigo, numeroOperarios:s.numeroOperarios, jornada:s.jornada||'lun_vie', qrEnabled:s.qrEnabled===true, qrLatitude:s.qrLatitude, qrLongitude:s.qrLongitude, qrRadiusMeters:s.qrRadiusMeters }, after:{ codigo:newCode, nombre:newName, dependenciaCodigo:newDepCode, zonaCodigo:newZoneCode, numeroOperarios:newOps, jornada:newJornada, qrEnabled:newQrEnabled, qrLatitude:newQrLatitude, qrLongitude:newQrLongitude, qrRadiusMeters:newQrRadiusMeters }, note: modal.values.detail||null });
    }catch(e){ alert('Error: '+(e?.message||e)); }
  }
  function actionsCell(s){
    const box=el('div',{className:'row-actions'},[]);
    const canManageQrDevices=can(PERMS.MANAGE_QR_DEVICES);
    const qrDisabledReason=s.qrEnabled!==true?'Activa QR en la sede para registrar tablets':'No tienes permiso para administrar tablets QR';
    const btnInfo=el('button',{className:'btn btn--icon',type:'button',title:'Ver informacion','aria-label':'Ver informacion'},['\u24D8']);
    btnInfo.addEventListener('click',()=>{ const info=auditInfoData(s); showInfoModal('Informacion del registro',[`Evento: ${info.action}`,`Usuario: ${info.user}`,`Fecha: ${info.date}`]); });
    const btnQr=el('button',{className:'btn btn--icon',type:'button',title:canManageQrDevices && s.qrEnabled===true?'Registrar tablet QR':qrDisabledReason,'aria-label':'Registrar tablet QR'},['QR']);
    btnQr.disabled=s.qrEnabled!==true || !canManageQrDevices;
    btnQr.addEventListener('click',()=> openRegisterQrDeviceModal(s));
    const btnMore=el('button',{className:'btn btn--icon',type:'button',title:'Mas opciones','aria-label':'Mas opciones'},['\u22EF']);
    btnMore.addEventListener('click',()=> openMoreOptionsModal(s));
    box.append(btnMore,btnQr,btnInfo); return box;
  }
  qs('#txtSearch',ui).addEventListener('input',()=>{ paginator.reset(); render(); });
  qs('#selStatus',ui).addEventListener('change',()=>{ paginator.reset(); render(); });
  initSorting();
  mount.replaceChildren(ui);
  let un=()=>{};
  try{
    unDeps=deps.streamDependencies?.((arr)=>{ depList=(arr||[]).filter(d=>d.estado!=='inactivo'); renderSelects(); render(); }) || (()=>{});
    unZones=deps.streamZones?.((arr)=>{ zoneList=(arr||[]).filter(z=>z.estado!=='inactivo'); renderSelects(); render(); }) || (()=>{});
    un=deps.streamSedes?.((arr)=>{ snapshot=arr||[]; render(); }) || (()=>{});
  }catch(e){
    const msg=qs('#msg',ui); if(msg) msg.textContent='Error cargando sedes: '+(e?.message||e);
  }
  return ()=>{ un?.(); unDeps?.(); unZones?.(); };
};
