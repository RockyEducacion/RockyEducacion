import { el, qs } from '../utils/dom.js';

export const CargueMasivoAdmin=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Cargue masivo de empleados']),
    el('p',{className:'text-muted mt-2'},['Columnas esperadas: documento, nombre, telefono, cargo codigo, sede codigo, fecha ingreso. El código del empleado se genera automáticamente y el teléfono se guarda con prefijo 57.']),
    el('div',{className:'form-row mt-2'},[
      el('button',{id:'btnTemplate',className:'btn',type:'button'},['Descargar plantilla CSV']),
      el('input',{id:'fileInput',className:'input',type:'file',accept:'.csv,.xls,.xlsx'}),
      el('button',{id:'btnValidate',className:'btn btn--primary'},['Validar archivo']),
      el('button',{id:'btnImport',className:'btn',disabled:true},['Importar empleados']),
      el('span',{id:'msg',className:'text-muted'},[' '])
    ]),
    el('div',{id:'importProgress',className:'bulk-progress hidden','aria-live':'polite'},[
      el('div',{className:'bulk-progress__meta'},[
        el('span',{id:'progressLabel',className:'bulk-progress__label'},['Listo para importar']),
        el('span',{id:'progressNumbers',className:'bulk-progress__numbers text-muted'},['0 / 0'])
      ]),
      el('div',{className:'bulk-progress__track',role:'progressbar','aria-valuemin':'0','aria-valuemax':'100','aria-valuenow':'0','aria-label':'Progreso de importacion'},[
        el('div',{id:'progressFill',className:'bulk-progress__fill',style:'width:0%'})
      ])
    ]),
    el('div',{className:'divider'}),
    el('div',{className:'form-row'},[
      el('div',{},[ el('label',{className:'label'},['Filas leidas']), el('input',{id:'sumRows',className:'input',disabled:true}) ]),
      el('div',{},[ el('label',{className:'label'},['Validas']), el('input',{id:'sumOk',className:'input',disabled:true}) ]),
      el('div',{},[ el('label',{className:'label'},['Errores']), el('input',{id:'sumErr',className:'input',disabled:true}) ])
    ]),
    el('div',{className:'mt-2 table-wrap'},[
      el('table',{className:'table',id:'tblPreview'},[
        el('thead',{},[ el('tr',{},[
          el('th',{},['Documento']),
          el('th',{},['Nombre']),
          el('th',{},['Telefono']),
          el('th',{},['Cargo codigo']),
          el('th',{},['Sede codigo']),
          el('th',{},['Cargo']),
          el('th',{},['Sede']),
          el('th',{},['Fecha ingreso']),
          el('th',{},['Estado'])
        ]) ]),
        el('tbody',{})
      ])
    ]),
    el('div',{className:'mt-2 table-wrap'},[
      el('table',{className:'table',id:'tblErrors'},[
        el('thead',{},[ el('tr',{},[ el('th',{},['Fila']), el('th',{},['Error']) ]) ]),
        el('tbody',{})
      ])
    ])
  ]);

  const msg=qs('#msg',ui);
  const btnImport=qs('#btnImport',ui);
  const btnValidate=qs('#btnValidate',ui);
  const fileInput=qs('#fileInput',ui);
  const btnTemplate=qs('#btnTemplate',ui);
  const progressBox=qs('#importProgress',ui);
  const progressLabel=qs('#progressLabel',ui);
  const progressNumbers=qs('#progressNumbers',ui);
  const progressFill=qs('#progressFill',ui);
  const progressTrack=ui.querySelector('.bulk-progress__track');
  let employees=[]; let cargos=[]; let sedes=[];
  let validRows=[];

  const unEmp=deps.streamEmployees?.((arr)=>{ employees=arr||[]; });
  const unCargo=deps.streamCargos?.((arr)=>{ cargos=arr||[]; });
  const unSede=deps.streamSedes?.((arr)=>{ sedes=arr||[]; });

  qs('#btnValidate',ui).addEventListener('click',async()=>{
    msg.textContent='Validando archivo...';
    btnImport.disabled=true;
    validRows=[];
    try{
      const file=fileInput.files?.[0];
      if(!file){ msg.textContent='Selecciona un archivo CSV/XLS/XLSX.'; return; }
      resetProgress();
      const rows=await readInputFile(file);
      const result=validateRows(rows, employees, cargos, sedes);
      renderSummary(result.rows.length, result.valid.length, result.errors.length);
      renderPreview(result.preview);
      renderErrors(result.errors);
      validRows=result.valid;
      btnImport.disabled=result.valid.length===0;
      msg.textContent=result.errors.length? 'Validacion finalizada con errores.' : 'Archivo valido. Puedes importar.';
    }catch(e){
      msg.textContent='Error: '+(e?.message||e);
    }
  });

  btnImport.addEventListener('click',async()=>{
    if(!validRows.length){ msg.textContent='No hay filas validas para importar.'; return; }
    btnImport.disabled=true;
    btnValidate.disabled=true;
    btnTemplate.disabled=true;
    fileInput.disabled=true;
    showProgress();
    updateProgress({ created:0, total:validRows.length, percent:0, phase:'preparing' });
    msg.textContent='Importando empleados...';
    try{
      const out=await deps.createEmployeesBulk?.(validRows,{
        chunkSize:250,
        onProgress:updateProgress
      });
      await deps.addAuditLog?.({
        targetType:'employee',
        action:'bulk_create_employees',
        after:{ total: out?.created||validRows.length }
      });
      msg.textContent=`Importacion completada. Creados: ${out?.created||validRows.length}`;
      updateProgress({ created:out?.created||validRows.length, total:validRows.length, percent:100, phase:'completed' });
      validRows=[];
    }catch(e){
      msg.textContent='Error al importar: '+(e?.message||e);
      btnImport.disabled=false;
      updateProgress({ created:0, total:validRows.length, percent:0, phase:'error' });
    } finally {
      btnValidate.disabled=false;
      btnTemplate.disabled=false;
      fileInput.disabled=false;
    }
  });

  btnTemplate.addEventListener('click',()=>{
    const headers=['documento','nombre','telefono','cargo codigo','sede codigo','fecha ingreso'];
    const sampleA=['10000001','Empleado ejemplo','573000000000','CAR-0001','SED-0001','2026-02-13'];
    const sampleB=['10000002','Empleado ejemplo 2','3000000001','CAR-0002','SED-0002','2026-02-14'];
    downloadCsv('plantilla_empleados.csv',[headers,sampleA,sampleB]);
  });

  function renderSummary(total, ok, err){
    qs('#sumRows',ui).value=String(total||0);
    qs('#sumOk',ui).value=String(ok||0);
    qs('#sumErr',ui).value=String(err||0);
  }

  function showProgress(){
    progressBox.classList.remove('hidden');
  }

  function resetProgress(){
    progressLabel.textContent='Listo para importar';
    progressNumbers.textContent='0 / 0';
    progressFill.style.width='0%';
    progressTrack?.setAttribute('aria-valuenow','0');
  }

  function updateProgress({ created=0, total=0, percent=0, phase='importing' } = {}){
    const labels={
      preparing:'Preparando importacion...',
      importing:'Importando empleados...',
      refreshing:'Actualizando vista...',
      completed:'Importacion completada',
      error:'Importacion interrumpida'
    };
    showProgress();
    const safePercent=Math.max(0,Math.min(100,Number(percent)||0));
    progressLabel.textContent=labels[phase]||'Importando empleados...';
    progressNumbers.textContent=`${created} / ${total}`;
    progressFill.style.width=`${safePercent}%`;
    progressTrack?.setAttribute('aria-valuenow',String(safePercent));
  }

  function renderPreview(rows){
    const tb=qs('#tblPreview tbody',ui);
    tb.replaceChildren(...rows.map(r=>el('tr',{},[
      el('td',{},[r.documento||'-']),
      el('td',{},[r.nombre||'-']),
      el('td',{},[r.telefono||'-']),
      el('td',{},[r.cargoCodigo||'-']),
      el('td',{},[r.sedeCodigo||'-']),
      el('td',{},[r.cargoNombre||'-']),
      el('td',{},[r.sedeNombre||'-']),
      el('td',{},[r.fechaIngreso||'-']),
      el('td',{},[r.ok? 'OK':'ERROR'])
    ])));
  }

  function renderErrors(errors){
    const tb=qs('#tblErrors tbody',ui);
    tb.replaceChildren(...errors.map(err=>el('tr',{},[
      el('td',{},[String(err.row)]),
      el('td',{},[err.message||'Error'])
    ])));
  }

  function validateRows(rows, employeesList, cargosList, sedesList){
    const existingDocs=new Set((employeesList||[]).map(e=> String(e.documento||'').trim()).filter(Boolean));
    const localDocs=new Set();
    const cargoByCode=new Map((cargosList||[]).map(c=> [String(c.codigo||'').trim().toLowerCase(), c]));
    const sedeByCode=new Map((sedesList||[]).map(s=> [String(s.codigo||'').trim().toLowerCase(), s]));
    const errors=[]; const valid=[]; const preview=[];

    rows.forEach((raw,idx)=>{
      const rowNum=idx+2;
      const documento=String(raw.documento||'').trim();
      const nombre=String(raw.nombre||'').trim();
      const telefono=String(raw.telefono||'').trim();
      const cargoCode=String(raw.cargoCodigo||raw.cargo||'').trim().toLowerCase();
      const sedeCode=String(raw.sedeCodigo||raw.sede||'').trim().toLowerCase();
      const fechaIngreso=normalizeDate(raw.fechaIngreso||raw.fecha_ingreso||raw.fecha||'');
      const issues=[];
      if(!documento) issues.push('Documento requerido.');
      if(!nombre) issues.push('Nombre requerido.');
      if(!telefono) issues.push('Telefono requerido.');
      if(!cargoCode) issues.push('Cargo codigo requerido.');
      if(!sedeCode) issues.push('Sede codigo requerida.');
      if(!fechaIngreso) issues.push('Fecha ingreso invalida.');
      const cargo=cargoByCode.get(cargoCode);
      const sede=sedeByCode.get(sedeCode);
      if(cargoCode && !cargo) issues.push(`Cargo no existe: ${cargoCode}`);
      if(sedeCode && !sede) issues.push(`Sede no existe: ${sedeCode}`);
      if(documento && existingDocs.has(documento)) issues.push('Documento ya existe en empleados.');
      if(documento && localDocs.has(documento)) issues.push('Documento duplicado en archivo.');
      if(documento) localDocs.add(documento);

      if(issues.length){
        errors.push({ row:rowNum, message: issues.join(' ') });
        preview.push({ documento, nombre, telefono, cargoCodigo:raw.cargoCodigo||raw.cargo||'', sedeCodigo:raw.sedeCodigo||raw.sede||'', cargoNombre:cargo?.nombre||'', sedeNombre:sede?.nombre||'', fechaIngreso, ok:false });
        return;
      }

      valid.push({
        documento,
        nombre,
        telefono,
        cargoCodigo:cargo.codigo,
        cargoNombre:cargo.nombre,
        sedeCodigo:sede.codigo,
        sedeNombre:sede.nombre,
        fechaIngreso: new Date(`${fechaIngreso}T00:00:00`)
      });
      preview.push({ documento, nombre, telefono, cargoCodigo:cargo.codigo, sedeCodigo:sede.codigo, cargoNombre:cargo.nombre, sedeNombre:sede.nombre, fechaIngreso, ok:true });
    });

    return { rows, valid, errors, preview };
  }

  async function readInputFile(file){
    const name=(file.name||'').toLowerCase();
    if(name.endsWith('.csv')) return parseCSVRows(await file.text());
    if(name.endsWith('.xls') || name.endsWith('.xlsx')){
      const mod=await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const buff=await file.arrayBuffer();
      const wb=mod.read(buff, { type:'array' });
      const first=wb.SheetNames[0];
      const ws=wb.Sheets[first];
      const rows=mod.utils.sheet_to_json(ws,{ defval:'' });
      return rows.map(r=> normalizeInputRow(r));
    }
    throw new Error('Formato no soportado. Usa CSV/XLS/XLSX.');
  }

  function parseCSVRows(text){
    const rows=[]; let row=[]; let cur=''; let inQuotes=false;
    for(let i=0;i<text.length;i++){
      const ch=text[i]; const next=text[i+1];
      if(ch==='\"'){
        if(inQuotes && next==='\"'){ cur+='\"'; i++; } else { inQuotes=!inQuotes; }
      } else if((ch===',' || ch===';' || ch==='\t') && !inQuotes){
        row.push(cur); cur='';
      } else if((ch==='\n' || ch==='\r') && !inQuotes){
        if(cur!=='' || row.length){ row.push(cur); rows.push(row); row=[]; cur=''; }
      } else {
        cur+=ch;
      }
    }
    if(cur!=='' || row.length){ row.push(cur); rows.push(row); }
    if(!rows.length) return [];
    const headers=rows[0].map(h=> String(h||'').trim());
    return rows.slice(1).map(cols=>{
      const obj={}; headers.forEach((h,i)=>{ obj[h]=cols[i]??''; }); return normalizeInputRow(obj);
    });
  }

  function normalizeInputRow(obj){
    const out={ documento:'', nombre:'', telefono:'', cargoCodigo:'', sedeCodigo:'', fechaIngreso:'' };
    Object.keys(obj||{}).forEach((k)=>{
      const key=String(k||'').trim().toLowerCase();
      const v=String(obj[k]??'').trim();
      if(key==='documento' || key==='doc') out.documento=v;
      if(key==='nombre' || key==='nombre completo') out.nombre=v;
      if(key==='telefono' || key==='celular' || key==='numero cel') out.telefono=v;
      if(key==='cargo codigo' || key==='cargo_codigo' || key==='cargo') out.cargoCodigo=v;
      if(key==='sede codigo' || key==='sede_codigo' || key==='sede') out.sedeCodigo=v;
      if(key==='fecha ingreso' || key==='fecha_ingreso' || key==='fecha') out.fechaIngreso=v;
    });
    return out;
  }

  function normalizeDate(value){
    const v=String(value||'').trim();
    if(!v) return '';
    if(/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
    const parts=v.split(/[\/\-.]/).map(p=> p.trim()).filter(Boolean);
    if(parts.length===3){
      let d=''; let m=''; let y='';
      if(parts[0].length===4){ y=parts[0]; m=parts[1]; d=parts[2]; }
      else { d=parts[0]; m=parts[1]; y=parts[2]; }
      let yy=Number(y); const dd=Number(d); const mm=Number(m);
      if(!Number.isFinite(yy)||!Number.isFinite(dd)||!Number.isFinite(mm)) return '';
      if(y.length===2) yy=2000+yy;
      if(dd<1||dd>31||mm<1||mm>12) return '';
      return `${String(yy).padStart(4,'0')}-${String(mm).padStart(2,'0')}-${String(dd).padStart(2,'0')}`;
    }
    return '';
  }

  function downloadCsv(filename, rows){
    const csv=rows.map(r=> r.map(csvCell).join(',')).join('\n');
    const blob=new Blob([csv],{ type:'text/csv;charset=utf-8;' });
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a');
    a.href=url;
    a.download=filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  function csvCell(value){
    const v=String(value??'');
    if(v.includes(',') || v.includes('"') || v.includes('\n')) return `"${v.replace(/"/g,'""')}"`;
    return v;
  }

  mount.replaceChildren(ui);
  resetProgress();
  return ()=>{ unEmp?.(); unCargo?.(); unSede?.(); };
};
