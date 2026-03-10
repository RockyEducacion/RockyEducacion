import { el, qs, enableSectionToggles } from '../utils/dom.js';

export const Absenteeism=(mount,deps={})=>{
  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Ausentismo y pago por dependencia']),
    el('div',{className:'form-row mt-2'},[
      el('div',{},[ el('label',{className:'label'},['Fecha']), el('input',{id:'opDate',className:'input',type:'date'}) ]),
      el('button',{id:'btnRun',className:'btn btn--primary',type:'button'},['Consultar fecha']),
      el('button',{id:'btnExportSummary',className:'btn',type:'button'},['Exportar resumen Excel']),
      el('button',{id:'btnExportSede',className:'btn',type:'button'},['Exportar sedes Excel']),
      el('button',{id:'btnExportDetail',className:'btn',type:'button'},['Exportar detalle Excel']),
      el('span',{id:'msg',className:'text-muted'},[' '])
    ]),
    el('div',{className:'section-block mt-2'},[
      el('h3',{className:'section-title'},['Resumen por dependencia']),
      el('div',{className:'table-wrap'},[
        el('table',{className:'table',id:'tblDependency'},[
          el('thead',{},[el('tr',{},[
            el('th',{'data-sort-dep':'dependenciaNombre',style:'cursor:pointer'},['Dependencia']),
            el('th',{'data-sort-dep':'planeados',style:'cursor:pointer'},['Planeados']),
            el('th',{'data-sort-dep':'contratados',style:'cursor:pointer'},['Contratados']),
            el('th',{'data-sort-dep':'noContratado',style:'cursor:pointer'},['No contratado']),
            el('th',{'data-sort-dep':'novSinReemplazo',style:'cursor:pointer'},['Novedad sin reemplazo']),
            el('th',{'data-sort-dep':'ausentismoTotal',style:'cursor:pointer'},['Total ausentismo']),
            el('th',{'data-sort-dep':'totalPagar',style:'cursor:pointer'},['Total a pagar']),
            el('th',{},['Detalle'])
          ])]),
          el('tbody',{})
        ])
      ]),
      el('p',{id:'totDependency',className:'text-muted'},['Total dependencias - Planeados: 0, Contratados: 0, No contratado: 0, Novedad sin reemplazo: 0, Total ausentismo: 0, Total a pagar: 0'])
    ]),
    el('div',{className:'section-block mt-2'},[
      el('h3',{className:'section-title'},['Resumen por sede']),
      el('div',{className:'table-wrap'},[
        el('table',{className:'table',id:'tblTotals'},[
          el('thead',{},[el('tr',{},[
            el('th',{'data-sort-sede':'sedeNombre',style:'cursor:pointer'},['Sede']),
            el('th',{'data-sort-sede':'planeados',style:'cursor:pointer'},['Planeados']),
            el('th',{'data-sort-sede':'contratados',style:'cursor:pointer'},['Contratados']),
            el('th',{'data-sort-sede':'noContratado',style:'cursor:pointer'},['No contratado']),
            el('th',{'data-sort-sede':'novSinReemplazo',style:'cursor:pointer'},['Novedad sin reemplazo']),
            el('th',{'data-sort-sede':'ausentismoTotal',style:'cursor:pointer'},['Total ausentismo']),
            el('th',{'data-sort-sede':'totalPagar',style:'cursor:pointer'},['Total a pagar']),
            el('th',{},['Detalle'])
          ])]),
          el('tbody',{})
        ])
      ]),
      el('p',{id:'totRange',className:'text-muted'},['Total rango a pagar: 0'])
    ]),
    el('div',{className:'section-block mt-2'},[
      el('h3',{id:'detailTitle',className:'section-title'},['Detalle dependencia']),
      el('div',{className:'table-wrap'},[
        el('table',{className:'table',id:'tblDetail'},[
          el('thead',{},[el('tr',{},[
            el('th',{'data-sort-detail':'fecha',style:'cursor:pointer'},['Fecha']),
            el('th',{'data-sort-detail':'sede',style:'cursor:pointer'},['Sede']),
            el('th',{'data-sort-detail':'documento',style:'cursor:pointer'},['Documento']),
            el('th',{'data-sort-detail':'nombre',style:'cursor:pointer'},['Nombre']),
            el('th',{'data-sort-detail':'estado',style:'cursor:pointer'},['Estado'])
          ])]),
          el('tbody',{})
        ])
      ])
    ])
  ]);

  const msg=qs('#msg',ui);
  const day=todayBogota();
  qs('#opDate',ui).value=day;

  let sedeDailyRows=[];
  let dependencyRows=[];
  let attendanceByKey=new Map();
  let replByEmpDate=new Map();
  let replacementSuperByDateDoc=new Set();
  let novedadRules={ byCode:new Map(), byName:new Map() };
  let totalsRows=[];
  let detailRowsCache=[];
  let depSortKey='dependenciaNombre';
  let depSortDir=1;
  let sedeSortKey='sedeNombre';
  let sedeSortDir=1;
  let detailSortKey='fecha';
  let detailSortDir=-1;

  qs('#btnRun',ui).addEventListener('click', run);
  qs('#btnExportSummary',ui).addEventListener('click',()=> exportSummaryExcel());
  qs('#btnExportSede',ui).addEventListener('click',()=> exportSedeExcel());
  qs('#btnExportDetail',ui).addEventListener('click',()=> exportDetailExcel());
  ui.querySelectorAll('#tblDependency th[data-sort-dep]').forEach((th)=>{
    th.addEventListener('click',()=>{
      const key=String(th.getAttribute('data-sort-dep')||'').trim();
      if(!key) return;
      if(depSortKey===key) depSortDir*=-1;
      else { depSortKey=key; depSortDir=1; }
      renderDependency(qs('#opDate',ui).value);
    });
  });
  ui.querySelectorAll('#tblTotals th[data-sort-sede]').forEach((th)=>{
    th.addEventListener('click',()=>{
      const key=String(th.getAttribute('data-sort-sede')||'').trim();
      if(!key) return;
      if(sedeSortKey===key) sedeSortDir*=-1;
      else { sedeSortKey=key; sedeSortDir=1; }
      renderTotals();
    });
  });
  ui.querySelectorAll('#tblDetail th[data-sort-detail]').forEach((th)=>{
    th.addEventListener('click',()=>{
      const key=String(th.getAttribute('data-sort-detail')||'').trim();
      if(!key) return;
      if(detailSortKey===key) detailSortDir*=-1;
      else { detailSortKey=key; detailSortDir=1; }
      renderDetailRows();
    });
  });

  function sortValue(row,key){
    const value=row?.[key];
    if(typeof value==='number') return value;
    return String(value??'').toLowerCase();
  }

  function sortRows(rows,key,dir){
    return [...(rows||[])].sort((a,b)=>{
      const va=sortValue(a,key);
      const vb=sortValue(b,key);
      if(va===vb) return 0;
      return va>vb?dir:-dir;
    });
  }

  function updateSortIndicators(selector,attrName,activeKey,dir){
    ui.querySelectorAll(selector).forEach((th)=>{
      const base=th.dataset.baseLabel||th.textContent.replace(/\s[\^v▲▼]$/,'');
      th.dataset.baseLabel=base;
      const key=String(th.getAttribute(attrName)||'').trim();
      th.textContent=key && key===activeKey ? `${base} ${dir===1?'▲':'▼'}` : base;
    });
  }

  async function run(){
    const date=qs('#opDate',ui).value;
    if(!date){ msg.textContent='Selecciona una fecha.'; return; }
    const dayClosed=await deps.isOperationDayClosed?.(date);
    if(!dayClosed){
      sedeDailyRows=[]; dependencyRows=[]; totalsRows=[]; detailRowsCache=[];
      qs('#tblDependency tbody',ui).replaceChildren();
      qs('#tblTotals tbody',ui).replaceChildren();
      qs('#tblDetail tbody',ui).replaceChildren();
      qs('#totDependency',ui).textContent='Total dependencias - Planeados: 0, Contratados: 0, No contratado: 0, Novedad sin reemplazo: 0, Total ausentismo: 0, Total a pagar: 0';
      qs('#totRange',ui).textContent='Total rango a pagar: 0';
      msg.textContent='La fecha seleccionada no esta cerrada. El CRUC solo muestra dias cerrados.';
      return;
    }
    msg.textContent='Consultando...';
    try{
      const [sedeStatus, attendance, replacements, sedes, novedades, employees, supernumerarios]=await Promise.all([
        deps.listSedeStatusRange?.(date,date) || [],
        deps.listAttendanceRange?.(date,date) || [],
        deps.listImportReplacementsRange?.(date,date) || [],
        loadSedesSnapshot(),
        loadNovedadesSnapshot(),
        loadEmployeesSnapshot(),
        loadSupernumerariosSnapshot()
      ]);
      novedadRules=buildNovedadReplacementRules(novedades||[]);

      const sedeMetaByCode=new Map();
      (sedes||[]).forEach((s)=>{
        sedeMetaByCode.set(String(s.codigo||''), {
          dependenciaCodigo: String(s.dependenciaCodigo||'').trim(),
          dependenciaNombre: String(s.dependenciaNombre||'').trim()
        });
      });

      replByEmpDate=new Map();
      replacementSuperByDateDoc=new Set();
      (replacements||[]).forEach((r)=>{
        const empKey=`${r.fecha||''}|${r.empleadoId||''}`;
        replByEmpDate.set(empKey,r);
        if(r.decision==='reemplazo'){
          const superDoc=String(r.supernumerarioDocumento||'').trim();
          if(superDoc){
            replacementSuperByDateDoc.add(`${r.fecha||''}|${superDoc}`);
          }
        }
      });
      attendanceByKey=new Map();
      (attendance||[]).forEach((a)=>{
        const attDoc=String(a.documento||'').trim();
        if(attDoc && replacementSuperByDateDoc.has(`${a.fecha||''}|${attDoc}`)) return;
        const key=`${a.fecha||''}|${a.sedeCodigo||''}`;
        if(!attendanceByKey.has(key)) attendanceByKey.set(key,[]);
        attendanceByKey.get(key).push(a);
      });

      const statusBySede=new Map((sedeStatus||[]).map((s)=> [String(s.sedeCodigo||''), s]));
      const superDocs=new Set(
        (supernumerarios||[])
          .filter((s)=> isEmployeeExpectedForDate(s,date,sedes))
          .map((s)=> String(s.documento||'').trim())
          .filter(Boolean)
      );
      const contratadosBySede=new Map();
      (employees||[]).forEach((e)=>{
        if(!isEmployeeExpectedForDate(e,date,sedes)) return;
        const doc=String(e.documento||'').trim();
        if(doc && superDocs.has(doc)) return;
        const sedeCode=String(e.sedeCodigo||'').trim();
        if(!sedeCode) return;
        contratadosBySede.set(sedeCode, Number(contratadosBySede.get(sedeCode)||0)+1);
      });

      sedeDailyRows=(sedes||[])
        .filter((s)=> String(s.estado||'activo').trim().toLowerCase()!=='inactivo')
        .map((s)=>{
        const sedeCode=String(s.codigo||'').trim();
        const key=`${date}|${sedeCode}`;
        const atts=attendanceByKey.get(key)||[];
        const status=statusBySede.get(sedeCode)||{};
        const planeadosRaw=s.numeroOperarios ?? status.operariosPlaneados ?? status.operariosEsperados ?? 0;
        const planeados=parseOperatorCount(planeadosRaw);
        const contratados=Number(contratadosBySede.get(sedeCode)||0);
        const noContratado=Math.max(0, planeados-contratados);
        const novSinReemplazo=atts.filter((a)=>{
          if(a.asistio===true) return false;
          const rep=replByEmpDate.get(`${a.fecha||''}|${a.empleadoId||''}`);
          if(rep && rep.decision==='reemplazo') return false;
          return attendanceRequiresReplacementForSummary(a,novedadRules);
        }).length;
        const ausentismoTotal=noContratado+novSinReemplazo;
        const totalPagar=Math.max(0, planeados-noContratado-novSinReemplazo);
        const meta=sedeMetaByCode.get(sedeCode)||{};
        const dependenciaCodigo=String(meta.dependenciaCodigo||'').trim();
        const dependenciaNombre=String(meta.dependenciaNombre||'').trim()||'Sin dependencia';
        const dependenciaKey=dependenciaCodigo || `NO_DEP:${dependenciaNombre}`;
        return {
          fecha:date,
          sedeCodigo:sedeCode,
          sedeNombre:String(s.nombre||sedeCode||'-'),
          dependenciaCodigo,
          dependenciaNombre,
          dependenciaKey,
          planeados,
          contratados,
          noContratado,
          novSinReemplazo,
          ausentismoTotal,
          totalPagar
        };
      }).sort((a,b)=> (a.fecha+a.sedeNombre).localeCompare(b.fecha+b.sedeNombre));

      const depMap=new Map();
      sedeDailyRows.forEach((r)=>{
        if(!depMap.has(r.dependenciaKey)){
          depMap.set(r.dependenciaKey,{
            dependenciaKey:r.dependenciaKey,
            dependenciaCodigo:r.dependenciaCodigo,
            dependenciaNombre:r.dependenciaNombre||'Sin dependencia',
            planeados:0,
            contratados:0,
            noContratado:0,
            novSinReemplazo:0,
            ausentismoTotal:0,
            totalPagar:0
          });
        }
        const t=depMap.get(r.dependenciaKey);
        t.planeados+=r.planeados;
        t.contratados+=r.contratados;
        t.noContratado+=r.noContratado;
        t.novSinReemplazo+=r.novSinReemplazo;
        t.ausentismoTotal+=r.ausentismoTotal;
        t.totalPagar+=r.totalPagar;
      });
      dependencyRows=Array.from(depMap.values()).sort((a,b)=> String(a.dependenciaNombre||'').localeCompare(String(b.dependenciaNombre||'')));

      renderDependency(date);
      renderTotals();
      msg.textContent=`Consulta OK. Dependencias: ${dependencyRows.length}`;
    }catch(e){
      msg.textContent='Error: '+(e?.message||e);
    }
  }

  function renderDependency(date){
    const rows=sortRows(dependencyRows,depSortKey,depSortDir);
    const tb=qs('#tblDependency tbody',ui);
    tb.replaceChildren(...rows.map((r)=>{
      const tr=el('tr',{},[]);
      const btn=el('button',{className:'btn',type:'button'},['Ver']);
      btn.addEventListener('click',()=> renderDetail(r.dependenciaKey,r.dependenciaNombre,date));
      tr.append(
        el('td',{},[r.dependenciaNombre||'-']),
        el('td',{},[String(r.planeados)]),
        el('td',{},[String(r.contratados)]),
        el('td',{},[String(r.noContratado)]),
        el('td',{},[String(r.novSinReemplazo)]),
        el('td',{},[String(r.ausentismoTotal)]),
        el('td',{},[String(r.totalPagar)]),
        el('td',{},[btn])
      );
      return tr;
    }));
    const totals=dependencyRows.reduce((acc,r)=>({
      planeados:acc.planeados+Number(r.planeados||0),
      contratados:acc.contratados+Number(r.contratados||0),
      noContratado:acc.noContratado+Number(r.noContratado||0),
      novSinReemplazo:acc.novSinReemplazo+Number(r.novSinReemplazo||0),
      ausentismoTotal:acc.ausentismoTotal+Number(r.ausentismoTotal||0),
      totalPagar:acc.totalPagar+Number(r.totalPagar||0)
    }),{ planeados:0, contratados:0, noContratado:0, novSinReemplazo:0, ausentismoTotal:0, totalPagar:0 });
    qs('#totDependency',ui).textContent=`Total dependencias - Planeados: ${totals.planeados}, Contratados: ${totals.contratados}, No contratado: ${totals.noContratado}, Novedad sin reemplazo: ${totals.novSinReemplazo}, Total ausentismo: ${totals.ausentismoTotal}, Total a pagar: ${totals.totalPagar}`;
    updateSortIndicators('#tblDependency th[data-sort-dep]','data-sort-dep',depSortKey,depSortDir);
  }

  function renderTotals(){
    const bySede=new Map();
    sedeDailyRows.forEach((r)=>{
      if(!bySede.has(r.sedeCodigo)) bySede.set(r.sedeCodigo,{
        sedeCodigo:r.sedeCodigo,
        sedeNombre:r.sedeNombre||'-',
        planeados:0,
        contratados:0,
        noContratado:0,
        novSinReemplazo:0,
        ausentismoTotal:0,
        totalPagar:0
      });
      const t=bySede.get(r.sedeCodigo);
      t.planeados+=r.planeados;
      t.contratados+=r.contratados;
      t.noContratado+=r.noContratado;
      t.novSinReemplazo+=r.novSinReemplazo;
      t.ausentismoTotal+=r.ausentismoTotal;
      t.totalPagar+=r.totalPagar;
    });
    const rows=Array.from(bySede.values()).sort((a,b)=> String(a.sedeNombre||'').localeCompare(String(b.sedeNombre||'')));
    totalsRows=rows;
    const sortedRows=sortRows(rows,sedeSortKey,sedeSortDir);
    const tb=qs('#tblTotals tbody',ui);
    tb.replaceChildren(...sortedRows.map((r)=>{
      const tr=el('tr',{},[]);
      const btn=el('button',{className:'btn',type:'button'},['Ver']);
      btn.addEventListener('click',()=> renderSedeDetail(r.sedeCodigo,r.sedeNombre));
      tr.append(
        el('td',{},[r.sedeNombre||'-']),
        el('td',{},[String(r.planeados)]),
        el('td',{},[String(r.contratados)]),
        el('td',{},[String(r.noContratado)]),
        el('td',{},[String(r.novSinReemplazo)]),
        el('td',{},[String(r.ausentismoTotal)]),
        el('td',{},[String(r.totalPagar)]),
        el('td',{},[btn])
      );
      return tr;
    }));
    const totalRange=rows.reduce((acc,r)=> acc+r.totalPagar,0);
    qs('#totRange',ui).textContent=`Total rango a pagar: ${totalRange}`;
    updateSortIndicators('#tblTotals th[data-sort-sede]','data-sort-sede',sedeSortKey,sedeSortDir);
  }

  function renderDetail(dependenciaKey,dependenciaNombre,date){
    qs('#detailTitle',ui).textContent=`Detalle dependencia: ${dependenciaNombre||'-'} (${date})`;
    const detailRows=[];
    const depDaily=sedeDailyRows
      .filter((r)=> r.dependenciaKey===dependenciaKey)
      .sort((a,b)=> (String(a.fecha)+String(a.sedeNombre)).localeCompare(String(b.fecha)+String(b.sedeNombre)));

    depDaily.forEach((d)=>{
      const key=`${d.fecha}|${d.sedeCodigo}`;
      const atts=attendanceByKey.get(key)||[];
      atts.forEach((a)=>{
        const rep=replByEmpDate.get(`${a.fecha||''}|${a.empleadoId||''}`);
        let estado='Trabajo';
        if(rep){
          estado=rep.decision==='reemplazo'
            ? `Reemplazado por ${rep.supernumerarioNombre||rep.supernumerarioDocumento||'-'}`
            : 'Ausentismo';
        }else if(a.asistio===false){
          estado=attendanceRequiresReplacementForSummary(a,novedadRules)
            ? 'Ausentismo'
            : `Novedad: ${a.novedadNombre||a.novedad||'-'}`;
        }
        detailRows.push({
          fecha:d.fecha,
          sede:d.sedeNombre,
          documento:a.documento||'-',
          nombre:a.nombre||'-',
          estado
        });
      });
      for(let i=0;i<Number(d.noContratado||0);i++){
        detailRows.push({
          fecha:d.fecha,
          sede:d.sedeNombre,
          documento:'-',
          nombre:`No contratado ${i+1}`,
          estado:'No contratado'
        });
      }
    });

    detailRowsCache=detailRows;
    renderDetailRows();
  }

  function renderSedeDetail(sedeCodigo,sedeNombre){
    const date=qs('#opDate',ui).value;
    qs('#detailTitle',ui).textContent=`Detalle sede: ${sedeNombre||'-'} (${date})`;
    const detailRows=[];
    const sedeRows=sedeDailyRows
      .filter((r)=> r.sedeCodigo===sedeCodigo)
      .sort((a,b)=> String(a.fecha).localeCompare(String(b.fecha)));

    sedeRows.forEach((d)=>{
      const key=`${d.fecha}|${d.sedeCodigo}`;
      const atts=attendanceByKey.get(key)||[];
      atts.forEach((a)=>{
        const rep=replByEmpDate.get(`${a.fecha||''}|${a.empleadoId||''}`);
        let estado='Trabajo';
        if(rep){
          estado=rep.decision==='reemplazo'
            ? `Reemplazado por ${rep.supernumerarioNombre||rep.supernumerarioDocumento||'-'}`
            : 'Ausentismo';
        }else if(a.asistio===false){
          estado=attendanceRequiresReplacementForSummary(a,novedadRules)
            ? 'Ausentismo'
            : `Novedad: ${a.novedadNombre||a.novedad||'-'}`;
        }
        detailRows.push({
          fecha:d.fecha,
          sede:d.sedeNombre,
          documento:a.documento||'-',
          nombre:a.nombre||'-',
          estado
        });
      });
      for(let i=0;i<Number(d.noContratado||0);i++){
        detailRows.push({
          fecha:d.fecha,
          sede:d.sedeNombre,
          documento:'-',
          nombre:`No contratado ${i+1}`,
          estado:'No contratado'
        });
      }
    });

    detailRowsCache=detailRows;
    renderDetailRows();
  }

  function renderDetailRows(){
    const rows=sortRows(detailRowsCache,detailSortKey,detailSortDir);
    const tb=qs('#tblDetail tbody',ui);
    tb.replaceChildren(...rows.map((r)=> el('tr',{},[
      el('td',{},[r.fecha||'-']),
      el('td',{},[r.sede||'-']),
      el('td',{},[r.documento||'-']),
      el('td',{},[r.nombre||'-']),
      el('td',{},[r.estado||'-'])
    ])));
    updateSortIndicators('#tblDetail th[data-sort-detail]','data-sort-detail',detailSortKey,detailSortDir);
  }

  async function exportSummaryExcel(){
    if(!dependencyRows.length && !totalsRows.length){ msg.textContent='No hay datos para exportar.'; return; }
    try{
      const mod=await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb=mod.utils.book_new();
      const depData=dependencyRows.map((r)=>(
        {
          Dependencia:r.dependenciaNombre,
          Planeados:r.planeados,
          Contratados:r.contratados,
          NoContratado:r.noContratado,
          NovedadSinReemplazo:r.novSinReemplazo,
          TotalAusentismo:r.ausentismoTotal,
          TotalPagar:r.totalPagar
        }
      ));
      const totalsData=totalsRows.map((r)=>(
        {
          Sede:r.sedeNombre,
          Planeados:r.planeados,
          Contratados:r.contratados,
          NoContratado:r.noContratado,
          NovedadSinReemplazo:r.novSinReemplazo,
          TotalAusentismo:r.ausentismoTotal,
          TotalPagar:r.totalPagar
        }
      ));
      const wsDep=mod.utils.json_to_sheet(depData.length?depData:[{Info:'Sin datos'}]);
      const wsTotals=mod.utils.json_to_sheet(totalsData.length?totalsData:[{Info:'Sin datos'}]);
      mod.utils.book_append_sheet(wb,wsDep,'ResumenDependencia');
      mod.utils.book_append_sheet(wb,wsTotals,'ResumenSede');
      const date=qs('#opDate',ui).value||'fecha';
      mod.writeFile(wb,`ausentismo_resumen_${date}.xlsx`);
      msg.textContent='Resumen exportado a Excel.';
    }catch(e){
      msg.textContent='Error exportando resumen: '+(e?.message||e);
    }
  }

  async function exportDetailExcel(){
    if(!detailRowsCache.length){ msg.textContent='Primero abre un detalle de dependencia para exportar.'; return; }
    try{
      const mod=await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb=mod.utils.book_new();
      const data=detailRowsCache.map((r)=>(
        {
          Fecha:r.fecha,
          Sede:r.sede,
          Documento:r.documento,
          Nombre:r.nombre,
          Estado:r.estado
        }
      ));
      const ws=mod.utils.json_to_sheet(data);
      mod.utils.book_append_sheet(wb,ws,'DetalleDependencia');
      const date=qs('#opDate',ui).value||'fecha';
      mod.writeFile(wb,`ausentismo_detalle_${date}.xlsx`);
      msg.textContent='Detalle exportado a Excel.';
    }catch(e){
      msg.textContent='Error exportando detalle: '+(e?.message||e);
    }
  }

  async function exportSedeExcel(){
    if(!totalsRows.length){ msg.textContent='No hay resumen por sede para exportar.'; return; }
    try{
      const mod=await import('https://cdn.jsdelivr.net/npm/xlsx@0.18.5/+esm');
      const wb=mod.utils.book_new();
      const data=totalsRows.map((r)=>(
        {
          Sede:r.sedeNombre,
          Planeados:r.planeados,
          Contratados:r.contratados,
          NoContratado:r.noContratado,
          NovedadSinReemplazo:r.novSinReemplazo,
          TotalAusentismo:r.ausentismoTotal,
          TotalPagar:r.totalPagar
        }
      ));
      const ws=mod.utils.json_to_sheet(data);
      mod.utils.book_append_sheet(wb,ws,'ResumenSede');
      const date=qs('#opDate',ui).value||'fecha';
      mod.writeFile(wb,`ausentismo_resumen_sedes_${date}.xlsx`);
      msg.textContent='Resumen por sede exportado a Excel.';
    }catch(e){
      msg.textContent='Error exportando sedes: '+(e?.message||e);
    }
  }

  async function loadSedesSnapshot(){
    if(typeof deps.streamSedes!=='function') return [];
    return new Promise((resolve)=>{
      let settled=false;
      let unsub=null;
      const finish=(rows)=>{
        if(settled) return;
        settled=true;
        try{ if(typeof unsub==='function') unsub(); }catch{}
        resolve(Array.isArray(rows)? rows : []);
      };
      unsub=deps.streamSedes((rows)=> finish(rows));
      setTimeout(()=> finish([]), 5000);
    });
  }

  async function loadNovedadesSnapshot(){
    if(typeof deps.streamNovedades!=='function') return [];
    return new Promise((resolve)=>{
      let settled=false;
      let unsub=null;
      const finish=(rows)=>{
        if(settled) return;
        settled=true;
        try{ if(typeof unsub==='function') unsub(); }catch{}
        resolve(Array.isArray(rows)? rows : []);
      };
      unsub=deps.streamNovedades((rows)=> finish(rows));
      setTimeout(()=> finish([]), 5000);
    });
  }

  async function loadEmployeesSnapshot(){
    if(typeof deps.streamEmployees!=='function') return [];
    return new Promise((resolve)=>{
      let settled=false;
      let unsub=null;
      const finish=(rows)=>{
        if(settled) return;
        settled=true;
        try{ if(typeof unsub==='function') unsub(); }catch{}
        resolve(Array.isArray(rows)? rows : []);
      };
      unsub=deps.streamEmployees((rows)=> finish(rows));
      setTimeout(()=> finish([]), 5000);
    });
  }

  async function loadSupernumerariosSnapshot(){
    if(typeof deps.streamSupernumerarios!=='function') return [];
    return new Promise((resolve)=>{
      let settled=false;
      let unsub=null;
      const finish=(rows)=>{
        if(settled) return;
        settled=true;
        try{ if(typeof unsub==='function') unsub(); }catch{}
        resolve(Array.isArray(rows)? rows : []);
      };
      unsub=deps.streamSupernumerarios((rows)=> finish(rows));
      setTimeout(()=> finish([]), 5000);
    });
  }

  mount.replaceChildren(ui);
  enableSectionToggles(ui);
  return ()=>{};
};

function buildNovedadReplacementRules(rows=[]){
  const byCode=new Map();
  const byName=new Map();
  (Array.isArray(rows)?rows:[]).forEach((r)=>{
    const code=String(r.codigoNovedad||r.codigo||'').trim();
    const name=normalizeText(String(r.nombre||'').trim());
    const repl=normalizeText(String(r.reemplazo||'').trim());
    const needs=['si','yes','true','1','reemplazo'].includes(repl);
    if(code) byCode.set(code,needs);
    if(name) byName.set(name,needs);
  });
  return { byCode, byName };
}

function attendanceRequiresReplacementForSummary(att={},rules={}){
  const code=String(att.novedadCodigo||'').trim();
  if(code==='8') return true;
  if(code && rules?.byCode?.has(code)) return rules.byCode.get(code)===true;
  const name=normalizeText(baseNovedadNameForSummary(att.novedadNombre||att.novedad||''));
  if(name && rules?.byName?.has(name)) return rules.byName.get(name)===true;
  return false;
}

function baseNovedadNameForSummary(raw){
  return String(raw||'').replace(/\s*\(.*\)\s*$/, '').trim();
}

function normalizeText(v){
  return String(v||'')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g,'')
    .replace(/\s+/g,' ')
    .trim()
    .toLowerCase();
}

function parseOperatorCount(value){
  if(value==null) return 0;
  if(typeof value==='number') return Number.isFinite(value)? Math.trunc(value):0;
  const raw=String(value).trim();
  if(!raw) return 0;
  const digits=raw.replace(/[^\d]/g,'');
  if(!digits) return 0;
  const n=Number(digits);
  return Number.isFinite(n)? n:0;
}

function isEmployeeExpectedForDate(emp,selectedDate,sedeRows=[]){
  if(!selectedDate) return false;
  const ingreso=toISODate(emp?.fechaIngreso);
  if(!ingreso || ingreso>selectedDate) return false;
  const retiro=toISODate(emp?.fechaRetiro);
  const estado=String(emp?.estado||'').trim().toLowerCase();
  if(estado==='inactivo') return Boolean(retiro && retiro>=selectedDate);
  if(retiro && retiro<selectedDate) return false;
  const sedeCodigo=String(emp?.sedeCodigo||'').trim();
  if(!sedeCodigo) return false;
  const sede=(sedeRows||[]).find((row)=> String(row?.codigo||'').trim()===sedeCodigo) || null;
  if(!isSedeScheduledForDate(sede, selectedDate)) return false;
  return true;
}

function isSedeScheduledForDate(sede, selectedDate){
  const iso=toISODate(selectedDate);
  if(!iso) return false;
  const [year, month, day]=iso.split('-').map((n)=> Number(n));
  const weekday=new Date(Date.UTC(year, (month||1)-1, day||1)).getUTCDay();
  const jornada=String(sede?.jornada||'lun_vie').trim().toLowerCase();
  if(jornada==='lun_dom') return true;
  if(jornada==='lun_sab') return weekday>=1 && weekday<=6;
  return weekday>=1 && weekday<=5;
}

function toISODate(value){
  if(!value) return null;
  if(typeof value==='string'){
    const v=value.trim();
    if(/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
    const dt=new Date(v);
    if(!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0,10);
    return null;
  }
  if(typeof value?.toDate==='function'){
    const dt=value.toDate();
    if(!Number.isNaN(dt.getTime())) return dt.toISOString().slice(0,10);
    return null;
  }
  if(value instanceof Date){
    if(!Number.isNaN(value.getTime())) return value.toISOString().slice(0,10);
    return null;
  }
  return null;
}

function todayBogota(){
  const fmt=new Intl.DateTimeFormat('en-CA',{ timeZone:'America/Bogota', year:'numeric', month:'2-digit', day:'2-digit' });
  return fmt.format(new Date());
}
