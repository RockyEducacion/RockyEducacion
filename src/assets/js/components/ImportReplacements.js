import { el, qs } from '../utils/dom.js';
import { getState, setState } from '../state.js';

export const ImportReplacements=(mount,deps={})=>{
  const flow=getState().pendingReplacementFlow||{};
  const candidates=Array.isArray(flow.candidates)? flow.candidates:[];
  const fecha=flow.fechaOperacion||'';
  const importId=flow.importId||null;

  const ui=el('section',{className:'main-card'},[
    el('h2',{},['Reemplazos de importacion']),
    el('p',{className:'text-muted'},[`Fecha: ${fecha||'-'}`]),
    el('div',{className:'mt-2 table-wrap'},[
      el('table',{className:'table',id:'tblRep'},[
        el('thead',{},[el('tr',{},[
          el('th',{},['Empleado']),
          el('th',{},['Documento']),
          el('th',{},['Sede']),
          el('th',{},['Novedad']),
          el('th',{},['Decision']),
          el('th',{},['Supernumerario'])
        ])]),
        el('tbody',{})
      ])
    ]),
    el('p',{id:'msg',className:'text-muted mt-2'},[candidates.length? 'Selecciona reemplazo o ausentismo por cada fila.':'No hay novedades por reemplazar en esta importacion.']),
    el('div',{className:'form-row mt-2'},[
      el('button',{id:'btnSave',className:'btn btn--primary',type:'button',disabled:!candidates.length},['Guardar decisiones'])
    ])
  ]);

  const tbody=qs('#tblRep tbody',ui);
  let supernumerarios=[];
  let occupiedReplacements=[];
  const unSupn=deps.streamSupernumerarios?.((arr)=>{ supernumerarios=(arr||[]).filter((s)=> s.estado!=='inactivo'); render(); });
  loadOccupancy();

  async function loadOccupancy(){
    if(!fecha || !deps.listSupernumerarioReplacementOccupancy) return;
    try{
      occupiedReplacements=await deps.listSupernumerarioReplacementOccupancy(fecha)||[];
      render();
    }catch(e){
      qs('#msg',ui).textContent='No se pudo cargar ocupacion de supernumerarios: '+(e?.message||e);
    }
  }

  function occupiedSupernumerarioKeys(){
    const ids=new Set();
    const docs=new Set();
    (occupiedReplacements||[]).forEach((row)=>{
      if(String(row?.decision||'').trim()!=='reemplazo') return;
      const id=String(row?.supernumerarioId||'').trim();
      const doc=String(row?.supernumerarioDocumento||'').trim();
      if(id) ids.add(id);
      if(doc) docs.add(doc);
    });
    return { ids, docs };
  }

  const byDoc=(doc)=>{
    const occupied=occupiedSupernumerarioKeys();
    return supernumerarios.filter((s)=>{
      const id=String(s.id||'').trim();
      const sdoc=String(s.documento||'').trim();
      if(sdoc===String(doc||'').trim()) return false;
      return !occupied.ids.has(id) && (!sdoc || !occupied.docs.has(sdoc));
    });
  };

  function decisionRow(c,idx){
    const tr=el('tr',{'data-idx':String(idx)},[]);
    const decision=el('select',{className:'select'},[
      el('option',{value:''},['Seleccione...']),
      el('option',{value:'reemplazo'},['Reemplazo']),
      el('option',{value:'ausentismo'},['Ausentismo'])
    ]);
    const supSel=el('select',{className:'select',disabled:true},[
      el('option',{value:''},['Seleccione supernumerario...'])
    ]);
    decision.addEventListener('change',()=>{
      supSel.disabled=decision.value!=='reemplazo';
      if(supSel.disabled) supSel.value='';
    });
    const available=byDoc(c.documento);
    const options=available.map((s)=> el('option',{value:s.id},[`${s.nombre||s.documento||'-'} (${s.documento||'-'})`]));
    supSel.append(...options);
    if(!available.length){
      supSel.append(el('option',{value:'',disabled:true},['Sin supernumerarios libres']));
    }
    tr.append(
      el('td',{},[c.nombre||'-']),
      el('td',{},[c.documento||'-']),
      el('td',{},[c.sedeNombre||c.sedeCodigo||'-']),
      el('td',{},[`${c.novedadNombre||'-'} (${c.novedadCodigo||'-'})`]),
      el('td',{},[decision]),
      el('td',{},[supSel])
    );
    return tr;
  }

  function render(){
    tbody.replaceChildren(...candidates.map((c,idx)=> decisionRow(c,idx)));
  }
  render();

  qs('#btnSave',ui).addEventListener('click',async()=>{
    const rows=Array.from(tbody.querySelectorAll('tr'));
    const assignments=[];
    const usedSupn=new Set();
    for(let i=0;i<rows.length;i++){
      const row=rows[i];
      const c=candidates[i];
      const decision=row.querySelectorAll('select')[0].value;
      const supnId=row.querySelectorAll('select')[1].value;
      if(!decision){ qs('#msg',ui).textContent='Debes seleccionar decision en todas las filas.'; return; }
      if(decision==='reemplazo'){
        if(!supnId){ qs('#msg',ui).textContent='Debes seleccionar supernumerario en todas las filas con reemplazo.'; return; }
        if(usedSupn.has(supnId)){ qs('#msg',ui).textContent='No puedes usar el mismo supernumerario dos veces.'; return; }
        const s=supernumerarios.find((x)=> x.id===supnId);
        const occupied=occupiedSupernumerarioKeys();
        const supnDoc=String(s?.documento||'').trim();
        if(occupied.ids.has(supnId) || (supnDoc && occupied.docs.has(supnDoc))){
          qs('#msg',ui).textContent='Ese supernumerario ya esta ocupado para esta fecha.';
          return;
        }
        usedSupn.add(supnId);
        assignments.push({
          ...c,
          decision,
          supernumerarioId: s?.id||null,
          supernumerarioDocumento: s?.documento||null,
          supernumerarioNombre: s?.nombre||null
        });
      }else{
        assignments.push({
          ...c,
          decision,
          supernumerarioId:null,
          supernumerarioDocumento:null,
          supernumerarioNombre:null
        });
      }
    }
    try{
      qs('#msg',ui).textContent='Guardando decisiones...';
      await deps.saveImportReplacements?.({ importId, fechaOperacion:fecha, assignments });
      setState({ pendingReplacementFlow:null });
      qs('#msg',ui).textContent='Decisiones guardadas OK.';
    }catch(e){
      qs('#msg',ui).textContent='Error: '+(e?.message||e);
    }
  });

  mount.replaceChildren(ui);
  return ()=>{ unSupn?.(); };
};
