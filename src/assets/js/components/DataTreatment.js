import { el } from '../utils/dom.js';

export const DataTreatment = (mount) => {
  mount.replaceChildren(
    el('section', { className: 'main-card' }, [
      el('h2', {}, ['Tratamiento Datos']),
      el('div', { className: 'policy-doc' }, [
        el('p', { className: 'policy-doc__lead' }, [
          'De conformidad con la Ley 1581 de 2012, el Decreto 1377 de 2013, el Decreto 1074 de 2015 y las demas normas aplicables en Colombia, el responsable del tratamiento de datos personales es SERVILIMPIEZA S.A.'
        ]),

        el('h3', { className: 'policy-doc__section-title' }, ['1. Responsable y Encargado']),
        el('p', { className: 'policy-doc__text' }, ['Responsable del tratamiento: SERVILIMPIEZA S.A.']),
        el('p', { className: 'policy-doc__text' }, ['Encargado del tratamiento en esta plataforma: CAPCOL S.A.S.']),
        el('p', { className: 'policy-doc__text' }, [
          'Politica oficial de tratamiento de datos del responsable: ',
          el('a', {
            href: 'https://oficial.servilimpieza.com.co/wp-content/uploads/2024/03/3.-Politica-Tratamiento-de-Datos-Personales.pdf',
            target: '_blank',
            rel: 'noopener noreferrer'
          }, ['Link tratamiento de datos'])
        ]),

        el('h3', { className: 'policy-doc__section-title' }, ['2. Ambito de Aplicacion']),
        el('p', { className: 'policy-doc__text' }, [
          'Esta plataforma es operada por CAPCOL S.A.S. en calidad de encargado. El tratamiento de datos se realiza por cuenta de SERVILIMPIEZA S.A., conforme a las instrucciones del responsable y a su politica oficial.'
        ]),

        el('h3', { className: 'policy-doc__section-title' }, ['3. Finalidades del Tratamiento']),
        el('p', { className: 'policy-doc__text' }, ['Los datos personales seran tratados, entre otras, para las siguientes finalidades:']),
        el('p', { className: 'policy-doc__text' }, ['3.1. Gestion contractual, comercial, administrativa, contable y operativa.']),
        el('p', { className: 'policy-doc__text' }, ['3.2. Atencion de solicitudes, consultas, peticiones, quejas, reclamos y requerimientos de soporte.']),
        el('p', { className: 'policy-doc__text' }, ['3.3. Cumplimiento de obligaciones legales y regulatorias ante autoridades competentes.']),
        el('p', { className: 'policy-doc__text' }, ['3.4. Administracion de usuarios, autenticacion y control de acceso a plataformas tecnicas y sistemas de informacion.']),
        el('p', { className: 'policy-doc__text' }, ['3.5. Gestion de comunicaciones institucionales relacionadas con la prestacion del servicio.']),

        el('h3', { className: 'policy-doc__section-title' }, ['4. Derechos de los Titulares']),
        el('p', { className: 'policy-doc__text' }, ['De acuerdo con la normatividad vigente, el titular de los datos personales tiene derecho a:']),
        el('p', { className: 'policy-doc__text' }, ['4.1. Conocer, actualizar y rectificar sus datos personales frente al responsable del tratamiento.']),
        el('p', { className: 'policy-doc__text' }, ['4.2. Solicitar prueba de la autorizacion otorgada, salvo cuando expresamente se exceptue como requisito para el tratamiento.']),
        el('p', { className: 'policy-doc__text' }, ['4.3. Ser informado, previa solicitud, respecto del uso que se ha dado a sus datos personales.']),
        el('p', { className: 'policy-doc__text' }, ['4.4. Presentar consultas y reclamos conforme a los procedimientos legalmente establecidos.']),
        el('p', { className: 'policy-doc__text' }, ['4.5. Solicitar la supresion de los datos o la revocatoria de la autorizacion cuando sea procedente.']),
        el('p', { className: 'policy-doc__text' }, ['4.6. Acceder en forma gratuita a sus datos personales objeto de tratamiento.']),

        el('h3', { className: 'policy-doc__section-title' }, ['5. Procedimiento para Consultas y Reclamos']),
        el('p', { className: 'policy-doc__text' }, ['Las consultas y reclamos sobre proteccion de datos deben dirigirse principalmente al responsable (SERVILIMPIEZA S.A.) mediante los canales definidos en su politica oficial.']),
        el('p', { className: 'policy-doc__text' }, ['CAPCOL S.A.S., como encargado, apoyara la gestion de solicitudes en los casos que correspondan operativamente y bajo instruccion del responsable.']),

        el('h3', { className: 'policy-doc__section-title' }, ['6. Seguridad de la Informacion']),
        el('p', { className: 'policy-doc__text' }, ['CAPCOL S.A.S. adopta medidas tecnicas, humanas y administrativas razonables para proteger los datos personales frente a acceso no autorizado, perdida, adulteracion, uso indebido o fraude.']),

        el('h3', { className: 'policy-doc__section-title' }, ['7. Vigencia']),
        el('p', { className: 'policy-doc__text' }, ['La vigencia, actualizacion y control principal de la politica de tratamiento corresponde al responsable (SERVILIMPIEZA S.A.), de acuerdo con su documento oficial.']),
        el('p', { className: 'policy-doc__text' }, ['CAPCOL S.A.S. mantendra esta referencia actualizada en la plataforma cuando reciba nuevas directrices del responsable.']),
        el('p', { className: 'policy-doc__text text-muted' }, ['Autoridad de control: Superintendencia de Industria y Comercio (SIC).'])
      ])
    ])
  );
};
