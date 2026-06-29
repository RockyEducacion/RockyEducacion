export const certificateTemplateConfig = {
  timezone: 'America/Bogota',
  locale: 'es-CO',
  city: 'Medellin',
  companyLegalName: 'No disponible para RockyEDU, por favor comunícate con la Empresa',
  companyNit: 'No disponible',
  companyRegimeText: 'No Disponible',
  layout: {
    margins: {
      top: 132,
      right: 74,
      bottom: 130,
      left: 74
    },
    header: {
      top: 18,
      height: 104,
      fullWidth: true
    },
    footer: {
      bottomOffset: 112,
      height: 90,
      fullWidth: true
    },
    signature: {
      width: 180,
      height: 90
    }
  },
  header: {
    imagePath: './assets/certificate-header-blank.png',
    maxWidth: '100%',
    maxHeight: '100%',
    align: 'left'
  },
  footer: {
    imagePath: './assets/certificate-footer-blank.png',
    maxWidth: '100%',
    maxHeight: '100%',
    lines: []
  },
  signature: {
    imagePath: './assets/certificate-signature-blank.png',
    maxWidth: '180px',
    maxHeight: '90px',
    signerName: 'NO DISPONIBLE EN ROCKY EDU',
    signerTitle: 'No disponible'
  }
};
