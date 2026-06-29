import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import PDFDocument from 'pdfkit';
import QRCode from 'qrcode';
import { certificateTemplateConfig } from './config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CERTIFICATE_TYPES = new Set(['basic', 'with_salary']);

export function normalizeCertificateType(value) {
  const type = String(value || 'basic').trim().toLowerCase();
  return CERTIFICATE_TYPES.has(type) ? type : 'basic';
}

export function certificateFileName(employee = {}, type = 'basic') {
  const doc = safeFilePart(employee?.documento || 'empleado');
  const suffix = type === 'with_salary' ? 'con-salario' : 'basico';
  return `certificado-laboral-${suffix}-${doc}.pdf`;
}

export async function buildEmployeeCertificatePdf({ employee, cargo, type, verificationCode = '', verificationUrl = '' }) {
  const normalizedType = normalizeCertificateType(type);
  validateCertificateData({ employee, cargo, type: normalizedType });
  return buildCertificatePdfWithPdfKit({ employee, cargo, type: normalizedType, verificationCode, verificationUrl });
}

function validateCertificateData({ employee, cargo, type }) {
  if (!employee?.id) throw new Error('employee_not_found');
  if (String(employee?.estado || '').trim().toLowerCase() !== 'activo') throw new Error('employee_inactive');
  const salary = cargo?.salario == null ? null : Number(cargo.salario);
  if (type === 'with_salary' && (!Number.isFinite(salary) || salary < 0)) {
    throw new Error('missing_salary');
  }
}

async function buildCertificatePdfWithPdfKit({ employee, cargo, type, verificationCode = '', verificationUrl = '' }) {
  const cfg = certificateTemplateConfig;
  const headerImage = await assetBuffer(cfg.header?.imagePath || cfg.header?.logoPath);
  const footerImage = await assetBuffer(cfg.footer?.imagePath);
  const signatureImage = await assetBuffer(cfg.signature?.imagePath);
  const verificationQrImage = verificationUrl ? await QRCode.toBuffer(verificationUrl, { type: 'png', width: 240, margin: 1 }) : null;
  const salary = cargo?.salario == null ? null : Number(cargo.salario);
  const layout = resolveLayout(cfg);

  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      size: 'LETTER',
      margins: layout.margins,
      info: {
        Title: 'Certificado laboral',
        Author: cfg.companyLegalName || 'Rocky'
      }
    });
    const chunks = [];
    doc.on('data', (chunk) => chunks.push(chunk));
    doc.on('error', reject);
    doc.on('end', () => resolve(Buffer.concat(chunks)));

    const pageWidth = doc.page.width;
    const pageHeight = doc.page.height;
    const contentWidth = pageWidth - doc.page.margins.left - doc.page.margins.right;
    const left = doc.page.margins.left;
    const headerBox = layout.header.fullWidth
      ? { x: 0, width: pageWidth }
      : { x: left, width: contentWidth };
    const footerBox = layout.footer.fullWidth
      ? { x: 0, width: pageWidth }
      : { x: left, width: contentWidth };

    drawImageFit(doc, headerImage, headerBox.x, layout.header.top, headerBox.width, layout.header.height, cfg.header?.align || 'center', 'stretch');
    drawImageFit(doc, footerImage, footerBox.x, pageHeight - layout.footer.bottomOffset, footerBox.width, layout.footer.height, 'center', 'stretch');

    doc.y = doc.page.margins.top + 20;
    doc.font('Helvetica').fontSize(11).text(`${cfg.city || ''}, ${formatLongDate(new Date(), cfg)}`, {
      width: contentWidth,
      align: 'right'
    });

    doc.moveDown(2.2);
    doc.font('Helvetica-Bold').fontSize(14).text('CERTIFICACION LABORAL', {
      width: contentWidth,
      align: 'center'
    });

    doc.moveDown(2);
    doc.font('Helvetica').fontSize(12);
    doc.text(
      `${cfg.companyLegalName || ''}, identificada con NIT ${cfg.companyNit || '-'}, CERTIFICA que ${employee.nombre || 'Empleado'}, identificado(a) con documento de identidad No. ${employee.documento || '-'}, se encuentra vinculado(a) laboralmente con nuestra compañía desde el ${formatLongDate(employee.fecha_ingreso, cfg)}, desempeñando el cargo de ${employee.cargo_nombre || cargo?.nombre || employee.cargo_codigo || '-'}.`,
      { width: contentWidth, align: 'justify' }
    );

    if (type === 'with_salary') {
      doc.moveDown(1);
      doc.text(`Actualmente devenga un salario básico de ${formatCurrency(salary, cfg)}.`, {
        width: contentWidth,
        align: 'justify'
      });
    }

    doc.moveDown(1);
    doc.text('La presente certificación se expide a solicitud del interesado(a), para los fines que estime convenientes.', {
      width: contentWidth,
      align: 'justify'
    });

    const signatureTop = Math.min(doc.y + 50, pageHeight - doc.page.margins.bottom - 105);
    drawImageFit(doc, signatureImage, left, signatureTop, layout.signature.width, layout.signature.height, 'left');
    doc.y = signatureTop + layout.signature.height + 8;
    doc.font('Helvetica-Bold').fontSize(11).text(cfg.signature?.signerName || '', { width: contentWidth });
    doc.font('Helvetica').fontSize(11).text(cfg.signature?.signerTitle || '', { width: contentWidth });

    if (verificationQrImage?.length) {
      const qrSize = 72;
      const qrX = left + contentWidth - qrSize;
      const qrY = signatureTop + 6;
      doc.image(verificationQrImage, qrX, qrY, { fit: [qrSize, qrSize] });
      doc.font('Helvetica').fontSize(7).fillColor('#333333');
      doc.text('Verificacion', qrX - 12, qrY + qrSize + 3, { width: qrSize + 24, align: 'center' });
      doc.text(verificationCode || '', qrX - 12, qrY + qrSize + 13, { width: qrSize + 24, align: 'center' });
      doc.fillColor('#1f2933');
    }

    const footerLines = Array.isArray(cfg.footer?.lines) ? cfg.footer.lines.filter(Boolean) : [];
    if (footerLines.length) {
      doc.font('Helvetica').fontSize(8).fillColor('#333333');
      doc.text([
        cfg.companyLegalName || '',
        `[ NIT: ${cfg.companyNit || ''} ] ${cfg.companyRegimeText || ''}`,
        ...footerLines
      ].filter(Boolean).join('\n'), left, pageHeight - 96, {
        width: contentWidth,
        align: 'center',
        lineGap: 1
      });
      doc.fillColor('#1f2933');
    }

    doc.end();
  });
}

function resolveLayout(cfg) {
  return {
    margins: {
      top: numberOr(cfg.layout?.margins?.top, 132),
      right: numberOr(cfg.layout?.margins?.right, 74),
      bottom: numberOr(cfg.layout?.margins?.bottom, 130),
      left: numberOr(cfg.layout?.margins?.left, 74)
    },
    header: {
      top: numberOr(cfg.layout?.header?.top, 18),
      height: numberOr(cfg.layout?.header?.height, 104),
      fullWidth: cfg.layout?.header?.fullWidth !== false
    },
    footer: {
      bottomOffset: numberOr(cfg.layout?.footer?.bottomOffset, 112),
      height: numberOr(cfg.layout?.footer?.height, 90),
      fullWidth: cfg.layout?.footer?.fullWidth !== false
    },
    signature: {
      width: numberOr(cfg.layout?.signature?.width, 180),
      height: numberOr(cfg.layout?.signature?.height, 90)
    }
  };
}

function numberOr(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function drawImageFit(doc, image, x, y, width, height, align = 'center', mode = 'contain') {
  if (!image?.length) return;
  try {
    if (mode === 'stretch') {
      doc.image(image, x, y, { width, height });
      return;
    }
    doc.image(image, x, y, {
      fit: [width, height],
      align: align === 'right' ? 'right' : align === 'left' ? 'left' : 'center',
      valign: 'center'
    });
  } catch (error) {
    console.error('Certificate image draw failed:', error?.message || error);
  }
}

async function assetBuffer(assetPath) {
  const raw = String(assetPath || '').trim();
  if (!raw) return null;
  try {
    const resolved = path.isAbsolute(raw) ? raw : path.resolve(__dirname, raw);
    return await fs.readFile(resolved);
  } catch {
    return null;
  }
}

function formatCurrency(value, cfg) {
  return new Intl.NumberFormat(cfg.locale || 'es-CO', {
    style: 'currency',
    currency: 'COP',
    maximumFractionDigits: 0
  }).format(Number(value || 0));
}

function formatLongDate(value, cfg) {
  const date = value instanceof Date ? value : new Date(value || Date.now());
  if (Number.isNaN(date.getTime())) return '-';
  return new Intl.DateTimeFormat(cfg.locale || 'es-CO', {
    timeZone: cfg.timezone || 'America/Bogota',
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  }).format(date);
}

function safeFilePart(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .trim() || 'archivo';
}
