import 'dotenv/config';

function required(name) {
  const value = String(process.env[name] || '').trim();
  if (!value) throw new Error(`Falta la variable de entorno ${name}.`);
  return value;
}

export const config = {
  port: Number(process.env.PORT || 8787),
  supabaseUrl: required('SUPABASE_URL'),
  supabaseServiceRoleKey: required('SUPABASE_SERVICE_ROLE_KEY'),
  cronSecret: String(process.env.CRON_SECRET || '').trim(),
  whatsappVerifyToken: required('WHATSAPP_VERIFY_TOKEN'),
  whatsappAccessToken: String(process.env.WHATSAPP_ACCESS_TOKEN || '').trim(),
  whatsappPhoneNumberId: String(process.env.WHATSAPP_PHONE_NUMBER_ID || '').trim(),
  whatsappGraphVersion: String(process.env.WHATSAPP_GRAPH_VERSION || 'v25.0').trim(),
  whatsappAppSecret: String(process.env.WHATSAPP_APP_SECRET || '').trim()
};
