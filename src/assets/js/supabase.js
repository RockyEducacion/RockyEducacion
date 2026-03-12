import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm';
import { SUPABASE_ANON_KEY, SUPABASE_PROFILES_TABLE, SUPABASE_URL } from './config.js';

function assertSupabaseConfig() {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error('Configura SUPABASE_URL y SUPABASE_ANON_KEY en src/assets/js/config.js.');
  }
}

assertSupabaseConfig();

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true
  }
});

function normalizeUser(user) {
  if (!user) return null;
  return {
    uid: user.id,
    email: user.email || '',
    displayName: user.user_metadata?.display_name || user.user_metadata?.full_name || null
  };
}

function normalizeProfileRow(uid, data = {}) {
  return {
    id: uid,
    email: String(data.email || '').trim().toLowerCase() || null,
    display_name: data.nombre || data.displayName || null,
    documento: data.documento || null,
    estado: data.estado || 'activo',
    updated_at: new Date().toISOString()
  };
}

async function upsertProfile(uid, data = {}) {
  const payload = normalizeProfileRow(uid, data);
  const { error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .upsert(payload, { onConflict: 'id' });
  if (error) throw error;
}

export const authState = (cb) => {
  supabase.auth.getSession().then(({ data, error }) => {
    if (error) {
      console.error('No se pudo consultar la sesion de Supabase:', error);
      cb(null);
      return;
    }
    cb(normalizeUser(data.session?.user || null));
  });

  const { data } = supabase.auth.onAuthStateChange((_event, session) => {
    cb(normalizeUser(session?.user || null));
  });

  return () => data.subscription.unsubscribe();
};

export async function login(email, pass) {
  const { data, error } = await supabase.auth.signInWithPassword({
    email: String(email || '').trim(),
    password: pass
  });
  if (error) throw error;
  return { user: normalizeUser(data.user) };
}

export async function register(email, pass) {
  const cleanEmail = String(email || '').trim().toLowerCase();
  const { data, error } = await supabase.auth.signUp({
    email: cleanEmail,
    password: pass
  });
  if (error) throw error;
  return { user: normalizeUser(data.user) };
}

export async function logout() {
  const { error } = await supabase.auth.signOut();
  if (error) throw error;
}

export async function createUserProfile(uid, data) {
  await upsertProfile(uid, data);
}

export async function ensureUserProfile(user) {
  if (!user?.uid) return;
  const existing = await loadUserProfile(user.uid);
  if (existing) return;
  await upsertProfile(user.uid, {
    email: user.email,
    displayName: user.displayName,
    estado: 'activo'
  });
}

export async function loadUserProfile(uid) {
  const { data, error } = await supabase
    .from(SUPABASE_PROFILES_TABLE)
    .select('*')
    .eq('id', uid)
    .maybeSingle();
  if (error) throw error;
  if (!data) return null;
  return {
    uid: data.id,
    email: data.email || '',
    displayName: data.display_name || null,
    documento: data.documento || null,
    estado: data.estado || 'activo',
    role: data.role || null,
    zonaCodigo: data.zona_codigo || null,
    zonasPermitidas: Array.isArray(data.zonas_permitidas) ? data.zonas_permitidas : [],
    supervisorEligible: data.supervisor_eligible === true
  };
}

export async function getUserOverrides() {
  const user = (await supabase.auth.getUser()).data.user;
  if (!user?.id) return {};
  const { data, error } = await supabase
    .from('user_overrides')
    .select('permissions')
    .eq('user_id', user.id)
    .maybeSingle();
  if (error) throw error;
  return data?.permissions || {};
}

export async function setUserOverrides() {
  throw new Error('setUserOverrides aun no esta migrado a Supabase.');
}

export async function clearUserOverrides() {
  throw new Error('clearUserOverrides aun no esta migrado a Supabase.');
}

export function streamRoleMatrix(onData) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('roles_matrix')
      .select('role, permissions');
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar roles_matrix:', error);
      onData({});
      return;
    }
    const map = {};
    (data || []).forEach((row) => {
      map[row.role] = row.permissions || {};
    });
    onData(map);
  };

  emit();

  const channel = supabase
    .channel('roles-matrix-watch')
    .on('postgres_changes', { event: '*', schema: 'public', table: 'roles_matrix' }, emit)
    .subscribe();

  return () => {
    active = false;
    supabase.removeChannel(channel);
  };
}

export function streamUserOverrides(uid, onData) {
  let active = true;
  const emit = async () => {
    const { data, error } = await supabase
      .from('user_overrides')
      .select('permissions')
      .eq('user_id', uid)
      .maybeSingle();
    if (!active) return;
    if (error) {
      console.error('No se pudo cargar user_overrides:', error);
      onData({});
      return;
    }
    onData(data?.permissions || {});
  };

  emit();

  const channel = supabase
    .channel(`user-overrides-${uid}`)
    .on('postgres_changes', { event: '*', schema: 'public', table: 'user_overrides', filter: `user_id=eq.${uid}` }, emit)
    .subscribe();

  return () => {
    active = false;
    supabase.removeChannel(channel);
  };
}

export { supabase };
