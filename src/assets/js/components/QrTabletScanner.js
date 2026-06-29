import { el, qs } from '../utils/dom.js';

const DEVICE_TOKEN_KEY = 'rocky_qr_device_token';
const SCANNER_STATE_KEY = 'rocky_qr_scanner_state';
const IDLE_PAUSE_MS = 3 * 60 * 1000;
const AFTER_SCAN_PAUSE_MS = 3 * 60 * 1000;
const SUPPORT_UNLOCK_MS = 90 * 1000;
const SCANNER_READY_STATE = 'ready_to_resume';

export const QrTabletScanner = (mount, deps = {}) => {
  let stream = null;
  let detector = null;
  let scanning = false;
  let lastValue = '';
  let lastScanAt = 0;
  let pauseTimer = null;
  let supportUnlockTimer = null;
  let supportUnlocked = false;
  let wakeLock = null;

  const savedToken = getDeviceToken();
  const ui = el('section', { className: 'main-card' }, [
    el('div', { className: 'wa-header__top' }, [
      el('h2', {}, ['Lector QR']),
      el('span', { id: 'deviceStatus', className: `badge ${savedToken ? 'badge--ok' : 'badge--off'}` }, [savedToken ? 'Tablet activa' : 'Sin activar'])
    ]),
    el('section', { className: 'qr-support-panel mt-2' }, [
      el('button', { id: 'btnUnlockSupport', className: 'btn qr-support-panel__unlock', type: 'button' }, ['Soporte']),
      el('p', { id: 'supportHint', className: 'text-muted qr-support-panel__hint' }, ['Toca para mostrar las opciones de soporte.']),
      el('div', { id: 'supportControls', className: 'qr-support-panel__controls hidden' }, [
        el('div', { className: 'form-row' }, [
          el('div', {}, [
            el('label', { className: 'label' }, ['Token de dispositivo']),
            el('input', { id: 'deviceToken', className: 'input', type: 'password', value: savedToken, placeholder: 'Token generado desde Sedes' })
          ]),
          el('button', { id: 'btnSaveDevice', className: 'btn btn--primary', type: 'button' }, ['Activar tablet']),
          el('button', { id: 'btnClearDevice', className: 'btn btn--danger', type: 'button' }, ['Limpiar token'])
        ])
      ]),
    ]),
    el('div', { className: 'mt-2', style: 'display:grid;gap:12px;' }, [
      el('div', { className: 'qr-scanner-frame' }, [
        el('video', { id: 'qrVideo', autoplay: true, muted: true, playsInline: true, className: 'qr-scanner-video' }),
        el('button', { id: 'qrPauseOverlay', className: 'qr-scanner-pause hidden', type: 'button' }, [
          el('strong', {}, ['Lector en pausa']),
          el('span', {}, ['Toca para activar la camara'])
        ])
      ]),
      el('div', { className: 'row-actions' }, [
        el('button', { id: 'btnStartCamera', className: 'btn btn--primary', type: 'button' }, ['Iniciar camara']),
        el('button', { id: 'btnStopCamera', className: 'btn', type: 'button' }, ['Detener'])
      ])
    ]),
    el('div', { className: 'form-row mt-2' }, [
      el('div', {}, [
        el('label', { className: 'label' }, ['Lectura manual']),
        el('input', { id: 'manualQr', className: 'input', placeholder: 'Pega aqui el contenido del QR si la camara no detecta' })
      ]),
      el('button', { id: 'btnManualScan', className: 'btn', type: 'button' }, ['Validar'])
    ]),
    el('p', { id: 'qrMessage', className: 'text-muted mt-2' }, ['Activa la tablet y luego inicia la camara.'])
  ]);

  function getDeviceToken() {
    try {
      return String(localStorage.getItem(DEVICE_TOKEN_KEY) || '').trim();
    } catch (_) {
      return '';
    }
  }

  function setMessage(text, kind = 'muted') {
    const msg = qs('#qrMessage', ui);
    if (!msg) return;
    msg.className = `mt-2 ${kind === 'error' ? 'text-danger' : kind === 'ok' ? 'text-success' : 'text-muted'}`;
    msg.textContent = text;
  }

  function setPausedOverlay(paused) {
    qs('#qrPauseOverlay', ui)?.classList.toggle('hidden', !paused);
  }

  function getScannerState() {
    try {
      return String(localStorage.getItem(SCANNER_STATE_KEY) || '').trim();
    } catch (_) {
      return '';
    }
  }

  function setScannerState(value) {
    try {
      if (value) localStorage.setItem(SCANNER_STATE_KEY, value);
      else localStorage.removeItem(SCANNER_STATE_KEY);
    } catch (_) {}
  }

  function restorePausedScannerIfNeeded() {
    if (!savedToken || getScannerState() !== SCANNER_READY_STATE) return;
    setPausedOverlay(true);
    setMessage('Lector en pausa. Toca el recuadro para activar la camara.', 'muted');
  }

  function clearPauseTimer() {
    if (!pauseTimer) return;
    clearTimeout(pauseTimer);
    pauseTimer = null;
  }

  function clearSupportTimers() {
    if (supportUnlockTimer) {
      clearTimeout(supportUnlockTimer);
      supportUnlockTimer = null;
    }
  }

  function setSupportUnlocked(unlocked, message = '') {
    supportUnlocked = unlocked === true;
    qs('#supportControls', ui)?.classList.toggle('hidden', !supportUnlocked);
    const btn = qs('#btnUnlockSupport', ui);
    if (btn) btn.textContent = supportUnlocked ? 'Ocultar soporte' : 'Soporte';
    const hint = qs('#supportHint', ui);
    if (hint) hint.textContent = message || (supportUnlocked
      ? 'Soporte puede activar o limpiar el token. Se bloqueara automaticamente.'
      : 'Toca para mostrar las opciones de soporte.');
    if (supportUnlockTimer) clearTimeout(supportUnlockTimer);
    supportUnlockTimer = null;
    if (supportUnlocked) {
      supportUnlockTimer = setTimeout(() => setSupportUnlocked(false), SUPPORT_UNLOCK_MS);
    }
  }

  function toggleSupport() {
    setSupportUnlocked(!supportUnlocked);
  }

  function schedulePause(ms, reason) {
    clearPauseTimer();
    pauseTimer = setTimeout(() => pauseScanner(reason), ms);
  }

  function markActivity() {
    if (!scanning) return;
    schedulePause(IDLE_PAUSE_MS, 'idle');
  }

  function pauseScanner(reason = 'manual') {
    clearPauseTimer();
    setScannerState(SCANNER_READY_STATE);
    stopCamera({ silent: true });
    setPausedOverlay(true);
    const message = reason === 'after_scan'
      ? 'Registro completado. El lector quedo en pausa para ahorrar energia.'
      : reason === 'screen'
        ? 'La tablet entro en reposo. Toca el recuadro para activar la camara.'
        : 'Lector en pausa por inactividad. Toca el recuadro para continuar.';
    setMessage(message, 'muted');
  }

  function syncDeviceStatus() {
    const token = getDeviceToken();
    const status = qs('#deviceStatus', ui);
    if (!status) return;
    status.className = `badge ${token ? 'badge--ok' : 'badge--off'}`;
    status.textContent = token ? 'Tablet activa' : 'Sin activar';
  }

  function saveDeviceToken() {
    if (!supportUnlocked) {
      setMessage('Toca Soporte para activar la tablet.', 'error');
      return;
    }
    const token = qs('#deviceToken', ui)?.value?.trim() || '';
    if (!token) {
      setMessage('Pega el token de dispositivo generado desde Sedes.', 'error');
      return;
    }
    localStorage.setItem(DEVICE_TOKEN_KEY, token);
    syncDeviceStatus();
    setScannerState(SCANNER_READY_STATE);
    setPausedOverlay(true);
    setMessage('Tablet activada en este navegador.', 'ok');
  }

  function clearDeviceToken() {
    if (!supportUnlocked) {
      setMessage('Toca Soporte para limpiar el token.', 'error');
      return;
    }
    if (!window.confirm('Esta accion desactiva esta tablet hasta pegar nuevamente el token. Continuar?')) return;
    localStorage.removeItem(DEVICE_TOKEN_KEY);
    setScannerState('');
    const input = qs('#deviceToken', ui);
    if (input) input.value = '';
    syncDeviceStatus();
    setMessage('Token de tablet eliminado.', 'muted');
  }

  async function startCamera() {
    const token = getDeviceToken();
    if (!token) {
      setMessage('Activa primero la tablet con su token de dispositivo.', 'error');
      return;
    }
    if (!('BarcodeDetector' in window)) {
      setMessage('Este navegador no soporta lectura QR por camara. Usa la lectura manual.', 'error');
      return;
    }
    detector = detector || new window.BarcodeDetector({ formats: ['qr_code'] });
    clearPauseTimer();
    setMessage('Activando camara...', 'muted');
    stream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: 'user' }, audio: false });
    const video = qs('#qrVideo', ui);
    video.srcObject = stream;
    await video.play();
    scanning = true;
    setScannerState(SCANNER_READY_STATE);
    setPausedOverlay(false);
    requestWakeLock();
    setMessage('Camara activa. Acerca el QR al recuadro.', 'muted');
    schedulePause(IDLE_PAUSE_MS, 'idle');
    scanLoop();
  }

  function stopCamera({ silent = false, keepResumeState = false } = {}) {
    clearPauseTimer();
    scanning = false;
    if (stream) {
      stream.getTracks().forEach((track) => track.stop());
      stream = null;
    }
    const video = qs('#qrVideo', ui);
    if (video) video.srcObject = null;
    releaseWakeLock();
    if (!silent) {
      if (!keepResumeState) setScannerState('');
      setPausedOverlay(false);
      setMessage('Camara detenida.', 'muted');
    }
  }

  async function requestWakeLock() {
    if (!('wakeLock' in navigator) || document.visibilityState !== 'visible') return;
    try {
      if (wakeLock) return;
      wakeLock = await navigator.wakeLock.request('screen');
      wakeLock.addEventListener('release', () => {
        wakeLock = null;
      });
    } catch (_) {}
  }

  function releaseWakeLock() {
    if (!wakeLock) return;
    const lock = wakeLock;
    wakeLock = null;
    lock.release().catch(() => {});
  }

  function handleVisibilityChange() {
    if (document.visibilityState === 'hidden' && scanning) {
      pauseScanner('screen');
      return;
    }
    if (document.visibilityState === 'visible' && scanning) {
      requestWakeLock();
    }
  }

  function handlePageHide() {
    if (scanning || getScannerState() === SCANNER_READY_STATE) setScannerState(SCANNER_READY_STATE);
  }

  async function scanLoop() {
    if (!scanning) return;
    const video = qs('#qrVideo', ui);
    try {
      if (video?.readyState >= 2 && detector) {
        const codes = await detector.detect(video);
        const value = String(codes?.[0]?.rawValue || '').trim();
        if (value) await processQrValue(value);
      }
    } catch (error) {
      console.error('Error leyendo QR:', error);
    }
    if (scanning) window.requestAnimationFrame(scanLoop);
  }

  async function processQrValue(value) {
    const now = Date.now();
    if (value === lastValue && now - lastScanAt < 5000) return;
    lastValue = value;
    lastScanAt = now;
    const deviceToken = getDeviceToken();
    if (!deviceToken) {
      setMessage('La tablet no esta activada.', 'error');
      return;
    }
    try {
      setMessage('Validando QR...', 'muted');
      const result = await deps.scanAttendanceQr?.({ qrValue: value, deviceToken });
      const action = result?.action === 'exit' ? 'Salida' : 'Ingreso';
      const name = result?.employee?.nombre || result?.employee?.documento || 'Empleado';
      const phone = result?.employee?.phoneNumber ? ` Telefono origen: ${result.employee.phoneNumber}.` : '';
      setMessage(`${action} registrado: ${name}.${phone}`, 'ok');
      schedulePause(AFTER_SCAN_PAUSE_MS, 'after_scan');
    } catch (error) {
      setMessage(error?.message || 'No se pudo validar el QR.', 'error');
      markActivity();
    }
  }

  qs('#btnSaveDevice', ui)?.addEventListener('click', saveDeviceToken);
  qs('#btnClearDevice', ui)?.addEventListener('click', clearDeviceToken);
  qs('#btnUnlockSupport', ui)?.addEventListener('click', toggleSupport);
  qs('#btnStartCamera', ui)?.addEventListener('click', () => startCamera().catch((error) => setMessage(error?.message || 'No se pudo iniciar la camara.', 'error')));
  qs('#btnStopCamera', ui)?.addEventListener('click', () => stopCamera());
  qs('#qrPauseOverlay', ui)?.addEventListener('click', () => startCamera().catch((error) => setMessage(error?.message || 'No se pudo iniciar la camara.', 'error')));
  document.addEventListener('visibilitychange', handleVisibilityChange);
  window.addEventListener('pagehide', handlePageHide);
  ui.addEventListener('pointerdown', markActivity);
  ui.addEventListener('keydown', markActivity);
  qs('#btnManualScan', ui)?.addEventListener('click', () => {
    const value = qs('#manualQr', ui)?.value?.trim() || '';
    if (!value) {
      setMessage('Pega el contenido del QR para validar.', 'error');
      return;
    }
    processQrValue(value);
  });

  restorePausedScannerIfNeeded();
  mount.replaceChildren(ui);
  return () => {
    clearPauseTimer();
    clearSupportTimers();
    document.removeEventListener('visibilitychange', handleVisibilityChange);
    window.removeEventListener('pagehide', handlePageHide);
    stopCamera();
  };
};
