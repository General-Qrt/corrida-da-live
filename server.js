// Corrida da Live — servidor (Node.js >= 20)
// Serve o jogo (index.html) e repassa eventos REAIS do TikTok LIVE ao navegador por WebSocket.
// Nenhuma credencial do TikTok é usada: a biblioteca lê lives públicas pelo @usuário.
import http from 'node:http';
import crypto from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { WebSocketServer } from 'ws';
import { TikTokLiveConnection, WebcastEvent } from 'tiktok-live-connector';

const PORT = Number(process.env.PORT) || 3000;
const ADMIN_PIN = process.env.ADMIN_PIN || '';                 // PIN que VOCÊ escolhe (não é senha do TikTok)
const DEFAULT_USER = (process.env.TIKTOK_USERNAME || '').trim().replace(/^@/, '');
const SIGN_KEY = process.env.EULER_API_KEY || '';              // opcional; fica só no servidor
const EXT_GIFT = process.env.EXTENDED_GIFT_INFO !== '0';
const LV2 = Number(process.env.GIFT_LEVEL2_DIAMONDS) || 10;    // diamantes totais para Impulso 2
const LV3 = Number(process.env.GIFT_LEVEL3_DIAMONDS) || 100;   // diamantes totais para Impulso 3
const HTML = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'index.html'));

const BASE = { 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' };
const CSP = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') { res.writeHead(200, { ...BASE, 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { ...BASE, 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'Content-Security-Policy': CSP });
    return res.end(HTML);
  }
  res.writeHead(404, BASE); res.end('404');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2048 });
const state = { status: 'idle', username: null, detail: '' };
let conn = null, wanted = false, retry = 0, retryTimer = null, lastConnect = 0, autoTried = false;
let pinFails = 0, pinLockUntil = 0;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const broadcast = (o) => { const s = JSON.stringify(o); for (const c of wss.clients) if (c.readyState === 1) c.send(s); };
function setStatus(status, detail = '') { state.status = status; state.detail = detail; broadcast({ type: 'status', ...state, control: !!ADMIN_PIN }); }
const clean = (s, n) => String(s ?? '').replace(/[\u0000-\u001f\u007f]/g, '').slice(0, n);
const who = (u) => ({ uniqueId: clean(u?.uniqueId, 40), nickname: clean(u?.nickname, 40) });

function authError(pin) {
  if (!ADMIN_PIN) return 'O servidor está sem ADMIN_PIN configurado.';
  if (Date.now() < pinLockUntil) return 'Muitas tentativas. Aguarde 1 minuto.';
  const h = (v) => crypto.createHash('sha256').update(String(v ?? '')).digest();
  if (crypto.timingSafeEqual(h(pin), h(ADMIN_PIN))) { pinFails = 0; return null; }
  if (++pinFails >= 5) { pinFails = 0; pinLockUntil = Date.now() + 60000; }
  return 'PIN incorreto.';
}

function stop() {
  wanted = false; clearTimeout(retryTimer); retryTimer = null;
  const c = conn; conn = null;
  if (c) c.disconnect().catch(() => {});
}

function scheduleRetry() {
  if (!wanted || retryTimer) return;
  if (++retry > 5) { wanted = false; setStatus('error', 'Conexão perdida. Toque em Conectar para tentar de novo.'); return; }
  setStatus('reconnecting', `Reconectando (${retry}/5)…`);
  retryTimer = setTimeout(() => { retryTimer = null; if (wanted) open(state.username); }, Math.min(60000, 15000 * retry));
}

async function open(username) {
  state.username = username; setStatus('connecting');
  const opts = { enableExtendedGiftInfo: EXT_GIFT };
  if (SIGN_KEY) opts.signApiKey = SIGN_KEY;
  const c = new TikTokLiveConnection(username, opts);
  conn = c;

  c.on('connected', () => { if (conn === c) { retry = 0; setStatus('connected'); } });
  c.on('disconnected', () => {
    if (conn !== c) return;
    conn = null;
    if (wanted) scheduleRetry(); else if (state.status !== 'ended') setStatus('idle');
  });
  c.on('error', (e) => console.error('[tiktok] erro:', e?.info || '', e?.exception?.message || ''));
  c.on(WebcastEvent.STREAM_END, () => { if (conn === c) { wanted = false; setStatus('ended', 'A live foi encerrada.'); } });

  // 1) Comentário !correr — só repassamos comentários que começam com !correr
  c.on(WebcastEvent.CHAT, (d) => {
    const text = clean(d.comment, 100);
    if (!/^\s*!correr\b/i.test(text)) return;
    const w = who(d.user); if (!w.uniqueId) return;
    broadcast({ type: 'comment', source: 'tiktok', ...w, text });
  });

  // 2) Presente — ignora sequências em andamento; só o evento final conta
  c.on(WebcastEvent.GIFT, (d) => {
    if (d.giftDetails?.giftType === 1 && !d.repeatEnd) return;
    const w = who(d.user); if (!w.uniqueId) return;
    const unit = Number(d.giftDetails?.diamondCount ?? d.extendedGiftInfo?.diamond_count ?? d.diamondCount ?? 0) || 0;
    const total = unit * Math.max(1, Number(d.repeatCount) || 1);
    const level = total >= LV3 ? 3 : total >= LV2 ? 2 : 1; // sem valor conhecido => nível 1
    broadcast({ type: 'gift', source: 'tiktok', ...w, giftName: clean(d.giftDetails?.giftName, 30), diamonds: total || null, level });
  });

  // 3) Novo seguidor
  c.on(WebcastEvent.FOLLOW, (d) => {
    const w = who(d.user); if (!w.uniqueId) return;
    broadcast({ type: 'follow', source: 'tiktok', ...w });
  });

  try { await c.connect(); }
  catch (err) {
    if (conn !== c) return;
    conn = null;
    console.error('[tiktok] falha ao conectar:', err?.name || '', err?.message || '');
    if (err?.name === 'UserOfflineError') { wanted = false; setStatus('offline', 'O canal não está ao vivo agora.'); }
    else if (retry > 0) scheduleRetry();
    else { wanted = false; setStatus('error', 'Falha ao conectar. Tente de novo em instantes.'); }
  }
}

function startConnection(username) { stop(); retry = 0; wanted = true; lastConnect = Date.now(); open(username); }

wss.on('connection', (ws) => {
  if (wss.clients.size > 60) return ws.close(1013);
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});
  send(ws, { type: 'status', ...state, control: !!ADMIN_PIN });
  if (DEFAULT_USER && !autoTried && !conn && !wanted) { autoTried = true; startConnection(DEFAULT_USER); }

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.type === 'ping') return send(ws, { type: 'pong' });
    if (m.type !== 'connect' && m.type !== 'disconnect') return;
    const err = authError(m.pin);
    if (err) return send(ws, { type: 'ack', ok: false, message: err });
    if (m.type === 'disconnect') { stop(); autoTried = true; setStatus('idle'); return send(ws, { type: 'ack', ok: true }); }
    const user = String(m.username || '').trim().replace(/^@/, '');
    if (!/^[A-Za-z0-9._]{2,24}$/.test(user)) return send(ws, { type: 'ack', ok: false, message: 'Usuário inválido.' });
    if (Date.now() - lastConnect < 20000) return send(ws, { type: 'ack', ok: false, message: 'Aguarde alguns segundos.' });
    startConnection(user); send(ws, { type: 'ack', ok: true });
  });
});

setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 30000).unref();

for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { stop(); server.close(); setTimeout(() => process.exit(0), 1500).unref(); });
process.on('unhandledRejection', (e) => console.error('[unhandled]', e?.message || e));

server.listen(PORT, '0.0.0.0', () => console.log(`Corrida da Live no ar na porta ${PORT}${ADMIN_PIN ? '' : ' (ADMIN_PIN não definido: conexão com o TikTok desativada)'}`));
