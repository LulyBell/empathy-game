const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const dir = path.join(__dirname, 'public');
const cases = JSON.parse(fs.readFileSync(path.join(dir, 'content.json'), 'utf8'));
const icons = ['🩺', '💉', '🌡️', '🩹', '💊', '❤️', '📋'];
const MAX_PLAYERS = 7, Q_MS = 10000, FAST_MS = 4000;
const rooms = new Map();

function newRoom(code) {
  return { code, phase: 'lobby', caseIndex: 0, qIndex: 0, reader: null, answers: {},
    questionStartedAt: 0, players: [], sockets: new Set(), hostId: null, timer: null, lastActive: Date.now() };
}
function pub(r) {
  return {
    room: r.code, phase: r.phase, caseIndex: r.caseIndex, qIndex: r.qIndex, totalCases: cases.length,
    reader: r.reader ? { id: r.reader.id, name: r.reader.name, icon: r.reader.icon } : null,
    questionStartedAt: r.questionStartedAt, questionMs: Q_MS, serverNow: Date.now(),
    answers: r.answers,
    players: r.players.map(p => ({ id: p.id, name: p.name, icon: p.icon, score: p.score,
      correct: p.correct, fastCount: p.fastCount, online: p.online, caseCorrect: p.caseCorrect, caseFast: p.caseFast }))
  };
}
function broadcast(r) {
  r.lastActive = Date.now();
  const s = JSON.stringify({ type: 'state', state: pub(r) });
  r.sockets.forEach(x => x.readyState === 1 && x.send(s));
}
function clearTimer(r) { if (r.timer) { clearTimeout(r.timer); r.timer = null; } }

function finish(r) {
  if (r.phase !== 'question') return;
  clearTimer(r);
  const correct = cases[r.caseIndex].questions[r.qIndex].answer;
  for (const p of r.players) {
    const a = r.answers[p.id];
    if (a && a.choice === correct) {
      p.score += 100; p.correct++; p.caseCorrect++;
      if (a.ms <= FAST_MS) { p.fastCount++; p.caseFast++; }
    }
  }
  r.phase = 'reveal';
  broadcast(r);
}
function startQuestion(r) {
  clearTimer(r);
  r.phase = 'question'; r.answers = {}; r.questionStartedAt = Date.now();
  r.timer = setTimeout(() => finish(r), Q_MS + 300);
  broadcast(r);
}
function startCase(r, i) {
  clearTimer(r);
  r.caseIndex = i; r.qIndex = 0; r.phase = 'case';
  r.players.forEach(p => { p.caseCorrect = 0; p.caseFast = 0; });
  r.reader = r.players.length ? r.players[i % r.players.length] : null;
  broadcast(r);
}
function allAnswered(r) {
  const active = r.players.filter(p => p.online);
  return active.length > 0 && active.every(p => r.answers[p.id] !== undefined);
}

const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg' };
const server = http.createServer((req, res) => {
  let u;
  try { u = decodeURIComponent(req.url.split('?')[0]); } catch { return res.writeHead(400).end(); }
  if (u === '/healthz') return res.writeHead(200).end('ok');
  if (u === '/') u = '/index.html';
  const f = path.join(dir, u);
  if (!f.startsWith(dir) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) return res.writeHead(404).end('Not found');
  res.writeHead(200, { 'Content-Type': types[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
  fs.createReadStream(f).pipe(res);
});

const wss = new WebSocketServer({ server });
wss.on('connection', ws => {
  let r = null, p = null, role = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', buf => {
    let m; try { m = JSON.parse(buf); } catch { return; }

    if (m.type === 'join') {
      const code = String(m.room || '').trim().toUpperCase().slice(0, 12);
      const id = String(m.id || '').slice(0, 64);
      if (!code || !id) return;
      role = m.role === 'host' ? 'host' : 'player';
      r = rooms.get(code);
      if (!r) {
        if (role !== 'host') { ws.send(JSON.stringify({ type: 'error', error: 'nogame' })); return; }
        r = newRoom(code); rooms.set(code, r);
      }
      r.sockets.add(ws);
      if (role === 'host') {
        if (!r.hostId) r.hostId = id;
        if (r.hostId !== id) role = 'viewer';
      } else {
        p = r.players.find(x => x.id === id);
        if (p) { p.online = true; p.socket = ws; if (m.name) p.name = String(m.name).slice(0, 24); }
        else if (r.players.length >= MAX_PLAYERS) { ws.send(JSON.stringify({ type: 'error', error: 'full' })); role = 'viewer'; }
        else {
          const name = String(m.name || '').trim().slice(0, 24) || 'משתתף/ת';
          p = { id, name, icon: icons[r.players.length % icons.length], score: 0, correct: 0, fastCount: 0,
            caseCorrect: 0, caseFast: 0, online: true, socket: ws };
          r.players.push(p);
        }
      }
      ws.send(JSON.stringify({ type: 'joined', role }));
      broadcast(r);
      return;
    }
    if (!r) return;

    if (role === 'host') {
      if (m.type === 'start' && r.phase === 'lobby') startCase(r, 0);
      else if (m.type === 'question' && r.phase === 'case') startQuestion(r);
      else if (m.type === 'reveal' && r.phase === 'question') finish(r);
      else if (m.type === 'next' && r.phase === 'reveal') {
        if (r.qIndex < cases[r.caseIndex].questions.length - 1) { r.qIndex++; startQuestion(r); }
        else { r.phase = 'learn'; broadcast(r); }
      } else if (m.type === 'nextCase' && r.phase === 'learn') {
        if (r.caseIndex + 1 >= cases.length) { r.phase = 'final'; broadcast(r); }
        else startCase(r, r.caseIndex + 1);
      } else if (m.type === 'restart') {
        clearTimer(r);
        r.players = r.players.filter(x => x.online);
        r.players.forEach(x => Object.assign(x, { score: 0, correct: 0, fastCount: 0, caseCorrect: 0, caseFast: 0 }));
        Object.assign(r, { phase: 'lobby', caseIndex: 0, qIndex: 0, reader: null, answers: {} });
        broadcast(r);
      } else if (m.type === 'kick' && r.phase === 'lobby') {
        r.players = r.players.filter(x => x.id !== m.playerId);
        r.players.forEach((x, i) => x.icon = icons[i % icons.length]);
        broadcast(r);
      }
    } else if (role === 'player' && p && m.type === 'answer' && r.phase === 'question' && r.answers[p.id] === undefined) {
      const choice = Number(m.answer);
      if (!Number.isInteger(choice) || choice < 0 || choice > 3) return;
      const ms = Date.now() - r.questionStartedAt;
      if (ms > Q_MS + 300) return;
      r.answers[p.id] = { choice, ms };
      if (allAnswered(r)) finish(r); else broadcast(r);
    }
  });

  ws.on('close', () => {
    if (!r) return;
    r.sockets.delete(ws);
    if (p && p.socket === ws) {
      p.online = false;
      // In the lobby, a player who leaves is removed; mid-game they keep their score and can rejoin.
      if (r.phase === 'lobby') {
        setTimeout(() => {
          if (!p.online && r.phase === 'lobby') {
            r.players = r.players.filter(x => x !== p);
            r.players.forEach((x, i) => x.icon = icons[i % icons.length]);
            broadcast(r);
          }
        }, 15000);
      }
      if (r.phase === 'question' && allAnswered(r)) finish(r); else broadcast(r);
    }
  });
});

// Keep connections alive through proxies (Render drops idle sockets) and clean old rooms.
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
  const now = Date.now();
  for (const [code, r] of rooms) if (r.sockets.size === 0 && now - r.lastActive > 3 * 3600 * 1000) { clearTimer(r); rooms.delete(code); }
}, 25000);

server.listen(PORT, () => console.log('Listening on http://localhost:' + PORT));
