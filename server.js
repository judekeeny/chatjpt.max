const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const port = Number(process.env.PORT || 8000);
const rootDir = __dirname;

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.ico': 'image/x-icon',
};

const rooms = new Map();

function leaveRoom(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;

  const room = rooms.get(roomId);
  if (!room) return;

  room.delete(ws);
  if (room.size === 0) {
    rooms.delete(roomId);
  }

  delete ws.roomId;
}

function joinRoom(ws, roomId) {
  const safeRoom = String(roomId || '').trim();
  if (!safeRoom) return;

  leaveRoom(ws);

  if (!rooms.has(safeRoom)) rooms.set(safeRoom, new Set());
  rooms.get(safeRoom).add(ws);
  ws.roomId = safeRoom;
}

function relayMessage(ws, payload) {
  const { roomId, topic, message, id, ts } = payload || {};
  if (!roomId || !topic) return;

  const room = rooms.get(String(roomId));
  if (!room) return;

  const envelope = {
    roomId: String(roomId),
    topic,
    message: String(message ?? ''),
    id: id || `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    ts: ts || Date.now(),
  };

  for (const client of room) {
    if (client.readyState === client.OPEN && client !== ws) {
      client.send(JSON.stringify(envelope));
    }
  }
}

const server = http.createServer((req, res) => {
  const requestPath = req.url.split('?')[0] || '/';
  const safePath = requestPath === '/' ? '/index.html' : requestPath;
  const filePath = path.join(rootDir, safePath);

  if (!filePath.startsWith(rootDir)) {
    res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Forbidden');
    return;
  }

  fs.stat(filePath, (error, stats) => {
    if (error || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    res.writeHead(200, { 'Content-Type': contentType });
    fs.createReadStream(filePath).pipe(res);
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      if (data?.type === 'joinRoom') {
        joinRoom(ws, data.roomId);
        return;
      }

      if (data?.type === 'message') {
        relayMessage(ws, data);
      }
    } catch (error) {
      // Ignore malformed packets.
    }
  });

  ws.on('close', () => leaveRoom(ws));
});

server.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`);
});
