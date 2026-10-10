// IPadd Chat server. No dependencies. Run: node server.js
// Everyone who joins with the same address (room code) is connected in one live room.
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, "public");
const MAX_ROOM_CLIENTS = 200;
const MAX_CLIENTS = 3000;
const HISTORY = 100;
const EMPTY_ROOM_TTL_MS = 30 * 60 * 1000;

const rooms = new Map();   // slug -> { clients: Map(id -> client), history: [], timer }
const clients = new Map(); // id -> client
const MIME = { ".html": "text/html; charset=utf-8", ".css": "text/css", ".js": "text/javascript", ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon", ".txt": "text/plain" };

function slugOf(a) {
  let s = String(a || "").trim().toLowerCase().replace(/[^a-z0-9._:-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60);
  if (/^\.+$/.test(s)) s = "";
  return s;
}
function cleanName(n) { return String(n || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, 24) || "Guest"; }
function clientIp(req) {
  const xf = req.headers["x-forwarded-for"];
  const ip = (xf ? String(xf).split(",")[0] : req.socket.remoteAddress || "").trim();
  return ip.replace(/^::ffff:/, "");
}
function send(client, event, data) {
  try { client.res.write("event: " + event + "\ndata: " + JSON.stringify(data) + "\n\n"); } catch (e) {}
}
function members(room) { return [...room.clients.values()].map(c => ({ id: c.id, name: c.name })); }
function broadcast(room, event, data) { for (const c of room.clients.values()) send(c, event, data); }
function json(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}

function handleJoin(req, res, url) {
  const slug = slugOf(url.searchParams.get("room"));
  const name = cleanName(url.searchParams.get("name"));
  if (!slug) return json(res, 400, { error: "Invalid address" });
  if (clients.size >= MAX_CLIENTS) return json(res, 503, { error: "Server is full" });
  let room = rooms.get(slug);
  if (!room) { room = { clients: new Map(), history: [], timer: null }; rooms.set(slug, room); }
  if (room.clients.size >= MAX_ROOM_CLIENTS) return json(res, 429, { error: "Room is full" });
  clearTimeout(room.timer);

  res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store, no-transform", Connection: "keep-alive", "X-Accel-Buffering": "no" });
  res.write("retry: 2000\n\n");
  const client = { id: crypto.randomUUID(), name, slug, res, last: 0 };
  clients.set(client.id, client);
  room.clients.set(client.id, client);
  send(client, "hello", { id: client.id, room: slug, members: members(room), history: room.history });
  broadcast(room, "members", { members: members(room) });

  const beat = setInterval(() => { try { res.write(": ping\n\n"); } catch (e) {} }, 20000);
  req.on("close", () => {
    clearInterval(beat);
    clients.delete(client.id);
    room.clients.delete(client.id);
    if (room.clients.size) broadcast(room, "members", { members: members(room) });
    else { room.timer = setTimeout(() => { if (!room.clients.size) rooms.delete(slug); }, EMPTY_ROOM_TTL_MS); room.timer.unref(); }
  });
}

function handleSend(req, res) {
  let body = "";
  req.on("data", ch => { body += ch; if (body.length > 4096) req.destroy(); });
  req.on("end", () => {
    let d; try { d = JSON.parse(body); } catch (e) { return json(res, 400, { error: "Bad JSON" }); }
    const client = clients.get(String(d.id || ""));
    if (!client) return json(res, 404, { error: "Not connected" });
    const text = String(d.text || "").replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "").trim().slice(0, 500);
    if (!text) return json(res, 400, { error: "Empty message" });
    const now = Date.now();
    if (now - client.last < 200) return json(res, 429, { error: "Slow down" });
    client.last = now;
    const msg = { id: crypto.randomUUID(), from: client.id, name: client.name, text, ts: now, reply: null };
    if (d.reply && d.reply.text) msg.reply = { name: cleanName(d.reply.name), text: String(d.reply.text).slice(0, 100) };
    const room = rooms.get(client.slug);
    if (!room) return json(res, 404, { error: "Room closed" });
    room.history.push(msg);
    if (room.history.length > HISTORY) room.history.shift();
    broadcast(room, "msg", msg);
    json(res, 200, { ok: true });
  });
}

function serveStatic(req, res, url) {
  let p = decodeURIComponent(url.pathname);
  if (p === "/") p = "/index.html";
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); return res.end("Forbidden"); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { "Content-Type": "text/plain" }); return res.end("Not found"); }
    res.writeHead(200, { "Content-Type": MIME[path.extname(file)] || "application/octet-stream", "Cache-Control": "no-cache" });
    res.end(buf);
  });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  if (req.method === "GET" && url.pathname === "/api/join") return handleJoin(req, res, url);
  if (req.method === "POST" && url.pathname === "/api/send") return handleSend(req, res);
  if (req.method === "GET" && url.pathname === "/api/myip") return json(res, 200, { ip: clientIp(req) });
  if (req.method === "GET" && url.pathname === "/api/stats") return json(res, 200, { rooms: rooms.size, online: clients.size });
  if (req.method === "GET" || req.method === "HEAD") return serveStatic(req, res, url);
  res.writeHead(405); res.end();
});
server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log("IPadd Chat running on port " + PORT));
