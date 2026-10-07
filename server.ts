// VLESS + WebSocket + TLS server for Deno Deploy.
// Public endpoint: wss://<project>.deno.net/<UUID>
// Accepts a VLESS handshake, opens a TCP connection to the requested
// target with Deno.connect, and relays bidirectionally.

const UUID = Deno.env.get("UUID") || "c0f7c589-e896-4fea-a780-36ba0e0c93f2";
const WS_PATH = "/" + UUID;
const PORT = Number(Deno.env.get("PORT") || "8000");

function uuidToBytes(u: string): Uint8Array {
  const hex = u.replace(/-/g, "");
  const b = new Uint8Array(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return b;
}
const UUID_BYTES = uuidToBytes(UUID);

interface VlessReq {
  port: number;
  host: string;
  headerLen: number;
}

/** Parse a VLESS request header. Returns null when the buffer is too short
 *  or the header is invalid. Command byte lives at 18 + addonLen. */
function parseVless(buf: Uint8Array): VlessReq | null {
  if (buf.length < 19) return null;
  for (let i = 0; i < 16; i++) {
    if (buf[1 + i] !== UUID_BYTES[i]) return null; // bad uuid
  }
  const cmdIdx = 18 + buf[17];
  if (buf.length < cmdIdx + 4) return null;
  if (buf[cmdIdx] !== 0x01) return null; // TCP only
  const port = (buf[cmdIdx + 1] << 8) | buf[cmdIdx + 2];
  const atyp = buf[cmdIdx + 3];
  let host = "";
  let headerLen = 0;
  if (atyp === 0x01) {
    // IPv4
    if (buf.length < cmdIdx + 8) return null;
    host = `${buf[cmdIdx + 4]}.${buf[cmdIdx + 5]}.${buf[cmdIdx + 6]}.${buf[cmdIdx + 7]}`;
    headerLen = cmdIdx + 8;
  } else if (atyp === 0x02) {
    // domain
    const dlen = buf[cmdIdx + 4];
    if (buf.length < cmdIdx + 5 + dlen) return null;
    host = new TextDecoder().decode(buf.subarray(cmdIdx + 5, cmdIdx + 5 + dlen));
    headerLen = cmdIdx + 5 + dlen;
  } else if (atyp === 0x03) {
    // IPv6
    if (buf.length < cmdIdx + 20) return null;
    const parts: string[] = [];
    for (let i = 0; i < 8; i++) {
      parts.push(((buf[cmdIdx + 4 + i * 2] << 8) | buf[cmdIdx + 5 + i * 2]).toString(16));
    }
    host = parts.join(":");
    headerLen = cmdIdx + 20;
  } else {
    return null;
  }
  return { port, host, headerLen };
}

async function pumpToWs(conn: Deno.Conn, socket: WebSocket, done: () => void) {
  const tmp = new Uint8Array(65536);
  try {
    while (true) {
      const n = await conn.read(tmp);
      if (n === null) break;
      if (socket.readyState !== WebSocket.OPEN) break;
      socket.send(tmp.slice(0, n));
    }
  } catch {
    // connection reset; fall through to cleanup
  }
  done();
}

function handleVless(socket: WebSocket) {
  socket.binaryType = "arraybuffer";
  let buf = new Uint8Array(0);
  let conn: Deno.Conn | null = null;
  let header: VlessReq | null = null;
  let closed = false;

  const cleanup = () => {
    if (closed) return;
    closed = true;
    try { socket.close(); } catch { /* noop */ }
    try { conn?.close(); } catch { /* noop */ }
  };

  socket.onmessage = async (ev: MessageEvent) => {
    if (closed) return;
    const d = ev.data;
    let chunk: Uint8Array;
    if (d instanceof ArrayBuffer) chunk = new Uint8Array(d);
    else if (d instanceof Uint8Array) chunk = d;
    else return; // ignore text frames

    if (!header) {
      const grown = new Uint8Array(buf.length + chunk.length);
      grown.set(buf);
      grown.set(chunk, buf.length);
      buf = grown;
      const h = parseVless(buf);
      if (!h) return; // header fragmented; wait for more
      header = h;
      try {
        conn = await Deno.connect({ hostname: h.host, port: h.port });
      } catch {
        cleanup();
        return;
      }
      socket.send(new Uint8Array([0x00, 0x00])); // VLESS response header
      const rest = buf.subarray(h.headerLen);
      if (rest.length > 0) {
        try {
          await conn.write(rest);
        } catch {
          cleanup();
          return;
        }
      }
      buf = new Uint8Array(0);
      pumpToWs(conn, socket, cleanup);
    } else if (conn) {
      try {
        await conn.write(chunk);
      } catch {
        cleanup();
      }
    }
  };
  socket.onclose = cleanup;
  socket.onerror = cleanup;
}

Deno.serve({ port: PORT }, (req: Request) => {
  const url = new URL(req.url);
  if (
    url.pathname === WS_PATH &&
    req.headers.get("upgrade")?.toLowerCase() === "websocket"
  ) {
    const { socket, response } = Deno.upgradeWebSocket(req);
    handleVless(socket);
    return response;
  }
  return new Response("Not Found", { status: 404 });
});

console.log(`listening on :${PORT}, ws path ${WS_PATH}`);
