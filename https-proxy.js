const https = require('https');
const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const options = {
  key: fs.readFileSync('/mnt/mmc1-4/poker-app/certs/key.pem'),
  cert: fs.readFileSync('/mnt/mmc1-4/poker-app/certs/cert.pem')
};

// HTTPS 服务器，代理 HTTP 请求和 WebSocket
const server = https.createServer(options, (req, res) => {
  const proxyReq = http.request({
    hostname: '127.0.0.1',
    port: 3017,
    path: req.url,
    method: req.method,
    headers: req.headers
  }, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res);
  });
  proxyReq.on('error', (e) => {
    res.writeHead(502);
    res.end('Bad Gateway');
  });
  req.pipe(proxyReq);
});

// WebSocket 代理
const wss = new WebSocketServer({ server });
wss.on('connection', (ws, req) => {
  const target = new (require('ws'))('ws://127.0.0.1:3017' + req.url);
  ws.on('message', (msg) => target.send(msg));
  target.on('message', (msg) => ws.send(msg));
  ws.on('close', () => target.close());
  target.on('close', () => ws.close());
  target.on('error', () => ws.close());
});

server.listen(8443, () => {
  console.log('HTTPS proxy running on :8443 -> http://127.0.0.1:3017');
});
