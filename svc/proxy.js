/**
 * Worker proxying requests to the Steam API.
 * */
import { createServer } from "http";
import { createProxyServer } from "http-proxy";
import { PORT as _PORT, PROXY_PORT } from "../config";

const PORT = _PORT || PROXY_PORT;
const proxy = createProxyServer({
  target: "http://api.steampowered.com",
  changeOrigin: true,
});

const server = createServer((req, res) => {
  if (req.url === "/healthz") {
    return res.end("ok");
  }
  return proxy.web(req, res);
});

server.listen(PORT);
console.log("listening on port %s", PORT);
