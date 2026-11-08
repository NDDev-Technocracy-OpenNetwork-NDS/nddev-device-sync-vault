import { loadArtifact } from "./artifact";

export async function startServer(root: string) {
  const { assets, manifest } = await loadArtifact(root);
  const headers = {
    "Cache-Control": "no-store",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Embedder-Policy": "require-corp",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy":
      "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'; connect-src 'self'; style-src 'self'; base-uri 'none'; frame-ancestors 'none'",
  };
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      if (request.method !== "GET" && request.method !== "HEAD")
        return new Response(null, { status: 405, headers });
      const path = new URL(request.url).pathname;
      if (path === "/favicon.ico") return new Response(null, { status: 204, headers });
      const name = path === "/" ? "/index.html" : path;
      const data = assets.get(name);
      if (!data) return new Response(null, { status: 404, headers });
      const mime = name.endsWith(".wasm")
        ? "application/wasm"
        : name.endsWith(".js")
          ? "text/javascript"
          : name.endsWith(".css")
            ? "text/css"
            : "text/html";
      return new Response(request.method === "HEAD" ? null : data, {
        headers: { ...headers, "Content-Type": mime, "Content-Length": String(data.length) },
      });
    },
  });
  return { server, manifest };
}
