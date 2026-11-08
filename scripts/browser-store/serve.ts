import { resolve } from "node:path";
import { startServer } from "./server";

const root = resolve(process.argv[2] ?? ".nddev-reports/browser-store");
const { server, manifest } = await startServer(root);
console.log(
  JSON.stringify({ url: server.url.href, source: manifest.source, baseline: manifest.baseline }),
);
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    server.stop(true);
    process.exit(0);
  });
}
