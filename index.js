/* eslint-disable global-require,import/no-dynamic-require */
/**
 * Entry point for the application.
 * */
import { each } from "async";
import { execSync } from "child_process";
import { connect, disconnect, flush, start } from "pm2";
import { apps } from "./manifest.json";

const args = process.argv.slice(2);
const group = args[0] || process.env.GROUP;

if (process.env.PROVIDER === "gce") {
  execSync(
    'curl -H "Metadata-Flavor: Google" -L http://metadata.google.internal/computeMetadata/v1/project/attributes/env > /usr/src/.env'
  );
}
if (process.env.ROLE) {
  // if role variable is set just run that script
  require(`./svc/${process.env.ROLE}.js`);
} else if (group) {
  connect(() => {
    each(
      apps,
      (app, cb) => {
        if (group === app.group) {
          console.log(app.script, app.instances);
          start(
            app.script,
            {
              instances: app.instances,
              restartDelay: 10000,
            },
            (err) => {
              if (err) {
                // Log the error and continue
                console.error(err);
              }
              cb();
            }
          );
        }
      },
      (err) => {
        if (err) {
          console.error(err);
        }
        disconnect();
      }
    );
  });
  // Clean up the logs once an hour
  setInterval(() => flush(), 3600 * 1000);
} else {
  // Block indefinitely (keep process alive for Docker)
  process.stdin.resume();
}
