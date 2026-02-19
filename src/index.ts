import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath, pathToFileURL } from 'url';
import type { ApplicationFunctionOptions, Probot } from 'probot';
import { coreSecrets } from './utils/secrets.js';

const { APP_ID, WEBHOOK_SECRET, PRIVATE_KEY } = coreSecrets;

if (!APP_ID || !WEBHOOK_SECRET || !PRIVATE_KEY) {
  throw new Error('Missing secrets: APP_ID/WEBHOOK_SECRET/PRIVATE_KEY');
}

type HandlerEntry = (app: Probot, options: ApplicationFunctionOptions) => void | Promise<void>;

type HandlerModule = {
  default?: HandlerEntry;
};

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

const appEntry: HandlerEntry = async (app, options) => {
  app.log.info('Request Bot booting...');

  const _filename = fileURLToPath(import.meta.url);
  const _dirname = path.dirname(_filename);
  const handlersPath = path.join(_dirname, 'handlers');

  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(handlersPath, { withFileTypes: true });
  } catch (e: unknown) {
    const err = asError(e);
    app.log.error({ err }, `Failed reading handlers directory: ${err.message}`);
    return;
  }

  const dirs = entries.filter((ent) => ent.isDirectory()).map((d) => d.name);
  app.log.info(`Found handler directories: ${dirs.join(', ')}`);

  for (const name of dirs) {
    const candidate = path.join(handlersPath, name, 'index.js');

    try {
      if (!fs.existsSync(candidate)) {
        app.log.debug(`Skipping handler '${name}' (no index.js)`);
        continue;
      }

      const url = pathToFileURL(candidate).href;
      const mod = (await import(url)) as unknown as HandlerModule;

      if (typeof mod.default === 'function') {
        await mod.default(app, options);
        app.log.info(`Loaded handler: ${name}/index.js`);
      } else {
        app.log.warn(`Skip ${name}/index.js (no default export function)`);
      }
    } catch (e: unknown) {
      const err = asError(e);
      app.log.error({ err }, `Failed loading handler entry for ${name}: ${err.stack ?? err.message}`);
    }
  }

  app.log.info('Request Bot ready (handlers scanned)');
};

export default appEntry;
