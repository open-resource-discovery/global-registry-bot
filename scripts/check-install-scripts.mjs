import { readFileSync } from 'node:fs';

const manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const lock = JSON.parse(readFileSync(new URL('../package-lock.json', import.meta.url), 'utf8'));
const decisions = manifest.allowScripts ?? {};
const missing = new Set();

for (const [location, entry] of Object.entries(lock.packages)) {
  if (!entry.hasInstallScript || !location.startsWith('node_modules/')) continue;

  const segments = location.split('/');
  const packageStart = segments.lastIndexOf('node_modules') + 1;
  const name = entry.name ?? segments.slice(packageStart).join('/');
  const id = name + '@' + entry.version;
  if (!Object.hasOwn(decisions, id) || typeof decisions[id] !== 'boolean') {
    missing.add(id);
  }
}

if (missing.size) {
  const report = ['Missing version-specific allowScripts decisions:', ...Array.from(missing).sort()];
  process.stderr.write(report.join('\n') + '\n');
  process.exitCode = 1;
}
