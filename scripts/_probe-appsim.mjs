import { readFileSync } from 'node:fs';
const ROOT = process.argv[2];
const mod = await import('/root/worldmonitor/scripts/source-attribution.mjs');
const manifest = JSON.parse(readFileSync('/root/worldmonitor/shared/source-attribution-manifest.json', 'utf8'));
const inv = mod.scanUpstreamHosts(ROOT);
console.log('hosts:', inv.length);
try {
  mod.sourceAttributionStats(inv, manifest);
  console.log('VALID');
} catch (e) {
  console.log('INVALID:', String(e.message || e).slice(0, 500));
}
