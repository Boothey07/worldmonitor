import { execSync } from 'node:child_process';
const mod = await import('/root/worldmonitor/scripts/source-attribution.mjs');
const norm = (inv) => {
  const m = new Map();
  for (const h of inv) m.set(h.host, { kinds: [...h.kinds].sort(), refs: h.references.map(r => r.path).sort() });
  return m;
};
const dumpWalk = (root) => execSync(`node -e '
import("/root/worldmonitor/scripts/source-attribution.mjs").then(async () => {
  const fsMod = await import("node:fs");
  // reimplement walk via same source? Simpler: reuse module-internal through env? fall back to find
})'`).toString();

const A = norm(mod.scanUpstreamHosts('/tmp/appsim'));   // git archive HEAD, no .git, no untracked
const B = norm(mod.scanUpstreamHosts('/tmp/dockersim'));// tar-sim of .dockerignore context
const C = norm(mod.scanUpstreamHosts('/root/worldmonitor'));

function report(name, x, y) {
  const onlyX = [...x.keys()].filter(k => !y.has(k));
  const onlyY = [...y.keys()].filter(k => !x.has(k));
  let refDiff = [];
  for (const k of x.keys()) if (y.has(k)) {
    const rx = JSON.stringify(x.get(k)), ry = JSON.stringify(y.get(k));
    if (rx !== ry) refDiff.push(k);
  }
  console.log(`${name}: hosts X=${x.size} Y=${y.size} | onlyX=${onlyX.slice(0,4)} onlyY=${onlyY.slice(0,6)} refdiff=${refDiff.length}`);
  // detail first two refdiffs
  for (const k of refDiff.slice(0, 2)) {
    console.log(`  ${k}`);
    console.log(`    X refs (${x.get(k).refs.length}):`, x.get(k).refs.slice(0,5));
    console.log(`    Y refs (${y.get(k).refs.length}):`, y.get(k).refs.slice(0,5));
  }
}
report('appsim vs dockersim', A, B);
report('dockersim vs worktree', B, C);
