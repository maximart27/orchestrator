// Tests for the multi-tenant URL handling of the web UI. No dependencies, run with:
//   node tests/js/tenant.test.js
'use strict';
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..', '..', 'resources');
const jsDir = path.join(root, 'public', 'js');
const T = require(path.join(jsDir, 'tenant.js'));

let passed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('ok   - ' + name); }
  catch (e) { console.log('FAIL - ' + name + '\n' + e.stack); process.exitCode = 1; }
}

// --- legacy pages: nothing changes ---
test('legacy page: URLs are untouched', () => {
  for (const u of ['/api/clusters-info', '/api/cluster/h:3306', '/web/cluster/h:3306?compact=true', '/images/a.png', '/api/health']) {
    assert.strictEqual(T.rewriteUrl(u, '/web/clusters'), u);
  }
});
test('legacy page behind a proxy prefix: only the prefix is added (as before)', () => {
  assert.strictEqual(T.rewriteUrl('/api/clusters-info', '/proxy/orc/web/clusters'), '/proxy/orc/api/clusters-info');
  assert.strictEqual(T.rewriteUrl('/web/cluster/a', '/proxy/orc/web/cluster/x'), '/proxy/orc/web/cluster/a');
});
test('page without /web/: URL returned as is', () => {
  assert.strictEqual(T.rewriteUrl('/api/x', '/'), '/api/x');
});
test('the tenants overview page (/web/tenants) has no tenant', () => {
  assert.strictEqual(T.currentTenant('/web/tenants'), '');
  assert.strictEqual(T.currentTenant('/web/tenants/'), '');
  assert.strictEqual(T.rewriteUrl('/api/tenants', '/web/tenants'), '/api/tenants');
  assert.strictEqual(T.rewriteUrl('/web/tenants/t1/clusters', '/web/tenants'), '/web/tenants/t1/clusters');
});

// --- tenant pages ---
const page = '/web/tenants/mariadb-a/cluster/h:3306';
test('tenant is read from the page URL', () => {
  assert.strictEqual(T.currentTenant(page), 'mariadb-a');
  assert.strictEqual(T.currentTenant('/web/tenants/a_b-1'), 'a_b-1');
  assert.strictEqual(T.currentTenant('/p/web/tenants/t1/clusters'), 't1');
});
test('api calls are scoped to the tenant', () => {
  assert.strictEqual(T.rewriteUrl('/api/clusters-info', page), '/api/tenants/mariadb-a/clusters-info');
  assert.strictEqual(T.rewriteUrl('/api/begin-downtime/h/3306/o/r/1h', page), '/api/tenants/mariadb-a/begin-downtime/h/3306/o/r/1h');
  assert.strictEqual(T.rewriteUrl('/api/ack-recovery/3?comment=x%20y', page), '/api/tenants/mariadb-a/ack-recovery/3?comment=x%20y');
  assert.strictEqual(T.rewriteUrl('/api/problems', page), '/api/tenants/mariadb-a/problems');
});
test('web links are scoped to the tenant', () => {
  assert.strictEqual(T.rewriteUrl('/web/cluster/alias/x?compact=true', page), '/web/tenants/mariadb-a/cluster/alias/x?compact=true');
  assert.strictEqual(T.rewriteUrl('/web/search?s=h:3306', page), '/web/tenants/mariadb-a/search?s=h:3306');
  assert.strictEqual(T.rewriteUrl('/web/audit-recovery/uid/abc', page), '/web/tenants/mariadb-a/audit-recovery/uid/abc');
});
test('global API endpoints are never scoped', () => {
  for (const g of ['health', 'status', 'leader-check', 'routed-leader-check', 'lb-check', '_ping', 'grab-election', 'reelect',
    'metrics', 'headers', 'tenants', 'all-tenants/clusters-info', 'reload-configuration', 'agents', 'seeds', 'raft-state', 'raft-peers', 'raft-yield-hint/node1', 'raft-health?x=1', 'leader-check/418']) {
    assert.strictEqual(T.rewriteUrl('/api/' + g, page), '/api/' + g, g);
  }
});
test('names that merely start like a global endpoint are scoped', () => {
  assert.strictEqual(T.rewriteUrl('/api/health-ish', page), '/api/tenants/mariadb-a/health-ish');
  assert.strictEqual(T.rewriteUrl('/api/status-x/1', page), '/api/tenants/mariadb-a/status-x/1');
});
test('already tenant-scoped URLs and static assets are untouched', () => {
  assert.strictEqual(T.rewriteUrl('/api/tenants/other/clusters-info', page), '/api/tenants/other/clusters-info');
  assert.strictEqual(T.rewriteUrl('/web/tenants/other/clusters', page), '/web/tenants/other/clusters');
  assert.strictEqual(T.rewriteUrl('/web/tenants', page), '/web/tenants');
  assert.strictEqual(T.rewriteUrl('/images/orchestrator-logo-32.png', page), '/images/orchestrator-logo-32.png');
  assert.strictEqual(T.rewriteUrl('/js/x.js', page), '/js/x.js');
});
test('URLPrefix / proxy prefix goes before everything, tenant after /web/', () => {
  const p = '/proxy/orc/web/tenants/t1/clusters';
  assert.strictEqual(T.rewriteUrl('/api/clusters-info', p), '/proxy/orc/api/tenants/t1/clusters-info');
  assert.strictEqual(T.rewriteUrl('/web/cluster/a', p), '/proxy/orc/web/tenants/t1/cluster/a');
  assert.strictEqual(T.rewriteUrl('/api/tenants', p), '/proxy/orc/api/tenants');
  assert.strictEqual(T.rewriteUrl('/images/a.png', p), '/proxy/orc/images/a.png');
});
test('a tenant called "web" does not confuse the prefix detection', () => {
  assert.strictEqual(T.currentTenant('/web/tenants/web/clusters'), 'web');
  assert.strictEqual(T.rewriteUrl('/api/clusters-info', '/web/tenants/web/clusters'), '/api/tenants/web/clusters-info');
});
test('invalid tenant ids in the page URL are not treated as tenants', () => {
  assert.strictEqual(T.currentTenant('/web/tenants/Bad.Id/clusters'), '');
});

// --- server-rendered links ---
test('tenantizeHref scopes server-rendered web links, keeping the configured prefix', () => {
  assert.strictEqual(T.tenantizeHref('/web/discover', 't1'), '/web/tenants/t1/discover');
  assert.strictEqual(T.tenantizeHref('/orc/web/clusters', 't1'), '/orc/web/tenants/t1/clusters');
  assert.strictEqual(T.tenantizeHref('/web/tenants', 't1'), '/web/tenants');
  assert.strictEqual(T.tenantizeHref('/web/tenants/t2/clusters', 't1'), '/web/tenants/t2/clusters');
  assert.strictEqual(T.tenantizeHref('#', 't1'), '#');
  assert.strictEqual(T.tenantizeHref('https://example.com/web/x', 't1'), 'https://example.com/web/x');
  assert.strictEqual(T.tenantizeHref('/web/discover', ''), '/web/discover');
});

// --- selector navigation ---
test('switching tenant keeps tenant-agnostic pages and falls back to clusters otherwise', () => {
  assert.strictEqual(T.switchTenantPath('/web/tenants/a/audit-recovery/2', 'b'), '/web/tenants/b/audit-recovery');
  assert.strictEqual(T.switchTenantPath('/web/tenants/a/clusters', 'b'), '/web/tenants/b/clusters');
  assert.strictEqual(T.switchTenantPath('/web/tenants/a/cluster/h:3306', 'b'), '/web/tenants/b/clusters');
  assert.strictEqual(T.switchTenantPath('/web/tenants/a/search', 'b'), '/web/tenants/b/clusters');
  assert.strictEqual(T.switchTenantPath('/web/discover', 'b'), '/web/tenants/b/discover');
  assert.strictEqual(T.switchTenantPath('/web/cluster/alias/x', 'b'), '/web/tenants/b/clusters');
  assert.strictEqual(T.switchTenantPath('/p/web/tenants/a/status', 'b'), '/p/web/tenants/b/status');
  assert.strictEqual(T.switchTenantPath('/web/tenants', 'b'), '/web/tenants/b/clusters');
});

// --- response parsing ---
test('normalizeTenantList accepts the API envelope, a bare array and strings', () => {
  const want = [{id: 'a', provider: 'mysql'}, {id: 'b', provider: 'postgresql'}];
  assert.deepStrictEqual(T.normalizeTenantList({Code: 'OK', Details: [{ID: 'a', Provider: 'mysql'}, {ID: 'b', Provider: 'postgresql'}]}), want);
  assert.deepStrictEqual(T.normalizeTenantList([{id: 'a', provider: 'mysql'}, {tenantId: 'b', engine: 'postgresql'}]), want);
  assert.deepStrictEqual(T.normalizeTenantList({Details: ['a', 'b']}), [{id: 'a', provider: ''}, {id: 'b', provider: ''}]);
  assert.deepStrictEqual(T.normalizeTenantList(null), []);
  assert.deepStrictEqual(T.normalizeTenantList({Code: 'ERROR'}), []);
});
test('normalizeTenantHealth reads Details.Tenants[]', () => {
  const h = {Details: {Tenants: [{ID: 'a', Healthy: true}, {ID: 'b', Healthy: false, Message: 'backend down'}]}};
  assert.deepStrictEqual(T.normalizeTenantHealth(h), {a: {healthy: true, message: ''}, b: {healthy: false, message: 'backend down'}});
  assert.deepStrictEqual(T.normalizeTenantHealth({Details: {}}), {});
  assert.deepStrictEqual(T.normalizeTenantHealth(null), {});
});

// --- wiring: appUrl() in orchestrator.js really goes through tenant.js ---
function loadAppUrl(pathname) {
  const src = fs.readFileSync(path.join(jsDir, 'orchestrator.js'), 'utf8');
  const start = src.indexOf('function appUrl(url) {');
  const end = src.indexOf('\nfunction ', start + 1);
  assert.ok(start >= 0 && end > start, 'appUrl not found in orchestrator.js');
  return new Function('window', 'OrchestratorTenant', src.slice(start, end) + '\nreturn appUrl;')({location: {pathname}}, T);
}
test('orchestrator.js appUrl() applies the tenant', () => {
  assert.strictEqual(loadAppUrl('/web/tenants/t1/clusters')('/api/clusters-info'), '/api/tenants/t1/clusters-info');
  assert.strictEqual(loadAppUrl('/web/clusters')('/api/clusters-info'), '/api/clusters-info');
});

// --- static audit: no URL reaches the network or the location without going through appUrl() ---
const sources = fs.readdirSync(jsDir).filter(f => f.endsWith('.js') && !/\.min\.js$/.test(f) &&
  !/^(d3|jquery|corex|corex-jquery|md5|bootbox|jquery-ui)/.test(f) && f !== 'tenant.js' && f !== 'bootstrap-legacy-bridge.js');
test('every jQuery request in the UI goes through appUrl()', () => {
  const offenders = [];
  for (const f of sources) {
    fs.readFileSync(path.join(jsDir, f), 'utf8').split('\n').forEach((line, i) => {
      if (/\$\.(get|post|ajax|getJSON)\(/.test(line) && !/\$\.(get|post|ajax|getJSON)\(\s*appUrl\(/.test(line)) {
        offenders.push(f + ':' + (i + 1) + ': ' + line.trim());
      }
      if (/\bfetch\(|XMLHttpRequest/.test(line)) { offenders.push(f + ':' + (i + 1) + ': ' + line.trim()); }
    });
  }
  assert.deepStrictEqual(offenders, []);
});
test('no hardcoded /web/ or /api/ href/action literal bypasses appUrl()', () => {
  const offenders = [];
  const files = sources.map(f => path.join(jsDir, f));
  for (const f of files) {
    fs.readFileSync(f, 'utf8').split('\n').forEach((line, i) => {
      if (/(href|action)\s*=\s*\\?["']\s*\/(web|api)\//.test(line)) { offenders.push(path.basename(f) + ':' + (i + 1) + ': ' + line.trim()); }
    });
  }
  assert.deepStrictEqual(offenders, []);
});
test('every "/api/..." or "/web/..." literal is consumed by appUrl/apiCommand/getData or passed to a helper that calls appUrl', () => {
  // Literals are only legal as arguments of: appUrl(, apiCommand(, getData(, or assigned to the
  // known URI variables later handed to those. Anything else is flagged for review.
  const allowed = /^\s*return \x27|appUrl\(|apiCommand\(|getData\(|\b(apiUri|apiUrl|problemsURI|uri|path|auditPath|var path|routePrefix)\b\s*(=|\+=)|^\s*"\/(web|api)\/[^"]*",?\s*$|buildRecoveryAuditLink|buildFailureDetectionLink/;
  const offenders = [];
  for (const f of sources) {
    fs.readFileSync(path.join(jsDir, f), 'utf8').split('\n').forEach((line, i) => {
      if (/["']\/(api|web)\//.test(line) && !/^\s*(\/\/|\*)/.test(line) && !allowed.test(line)) {
        offenders.push(f + ':' + (i + 1) + ': ' + line.trim());
      }
    });
  }
  assert.deepStrictEqual(offenders, []);
});

console.log('\npassed: ' + passed);
