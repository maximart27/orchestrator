/*
   Multi-tenant URL handling for the orchestrator web UI.

   The tenant is taken from the page URL: /web/tenants/<tenant>/<page>. On a legacy page (/web/<page>)
   there is no tenant and every URL is left exactly as it was (single-tenant behavior).

   This file holds pure functions only (no DOM, no jQuery) so it can be exercised from node:
   see tests/js/tenant.test.js. appUrl() in orchestrator.js is the single place that applies them.
*/
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.OrchestratorTenant = factory();
  }
})(this, function () {
  var TENANT_PATH = /\/web\/tenants\/([a-z0-9][a-z0-9_-]{0,62})(?=\/|$)/;

  // First path segment of the API endpoints that are global (never tenant-scoped).
  var GLOBAL_API_SEGMENT = /^(headers|health|status|lb-check|_ping|leader-check|routed-leader-check|grab-election|reelect|metrics|tenants|all-tenants|reload-configuration|agents|agent|agent-[^\/?#]*|seeds|seed|seed-[^\/?#]*|access-token|debug|raft-[^\/?#]*)$/;

  // Web pages that make sense on any tenant (the rest are specific to a tenant's clusters and instances).
  var TENANT_AGNOSTIC_PAGES = {
    "clusters": true,
    "clusters-analysis": true,
    "discover": true,
    "audit": true,
    "audit-failure-detection": true,
    "audit-recovery": true,
    "agents": true,
    "seeds": true,
    "status": true,
    "about": true
  };

  function firstSegment(path) {
    return path.replace(/^\/+/, '').split(/[\/?#]/)[0];
  }

  function isGlobalApiPath(path) {
    return GLOBAL_API_SEGMENT.test(firstSegment(path));
  }

  // parsePathname splits a page pathname into the URL-rewriting proxy prefix (what precedes "/web/"),
  // the tenant ("" on legacy pages), and the page path after the tenant.
  // The tenant lives after /web/, so the proxy prefix logic is the same as for legacy pages.
  function parsePathname(pathname) {
    var m = TENANT_PATH.exec(pathname);
    if (m) {
      return {
        hasWeb: true,
        prefix: pathname.substr(0, m.index),
        tenant: m[1],
        page: pathname.substr(m.index + m[0].length)
      };
    }
    var pos = pathname.lastIndexOf('/web/');
    if (pos < 0) {
      return {hasWeb: false, prefix: '', tenant: '', page: pathname};
    }
    return {hasWeb: true, prefix: pathname.substr(0, pos), tenant: '', page: pathname.substr(pos + '/web'.length)};
  }

  function currentTenant(pathname) {
    return parsePathname(pathname).tenant;
  }

  // scopeUrl adds the tenant to an application URL ("/api/..." or "/web/...").
  // Global API endpoints, the tenant list, already tenant-scoped URLs and static assets are left untouched.
  function scopeUrl(url, tenant) {
    if (!tenant) {
      return url;
    }
    var m = /^\/(api|web)\/(.*)$/.exec(url);
    if (!m) {
      return url;
    }
    var kind = m[1], rest = m[2];
    if (rest === 'tenants' || rest.indexOf('tenants/') === 0 || rest.indexOf('tenants?') === 0) {
      return url;
    }
    if (kind === 'api' && isGlobalApiPath(rest)) {
      return url;
    }
    return '/' + kind + '/tenants/' + tenant + '/' + rest;
  }

  // rewriteUrl is the logic behind appUrl(): it respects URL-rewriting proxies (prefix before "/web/")
  // and scopes the URL to the tenant of the current page, if any.
  function rewriteUrl(url, pathname) {
    var ctx = parsePathname(pathname);
    if (!ctx.hasWeb) {
      return url;
    }
    return ctx.prefix + scopeUrl(url, ctx.tenant);
  }

  // tenantizeHref scopes a server-rendered link (which already carries the configured URL prefix) to a tenant.
  function tenantizeHref(href, tenant) {
    if (!tenant || href.charAt(0) !== '/') {
      return href;
    }
    var i = href.indexOf('/web/');
    if (i < 0) {
      return href;
    }
    var rest = href.substr(i + '/web/'.length);
    if (rest === 'tenants' || rest.indexOf('tenants/') === 0 || rest.indexOf('tenants?') === 0) {
      return href;
    }
    return href.substr(0, i) + '/web/tenants/' + tenant + '/' + rest;
  }

  // switchTenantPath returns the page to open when the user picks another tenant: the same page when it
  // is not tied to the current tenant's data, the clusters dashboard otherwise (a cluster or instance
  // name of one tenant means nothing in another).
  function switchTenantPath(pathname, newTenant) {
    var ctx = parsePathname(pathname);
    var page = firstSegment(ctx.page);
    if (!TENANT_AGNOSTIC_PAGES[page]) {
      page = 'clusters';
    }
    return ctx.prefix + '/web/tenants/' + newTenant + '/' + page;
  }

  // normalizeTenantList accepts the /api/tenants response (API envelope or bare array) and returns
  // [{id, provider}]. Elements may be plain strings (the ID) or objects.
  function normalizeTenantList(response) {
    var list = response;
    if (list && !Array.isArray(list)) {
      list = list.Details;
    }
    if (list && !Array.isArray(list) && Array.isArray(list.Tenants)) {
      list = list.Tenants;
    }
    if (!Array.isArray(list)) {
      return [];
    }
    var result = [];
    list.forEach(function (t) {
      var id = (typeof t === 'string') ? t : (t.ID || t.Id || t.TenantId || t.tenantId || t.id);
      if (!id) {
        return;
      }
      var provider = (typeof t === 'string') ? '' : (t.Provider || t.provider || t.Engine || t.engine || '');
      result.push({id: String(id), provider: String(provider)});
    });
    return result;
  }

  // normalizeTenantHealth reads Details.Tenants[] of /api/health into {id: {healthy, message}}.
  function normalizeTenantHealth(health) {
    var result = {};
    var list = health && health.Details && health.Details.Tenants;
    if (!Array.isArray(list)) {
      return result;
    }
    list.forEach(function (t) {
      var id = t.ID || t.Id || t.TenantId || t.tenantId || t.id;
      if (!id) {
        return;
      }
      var healthy = t.Healthy;
      if (healthy === undefined) { healthy = t.healthy; }
      if (healthy === undefined) { healthy = t.IsHealthy; }
      result[id] = {
        healthy: healthy === true,
        message: String(t.Message || t.Error || t.message || t.error || '')
      };
    });
    return result;
  }

  return {
    parsePathname: parsePathname,
    currentTenant: currentTenant,
    isGlobalApiPath: isGlobalApiPath,
    scopeUrl: scopeUrl,
    rewriteUrl: rewriteUrl,
    tenantizeHref: tenantizeHref,
    switchTenantPath: switchTenantPath,
    normalizeTenantList: normalizeTenantList,
    normalizeTenantHealth: normalizeTenantHealth
  };
});
