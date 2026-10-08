// Multi-tenant UI: tenant-aware links, tenant selector in the top bar, and the Tenants overview page.
// URL logic lives in tenant.js; requests go through appUrl() (orchestrator.js), which scopes them to the tenant.

function currentTenantId() {
  return OrchestratorTenant.currentTenant(window.location.pathname);
}

// Links and forms rendered by the server (templates) carry no tenant: add the one of the current page.
function tenantizeServerRenderedLinks(tenant) {
  $("a[href]").each(function() {
    var href = $(this).attr("href");
    var scoped = OrchestratorTenant.tenantizeHref(href, tenant);
    if (scoped !== href) {
      $(this).attr("href", scoped);
    }
  });
  $("form[action]").each(function() {
    var action = $(this).attr("action");
    var scoped = OrchestratorTenant.tenantizeHref(action, tenant);
    if (scoped !== action) {
      $(this).attr("action", scoped);
    }
  });
}

function tenantLabel(tenant) {
  return tenant.provider ? tenant.id + " (" + tenant.provider + ")" : tenant.id;
}

// Pages without a tenant (currently the tenants overview) must not expose controls
// whose destination or API calls require tenant context. The server deliberately
// rejects those legacy URLs when several tenants exist, so letting the user click
// them only leads to a JSON error page.
function setTenantRequiredNavigationEnabled(enabled) {
  var selector = [
    "[data-nav-page=cluster]",
    "[data-nav-page=audit]",
    "[data-nav-page=search]",
    "[data-nav-page=context]",
    "[data-nav-page=global-recoveries]",
    "[data-nav-page=problems]"
  ].join(",");
  $(selector).toggleClass("d-none", !enabled).attr("aria-hidden", enabled ? null : "true");
}

// The selector only appears when there is more than one tenant: single-tenant and legacy
// installations see no change in the top bar.
function displayTenantSelector(tenants, currentTenant) {
  // A selected tenant, or a legacy/single-tenant installation, has an
  // unambiguous context. With several tenants the controls stay hidden until
  // the operator explicitly chooses one.
  setTenantRequiredNavigationEnabled(Boolean(currentTenant) || tenants.length === 1);
  if (tenants.length <= 1) {
    return;
  }
  var select = $("#tenant-selector");
  if (!currentTenant) {
    select.append($("<option>").val("").text("tenant…").prop("selected", true));
  }
  tenants.forEach(function(tenant) {
    var option = $("<option>").val(tenant.id).text(tenantLabel(tenant));
    if (tenant.id === currentTenant) {
      option.prop("selected", true);
    }
    select.append(option);
  });
  select.on("change", function() {
    var chosen = $(this).val();
    if (chosen) {
      window.location.href = OrchestratorTenant.switchTenantPath(window.location.pathname, chosen);
    }
  });
  $("#tenant-selector-item").removeClass("d-none");
  // The overview link is only meaningful with several tenants, so it is added here rather than in the layout.
  $("[data-nav-page=home] .dropdown-menu").append(
    $("<li>").append($("<a>").addClass("dropdown-item").attr("href", appUrl("/web/tenants")).text("Tenants"))
  );
}

function loadTenantSelector(currentTenant) {
  $.get(appUrl("/api/tenants"), function(response) {
    displayTenantSelector(OrchestratorTenant.normalizeTenantList(response), currentTenant);
  }, "json").fail(function() {
    // Legacy/single-tenant server without the tenants endpoint: no selector.
    setTenantRequiredNavigationEnabled(true);
  });
}

function displayTenantsOverview(tenants, tenantHealth) {
  $("#tenants_loading").hide();
  if (tenants.length == 0) {
    $("#tenants_empty").show();
    return;
  }
  var tbody = $("#tenants_content tbody").empty();
  tenants.forEach(function(tenant) {
    var health = tenantHealth[tenant.id];
    var healthCell = $("<td>");
    if (!health) {
      healthCell.append($("<span>").addClass("badge text-bg-secondary").text("unknown"));
    } else if (health.healthy) {
      healthCell.append($("<span>").addClass("badge text-bg-success").text("healthy"));
    } else {
      healthCell.append($("<span>").addClass("badge text-bg-danger").text("unhealthy"));
      if (health.message) {
        healthCell.append(" ").append($("<small>").addClass("text-muted").text(health.message));
      }
    }
    var link = $("<a>").attr("href", appUrl("/web/tenants/" + encodeURIComponent(tenant.id) + "/clusters")).text(tenant.id);
    tbody.append(
      $("<tr>")
        .append($("<td>").append(link))
        .append($("<td>").text(tenant.provider))
        .append(healthCell)
    );
  });
  $("#tenants_content").show();
}

function initTenantsPage() {
  showLoader();
  $.get(appUrl("/api/tenants"), function(response) {
    var tenants = OrchestratorTenant.normalizeTenantList(response);
    // Health is best effort: the list is shown even when /api/health has no per-tenant detail.
    $.get(appUrl("/api/health"), function(health) {
      hideLoader();
      displayTenantsOverview(tenants, OrchestratorTenant.normalizeTenantHealth(health));
    }, "json").fail(function() {
      hideLoader();
      displayTenantsOverview(tenants, {});
    });
  }, "json").fail(function() {
    hideLoader();
    $("#tenants_loading").hide();
    $("#tenants_unavailable").show();
  });
}

$(document).ready(function() {
  var tenant = currentTenantId();
  // Hide tenant-dependent controls immediately on an unscoped page, avoiding
  // both a clickable flash and invalid requests while /api/tenants is loading.
  setTenantRequiredNavigationEnabled(!tenantSelectionRequired());
  if (tenant) {
    tenantizeServerRenderedLinks(tenant);
    document.title = tenant + " · " + document.title;
  }
  loadTenantSelector(tenant);
  if ($("#tenants_workspace").length) {
    initTenantsPage();
  }
});
