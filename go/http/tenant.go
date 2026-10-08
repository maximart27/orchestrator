/*
   Copyright 2026 Orchestrator Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/logic"
)

// Multi-tenant HTTP layer.
//
// One process serves N tenants through one HTTP server and one Raft group. Every
// tenant-scoped route exists twice: in its canonical form
//
//	{prefix}/api/tenants/{tenantId}/<route>        (also /web/tenants/... and /api/v2/tenants/...)
//
// and in the legacy form without the tenant prefix. The tenant of a request is
// resolved, in this order, from the path parameter {tenantId}, the header
// X-Orchestrator-Tenant, the query parameter "tenant", and finally from the
// implicit tenant: the tenant "default" if configured, or the only tenant. When none
// applies the request is rejected with 400; a tenant that does not exist is a 404.
//
// The resolved tenant travels in the request context (TenantFrom); there is no
// "current tenant" anywhere else. Authentication, ReadOnly and everything that is
// global to the process (Raft, health of the node, the leader checks, /metrics)
// does not depend on the tenant and has no tenant form.
const (
	// TenantHeader is the request header that names the tenant of a request.
	TenantHeader = "X-Orchestrator-Tenant"
	// TenantQueryParam is the query parameter that names the tenant of a request.
	TenantQueryParam = "tenant"

	tenantPathParam = "tenantId"
)

// serverManager is the TenantManager the handlers resolve tenants in, when none
// was given to the API/Web objects themselves (see SetTenantManager).
var serverManager atomic.Pointer[logic.TenantManager]

// SetTenantManager injects the TenantManager of the process into the HTTP layer
// (API, Web and the v2 routes registered with RegisterV2Routes). It may be called
// before or after the routes are registered, but before requests are served.
//
// Without it the handlers use logic.DefaultManager(), which is what the
// single-tenant legacy start-up installs (go/app calls logic.ContinuousDiscovery):
// that keeps the pre-multi-tenant initialization working unchanged.
func SetTenantManager(m *logic.TenantManager) {
	serverManager.Store(m)
}

// managerOf returns explicit when set, else the injected manager, else the
// legacy default manager.
func managerOf(explicit *logic.TenantManager) *logic.TenantManager {
	if explicit != nil {
		return explicit
	}
	if m := serverManager.Load(); m != nil {
		return m
	}
	return logic.DefaultManager()
}

type tenantContextKey struct{}

// tenantInfo is what the tenant middleware leaves in the request context.
type tenantInfo struct {
	tenant *logic.Tenant
	// tagObjects: the raw objects of the response carry the tenant ID (see
	// renderTenantJSON): the tenant was named explicitly or the process has more
	// than one tenant.
	tagObjects bool
}

// TenantFrom returns the tenant the request was resolved to by the tenant
// middleware, or nil if the request did not go through it (global routes).
func TenantFrom(r *http.Request) *logic.Tenant {
	if info, ok := r.Context().Value(tenantContextKey{}).(*tenantInfo); ok {
		return info.tenant
	}
	return nil
}

// tenantOf is TenantFrom for the tenant-scoped handlers. A handler invoked
// directly, without the middleware (unit tests, embedding), falls back to the
// implicit tenant, as the legacy routes do; if there is none it panics, which
// chi's Recoverer turns into a 500 -- it cannot happen through the router.
func tenantOf(r *http.Request) *logic.Tenant {
	if t := TenantFrom(r); t != nil {
		return t
	}
	if t := managerOf(nil).Default(); t != nil {
		return t
	}
	panic("http: tenant-scoped handler invoked without a resolved tenant")
}

// errorStyle selects how a failed tenant resolution is rendered.
type errorStyle int

const (
	styleV1 errorStyle = iota // APIResponse envelope (API v1 and web)
	styleV2                   // V2APIResponse envelope
)

// tenantIDs returns the IDs of the tenants of m.
func tenantIDs(m *logic.TenantManager) []string {
	tenants := m.All()
	ids := make([]string, 0, len(tenants))
	for _, t := range tenants {
		ids = append(ids, t.ID)
	}
	return ids
}

// resolveTenant resolves the tenant of r in m. explicit tells whether the
// request named the tenant itself. A non-zero status means the request must be
// rejected with that status and message.
func resolveTenant(m *logic.TenantManager, r *http.Request) (t *logic.Tenant, explicit bool, status int, message string) {
	id := chi.URLParam(r, tenantPathParam)
	if id == "" {
		id = r.Header.Get(TenantHeader)
	}
	if id == "" {
		id = r.URL.Query().Get(TenantQueryParam)
	}
	if id != "" {
		t, ok := m.Get(id)
		if !ok {
			return nil, true, http.StatusNotFound, fmt.Sprintf("unknown tenant %q; available tenants: %s", id, strings.Join(tenantIDs(m), ", "))
		}
		return t, true, 0, ""
	}
	if t := m.Default(); t != nil {
		return t, false, 0, ""
	}
	return nil, false, http.StatusBadRequest, fmt.Sprintf("tenantId required: the request names no tenant (path /tenants/{tenantId}/..., header %s or query parameter %s) and there is neither a %q tenant nor a single tenant; available tenants: %s",
		TenantHeader, TenantQueryParam, config.DefaultTenantID, strings.Join(tenantIDs(m), ", "))
}

// tenantMiddleware resolves the tenant of every request it wraps and leaves it
// in the request context. manager is evaluated per request, so that the manager
// may be installed after the routes are registered.
func tenantMiddleware(manager func() *logic.TenantManager, style errorStyle) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m := manager()
			t, explicit, status, message := resolveTenant(m, r)
			if status != 0 {
				renderTenantError(w, style, status, message, tenantIDs(m))
				return
			}
			w.Header().Set(TenantHeader, t.ID)
			info := &tenantInfo{tenant: t, tagObjects: explicit || len(m.All()) > 1}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantContextKey{}, info)))
		})
	}
}

func renderTenantError(w http.ResponseWriter, style errorStyle, status int, message string, tenants []string) {
	if style == styleV2 {
		code := "TENANT_REQUIRED"
		if status == http.StatusNotFound {
			code = "TENANT_NOT_FOUND"
		}
		respondError(w, status, code, message)
		return
	}
	renderJSON(w, status, &APIResponse{
		Code:    ERROR,
		Message: message,
		Details: map[string]interface{}{"Tenants": tenants},
	})
}

// respondTenant is Respond for tenant-scoped handlers: the envelope carries the
// tenant ID.
func respondTenant(w http.ResponseWriter, r *http.Request, apiResponse *APIResponse) {
	if t := TenantFrom(r); t != nil {
		apiResponse.TenantId = t.ID
	}
	Respond(w, apiResponse)
}

// renderTenantJSON is renderJSON for the handlers that return a raw object (an
// instance, a cluster, a recovery...) instead of an APIResponse envelope.
//
// The tenant is always announced in the X-Orchestrator-Tenant response header
// (set by the middleware). In addition, when the request named its tenant
// explicitly or the process has several tenants, a struct -- or every struct of a
// list -- gets an additive "TenantId" member, so that an object taken out of its
// response never loses the tenant it belongs to (the same host:port or cluster
// name may exist in several tenants). A legacy single-tenant request is
// byte-identical to what it was. Lists of scalars and maps cannot carry the
// member; they rely on the header.
func renderTenantJSON(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	if info, ok := r.Context().Value(tenantContextKey{}).(*tenantInfo); ok && info.tagObjects {
		data = tagTenant(data, info.tenant.ID)
	}
	renderJSON(w, status, data)
}

// tagTenant returns data with a "TenantId" member added to its objects, or data
// itself when it holds no struct.
func tagTenant(data interface{}, tenantID string) interface{} {
	v := reflect.ValueOf(data)
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return data
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return data
	}
	switch v.Kind() {
	case reflect.Struct:
		if tagged, ok := tagObject(v.Interface(), tenantID); ok {
			return tagged
		}
	case reflect.Slice, reflect.Array:
		elem := v.Type().Elem()
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		if elem.Kind() != reflect.Struct || v.Len() == 0 {
			return data
		}
		tagged := make([]json.RawMessage, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			item, ok := tagObject(v.Index(i).Interface(), tenantID)
			if !ok {
				return data
			}
			tagged = append(tagged, item)
		}
		return tagged
	}
	return data
}

// tagObject marshals obj and inserts "TenantId" as its first member. It reports
// false when obj does not marshal to a JSON object or already has that member.
func tagObject(obj interface{}, tenantID string) (json.RawMessage, bool) {
	t := reflect.TypeOf(obj)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		if _, has := t.FieldByName("TenantId"); has {
			return nil, false
		}
	}
	encoded, err := json.Marshal(obj)
	if err != nil || len(encoded) < 2 || encoded[0] != '{' {
		return nil, false
	}
	id, err := json.Marshal(tenantID)
	if err != nil {
		return nil, false
	}
	out := make([]byte, 0, len(encoded)+len(id)+14)
	out = append(out, `{"TenantId":`...)
	out = append(out, id...)
	if string(encoded) != "{}" {
		out = append(out, ',')
		out = append(out, encoded[1:]...)
	} else {
		out = append(out, '}')
	}
	return out, true
}

// TenantStatus is the health of one tenant as seen by this node: the outcome of
// the node heartbeat into the backend of the tenant.
type TenantStatus struct {
	ID       string
	Provider string
	// Default marks the tenant that answers the requests that name no tenant.
	Default bool
	Healthy bool
	// Message is the last heartbeat error of the tenant backend; empty when healthy.
	Message string
	// LastGood is the last successful heartbeat into the backend of the tenant.
	LastGood *time.Time `json:",omitempty"`
}

// tenantStatuses returns the status of every tenant of m, sorted by tenant ID.
// It reads the bookkeeping of the heartbeats: it does not touch any backend.
func tenantStatuses(m *logic.TenantManager) []TenantStatus {
	health := map[string]TenantStatus{}
	if control := m.Control(); control != nil {
		for _, h := range control.TenantsHealth() {
			s := TenantStatus{Healthy: h.Healthy}
			if h.Error != nil {
				s.Message = h.Error.Error()
			}
			if !h.LastGood.IsZero() {
				lastGood := h.LastGood
				s.LastGood = &lastGood
			}
			health[h.TenantID] = s
		}
	}
	implicit := m.Default()
	statuses := make([]TenantStatus, 0, len(m.All()))
	for _, t := range m.All() {
		s, known := health[t.ID]
		if !known {
			// no heartbeat bookkeeping for the tenant: nothing is known to fail
			s = TenantStatus{Healthy: true}
		}
		s.ID = t.ID
		s.Provider = t.Provider
		s.Default = implicit != nil && implicit.ID == t.ID
		statuses = append(statuses, s)
	}
	return statuses
}

// ListTenants answers GET /api/tenants: the tenants of the process, with their
// provider and health. It is a node-local, global route (no tenant, not proxied
// to the Raft leader: the health is that of this node).
func (this *HttpAPI) ListTenants(w http.ResponseWriter, r *http.Request) {
	statuses := tenantStatuses(managerOf(this.Manager))
	Respond(w, &APIResponse{Code: OK, Message: fmt.Sprintf("%d tenants", len(statuses)), Details: statuses})
}

// renderTenantHTML renders a page; a page of a tenant workspace gets the
// template variable tenantId.
func renderTenantHTML(w http.ResponseWriter, r *http.Request, name string, data map[string]interface{}) {
	if t := TenantFrom(r); t != nil {
		data["tenantId"] = t.ID
	}
	renderHTML(w, http.StatusOK, name, data)
}
