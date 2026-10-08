/*
   Copyright 2024 Orchestrator Authors

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
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/proxysql/orchestrator/go/config"
	"github.com/proxysql/orchestrator/go/logic"
)

// V2APIResponse is the standard response envelope for API v2 endpoints.
type V2APIResponse struct {
	Status  string      `json:"status"`
	Data    interface{} `json:"data,omitempty"`
	Error   *V2APIError `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
	// TenantID is the tenant the response is about; set on the tenant-scoped endpoints.
	TenantID string `json:"tenantId,omitempty"`
}

// V2APIError represents an error in the API v2 response.
type V2APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// respondOK writes a successful JSON response with the given data.
func respondOK(w http.ResponseWriter, data interface{}) {
	renderJSON(w, http.StatusOK, &V2APIResponse{
		Status: "ok",
		Data:   data,
	})
}

// respondError writes an error JSON response with the given HTTP status, error code, and message.
func respondError(w http.ResponseWriter, status int, code string, message string) {
	renderJSON(w, status, &V2APIResponse{
		Status: "error",
		Error: &V2APIError{
			Code:    code,
			Message: message,
		},
	})
}

// respondOKFor is respondOK for the tenant-scoped endpoints: the envelope carries the tenant.
func respondOKFor(w http.ResponseWriter, r *http.Request, data interface{}) {
	resp := &V2APIResponse{Status: "ok", Data: data}
	if t := TenantFrom(r); t != nil {
		resp.TenantID = t.ID
	}
	renderJSON(w, http.StatusOK, resp)
}

// respondErrorFor is respondError for the tenant-scoped endpoints.
func respondErrorFor(w http.ResponseWriter, r *http.Request, status int, code string, message string) {
	resp := &V2APIResponse{Status: "error", Error: &V2APIError{Code: code, Message: message}}
	if t := TenantFrom(r); t != nil {
		resp.TenantID = t.ID
	}
	renderJSON(w, status, resp)
}

// respondNotFoundFor is respondNotFound for the tenant-scoped endpoints.
func respondNotFoundFor(w http.ResponseWriter, r *http.Request, message string) {
	respondErrorFor(w, r, http.StatusNotFound, "NOT_FOUND", message)
}

// respondNotFound writes a 404 JSON response with the given message.
func respondNotFound(w http.ResponseWriter, message string) {
	respondError(w, http.StatusNotFound, "NOT_FOUND", message)
}

// RegisterV2Routes mounts all v2 API routes under /api/v2 on the given router,
// resolving tenants in the manager given to SetTenantManager (or the legacy default).
func RegisterV2Routes(r chi.Router) {
	RegisterV2RoutesFor(r, nil)
}

// RegisterV2RoutesFor is RegisterV2Routes resolving tenants in m (nil: as RegisterV2Routes).
//
// The tenant-scoped endpoints exist in their legacy form, /api/v2/<route>, and as
// /api/v2/tenants/{tenantId}/<route>. /api/v2/status (the health of the node) and
// /api/v2/tenants (the tenants of the process) are global.
//
// Like the rest of v2, none of them goes through the Raft reverse proxy: a
// follower answers from its own backend (all v2 endpoints only read). This is the
// pre-existing behavior, kept.
func RegisterV2RoutesFor(r chi.Router, m *logic.TenantManager) {
	prefix := config.Config.URLPrefix
	tenantMW := tenantMiddleware(func() *logic.TenantManager { return managerOf(m) }, styleV2)
	tenantRoutes := func(r chi.Router) {
		r.Use(tenantMW)

		// Cluster endpoints
		r.Get("/clusters", V2Clusters)
		r.Get("/clusters/{name}", V2ClusterInfo)
		r.Get("/clusters/{name}/instances", V2ClusterInstances)
		r.Get("/clusters/{name}/topology", V2Topology)

		// Instance endpoints
		r.Get("/instances/{host}/{port}", V2Instance)
		r.Get("/instances/{host}/{port}/channels", V2InstanceChannels)

		// Recovery endpoints
		r.Get("/recoveries", V2Recoveries)
		r.Get("/recoveries/active", V2ActiveRecoveries)

		// ProxySQL endpoints
		r.Get("/proxysql/servers", V2ProxySQLServers)
	}
	r.Route(fmt.Sprintf("%s/api/v2", prefix), func(r chi.Router) {
		// Global endpoints
		r.Get("/status", v2Status(m))
		r.Get("/tenants", func(w http.ResponseWriter, r *http.Request) {
			respondOK(w, tenantStatuses(managerOf(m)))
		})

		r.Group(tenantRoutes)
		r.Route(fmt.Sprintf("/tenants/{%s}", tenantPathParam), tenantRoutes)
	})
}

// V2Clusters returns a list of all known clusters with metadata.
func V2Clusters(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	clustersInfo, err := tenant.Store.ReadClustersInfo("")
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "CLUSTER_LIST_ERROR", fmt.Sprintf("Failed to read clusters: %v", err))
		return
	}
	respondOKFor(w, r, clustersInfo)
}

// V2ClusterInfo returns detailed information about a specific cluster.
func V2ClusterInfo(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	clusterName := chi.URLParam(r, "name")
	clusterName, err := figureClusterName(tenant, clusterName)
	if err != nil {
		respondNotFoundFor(w, r, fmt.Sprintf("Cluster not found: %v", err))
		return
	}
	clusterInfo, err := tenant.Store.ReadClusterInfo(clusterName)
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "CLUSTER_INFO_ERROR", fmt.Sprintf("Failed to read cluster info: %v", err))
		return
	}
	respondOKFor(w, r, clusterInfo)
}

// V2ClusterInstances returns all instances belonging to a given cluster.
func V2ClusterInstances(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	clusterName := chi.URLParam(r, "name")
	clusterName, err := figureClusterName(tenant, clusterName)
	if err != nil {
		respondNotFoundFor(w, r, fmt.Sprintf("Cluster not found: %v", err))
		return
	}
	instances, err := tenant.Store.ReadClusterInstances(clusterName)
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "CLUSTER_INSTANCES_ERROR", fmt.Sprintf("Failed to read cluster instances: %v", err))
		return
	}
	respondOKFor(w, r, instances)
}

// V2Instance returns detailed information about a specific MySQL instance.
func V2Instance(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	host := chi.URLParam(r, "host")
	port := chi.URLParam(r, "port")

	instanceKey, err := tenant.Store.NewResolveInstanceKeyStrings(host, port)
	if err != nil {
		respondErrorFor(w, r, http.StatusBadRequest, "INVALID_INSTANCE", fmt.Sprintf("Invalid instance key: %v", err))
		return
	}
	instanceKey, err = tenant.Store.FigureInstanceKey(instanceKey, nil)
	if err != nil {
		respondErrorFor(w, r, http.StatusBadRequest, "INVALID_INSTANCE", fmt.Sprintf("Cannot resolve instance: %v", err))
		return
	}
	instance, found, err := tenant.Store.ReadInstance(instanceKey)
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "INSTANCE_READ_ERROR", fmt.Sprintf("Failed to read instance: %v", err))
		return
	}
	if !found {
		respondNotFoundFor(w, r, fmt.Sprintf("Instance not found: %s:%s", host, port))
		return
	}
	respondOKFor(w, r, instance)
}

// V2Topology returns the ASCII topology representation for a given cluster.
func V2Topology(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	clusterName := chi.URLParam(r, "name")
	clusterName, err := figureClusterName(tenant, clusterName)
	if err != nil {
		respondNotFoundFor(w, r, fmt.Sprintf("Cluster not found: %v", err))
		return
	}
	asciiOutput, err := tenant.Store.ASCIITopology(clusterName, "", false, false)
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "TOPOLOGY_ERROR", fmt.Sprintf("Failed to generate topology: %v", err))
		return
	}
	respondOKFor(w, r, map[string]interface{}{
		"clusterName": clusterName,
		"topology":    asciiOutput,
	})
}

// V2Recoveries returns recent recovery entries, optionally filtered by cluster.
func V2Recoveries(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	clusterName := r.URL.Query().Get("cluster")
	clusterAlias := r.URL.Query().Get("alias")
	page := 0
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil {
			page = p
		}
	}
	recoveries, err := tenant.ReadRecentRecoveries(clusterName, clusterAlias, false, page)
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "RECOVERIES_ERROR", fmt.Sprintf("Failed to read recoveries: %v", err))
		return
	}
	respondOKFor(w, r, recoveries)
}

// V2ActiveRecoveries returns currently active (in-progress) recoveries.
func V2ActiveRecoveries(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	recoveries, err := tenant.ReadActiveRecoveries()
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "ACTIVE_RECOVERIES_ERROR", fmt.Sprintf("Failed to read active recoveries: %v", err))
		return
	}
	respondOKFor(w, r, recoveries)
}

// V2Status returns the health of the orchestrator node: global, it names no tenant.
func V2Status(w http.ResponseWriter, r *http.Request) {
	v2Status(nil)(w, r)
}

func v2Status(m *logic.TenantManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		health, err := managerOf(m).Control().HealthTest()
		if err != nil {
			respondError(w, http.StatusInternalServerError, "UNHEALTHY", fmt.Sprintf("Application node is unhealthy: %v", err))
			return
		}
		respondOK(w, &healthDetails{HealthStatus: health, Tenants: tenantStatuses(managerOf(m))})
	}
}

// V2InstanceChannels returns the replication channels for a specific MySQL instance.
// For multi-source replicas, this returns all named replication channels and their status.
func V2InstanceChannels(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	host := chi.URLParam(r, "host")
	port := chi.URLParam(r, "port")

	instanceKey, err := tenant.Store.NewResolveInstanceKeyStrings(host, port)
	if err != nil {
		respondErrorFor(w, r, http.StatusBadRequest, "INVALID_INSTANCE", fmt.Sprintf("Invalid instance key: %v", err))
		return
	}
	instanceKey, err = tenant.Store.FigureInstanceKey(instanceKey, nil)
	if err != nil {
		respondErrorFor(w, r, http.StatusBadRequest, "INVALID_INSTANCE", fmt.Sprintf("Cannot resolve instance: %v", err))
		return
	}
	instance, found, err := tenant.Store.ReadInstance(instanceKey)
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "INSTANCE_READ_ERROR", fmt.Sprintf("Failed to read instance: %v", err))
		return
	}
	if !found {
		respondNotFoundFor(w, r, fmt.Sprintf("Instance not found: %s:%s", host, port))
		return
	}
	respondOKFor(w, r, instance.ReplicationChannels)
}

// V2ProxySQLServers returns all servers from ProxySQL's runtime_mysql_servers table.
func V2ProxySQLServers(w http.ResponseWriter, r *http.Request) {
	tenant := tenantOf(r)
	hook := tenant.ProxySQL
	if hook == nil || !hook.IsConfigured() {
		respondErrorFor(w, r, http.StatusServiceUnavailable, "PROXYSQL_NOT_CONFIGURED", "ProxySQL is not configured")
		return
	}
	servers, err := hook.GetClient().GetServers()
	if err != nil {
		respondErrorFor(w, r, http.StatusInternalServerError, "PROXYSQL_ERROR", fmt.Sprintf("Failed to query ProxySQL servers: %v", err))
		return
	}
	respondOKFor(w, r, servers)
}
