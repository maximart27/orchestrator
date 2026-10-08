package http

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/proxysql/golib/log"

	"github.com/proxysql/orchestrator/go/raft"
)

// raftReverseProxyMiddleware returns chi middleware that proxies requests to
// the raft leader when the current node is not the leader.
func raftReverseProxyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !orcraft.IsRaftEnabled() {
			next.ServeHTTP(w, r)
			return
		}
		if orcraft.IsLeader() {
			next.ServeHTTP(w, r)
			return
		}
		if orcraft.GetLeader() == "" {
			next.ServeHTTP(w, r)
			return
		}
		if orcraft.LeaderURI.IsThisLeaderURI() {
			next.ServeHTTP(w, r)
			return
		}
		u, err := url.Parse(orcraft.LeaderURI.Get())
		if err != nil {
			_ = log.Errore(err)
			next.ServeHTTP(w, r)
			return
		}
		transport, err := orcraft.GetRaftHttpTransport()
		if err != nil {
			_ = log.Errore(err)
			next.ServeHTTP(w, r)
			return
		}
		forwardToLeader(w, r, u, transport)
	})
}

// forwardToLeader proxies r to the leader as it came: path, query string and
// headers -- hence the tenant of the request, whether it is in the path, in the
// X-Orchestrator-Tenant header or in the tenant query parameter -- travel
// untouched. The tenant is resolved again by the leader.
func forwardToLeader(w http.ResponseWriter, r *http.Request, leader *url.URL, transport http.RoundTripper) {
	r.Header.Del("Accept-Encoding")
	proxy := httputil.NewSingleHostReverseProxy(leader)
	proxy.Transport = transport
	proxy.ServeHTTP(w, r)
}
