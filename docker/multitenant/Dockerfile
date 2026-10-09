# Orchestrator, multi-tenant deployments (one process, several tenants, one Raft group).
#
# Build from the ROOT of the repository (the context must contain go.mod, vendor/ and resources/):
#
#   docker build -f docker/multitenant/Dockerfile -t orchestrator-mt:local .
#
# The build is offline-friendly: Go modules come from vendor/ (-mod=vendor), so the only network access
# is `apk add` of the C toolchain (go-sqlite3 needs cgo) and of the runtime tools. Override the base images
# with --build-arg GO_IMAGE=... / RUNTIME_IMAGE=... (both must be Alpine of the same release: the binary is
# linked against musl).
#
# Runtime contract:
#   /etc/orchestrator/orchestrator.conf.json   global configuration  (mount read-only)
#   /etc/orchestrator/tenants.json             tenants file          (mount read-only; TenantsConfigFile points to it)
#   /var/lib/orchestrator                      DATA VOLUME: Raft directory + SQLite backends. Never lose it.
#   3000/tcp HTTP + dashboard + API,  10008/tcp Raft (internal network only)
#   runs as uid/gid 10001 (orchestrator), PID 1 is the orchestrator binary (run it with `--init` / `init: true`
#   so that the processes spawned by recovery hooks are reaped).
ARG GO_IMAGE=golang:1.25-alpine3.23
ARG RUNTIME_IMAGE=alpine:3.23

# ---------------------------------------------------------------------------------------------- build
FROM ${GO_IMAGE} AS build
# hadolint ignore=DL3018
RUN apk add --no-cache build-base
WORKDIR /src
COPY go.mod go.sum RELEASE_VERSION ./
COPY vendor ./vendor
COPY go ./go
ARG VERSION=
ARG GIT_COMMIT=
RUN set -eu; \
    v="${VERSION:-$(cat RELEASE_VERSION)}"; \
    CGO_ENABLED=1 GOFLAGS=-mod=vendor go build -trimpath \
      -ldflags "-X main.AppVersion=${v} -X main.GitCommit=${GIT_COMMIT}" \
      -o /out/orchestrator ./go/cmd/orchestrator

# -------------------------------------------------------------------------------------------- runtime
FROM ${RUNTIME_IMAGE}
LABEL org.opencontainers.image.title="orchestrator (multi-tenant)" \
      org.opencontainers.image.source="https://github.com/proxysql/orchestrator"

# bash, curl and jq are what orchestrator-client and the HEALTHCHECK need
# hadolint ignore=DL3018
RUN set -eu; \
    apk add --no-cache bash curl jq ca-certificates tzdata; \
    addgroup -S -g 10001 orchestrator; \
    adduser -S -u 10001 -G orchestrator -h /var/lib/orchestrator -s /sbin/nologin orchestrator; \
    install -d -o orchestrator -g orchestrator -m 0750 /var/lib/orchestrator; \
    install -d -m 0755 /etc/orchestrator

COPY --from=build /out/orchestrator /usr/local/orchestrator/orchestrator
# The web UI and templates are read relative to the working directory (resources/, resources/public)
COPY resources /usr/local/orchestrator/resources
RUN ln -s /usr/local/orchestrator/resources/bin/orchestrator-client /usr/local/bin/orchestrator-client

WORKDIR /usr/local/orchestrator
USER orchestrator

# Raft state and the SQLite backends live here: it must be a volume (named volume or bind mount)
VOLUME ["/var/lib/orchestrator"]
EXPOSE 3000 10008

# Docker sends SIGTERM on `docker stop`; orchestrator audits the shutdown and exits. Give it time (stop_grace_period).
STOPSIGNAL SIGTERM

# /health/live only says the HTTP server answers. For a readiness gate use /health/ready or /api/health.
# If you enable HTTP authentication or TLS (UseSSL) point this at the right URL with ORC_HEALTH_URL.
ENV ORC_HEALTH_URL=http://127.0.0.1:3000/health/live
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD curl -fsS "$ORC_HEALTH_URL" >/dev/null || exit 1

ENTRYPOINT ["/usr/local/orchestrator/orchestrator"]
CMD ["-config", "/etc/orchestrator/orchestrator.conf.json", "http"]
