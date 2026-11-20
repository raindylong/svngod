# svngod container image.
#
# Multi-stage: the builder carries the Go toolchain, the final image carries the
# binaries. svngod is CGO-free by design (scripts/build.sh pins CGO_ENABLED=0),
# so the runtime layer needs no libc, no shell and no package manager.
#
# Build:
#   docker build -f deploy/docker/Dockerfile -t svngod:latest .
#   docker build -f deploy/docker/Dockerfile -t svngod:1.2.3 \
#     --build-arg VERSION=1.2.3 --build-arg COMMIT=$(git rev-parse --short=12 HEAD) .
#
# Run:
#   docker run -d --name svngod -p 8080:8080 \
#     -v /srv/svngod/repos:/var/lib/svngod/repos \
#     -v /srv/svngod/etc:/etc/svngod:ro \
#     svngod:latest
#
# The config file is mounted, not baked in: it holds backend credentials and
# differs per deployment.

# ---- build ----
FROM golang:1.26 AS build

WORKDIR /src

# Dependencies first, so the module cache layer survives source-only changes and
# an ordinary rebuild does not re-download everything.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=
ARG BUILD_DATE=

# Same shape as scripts/build.sh: -trimpath keeps the build path out of the
# binary, and the ldflags make --version name the release instead of a
# pseudo-version. BUILD_DATE is passed in rather than read from the clock so the
# same commit always stamps the same date.
RUN set -eu; \
    LDFLAGS="-s -w -X svngod/internal/version.Version=${VERSION}"; \
    if [ -n "${COMMIT}" ]; then LDFLAGS="${LDFLAGS} -X svngod/internal/version.Commit=${COMMIT}"; fi; \
    if [ -n "${BUILD_DATE}" ]; then LDFLAGS="${LDFLAGS} -X svngod/internal/version.BuildDate=${BUILD_DATE}"; fi; \
    for cmd in svngod svnadmin svngocli; do \
      CGO_ENABLED=0 go build -trimpath -ldflags "${LDFLAGS}" -o "/out/${cmd}" "./cmd/${cmd}"; \
    done

# ---- runtime ----
# distroless static: no shell, no libc, no package manager. Nothing to exec into,
# which is the point - there is no way to change this image at runtime.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/svngod /usr/local/bin/svngod
COPY --from=build /out/svnadmin /usr/local/bin/svnadmin
COPY --from=build /out/svngocli /usr/local/bin/svngocli

# temp-dir is set through TMPDIR because /tmp in a container is commonly a small
# tmpfs, and one large commit buffers up to 64 GiB: the same failure the systemd
# unit guards against. For the config-driven path, set "temp-dir" in svngod.json
# as well, since that field takes precedence over TMPDIR.
ENV TMPDIR=/var/lib/svngod/tmp

WORKDIR /var/lib/svngod
EXPOSE 8080

# No HEALTHCHECK here. svngod's real readiness signal is
# GET /_api/v1/health on the *management* listener, which is off unless
# admin-addr and a token are configured; a container-local probe therefore
# cannot be written that works for every deployment. Point the orchestrator's
# readiness probe at that endpoint when the admin listener is enabled, and at a
# TCP check otherwise. distroless has no shell, so any probe must be an
# orchestrator-side HTTP/TCP probe - not an exec probe.

# Exec form: svngod must be PID 1 to receive SIGTERM directly and drain in-flight
# requests (it waits up to 30s).
ENTRYPOINT ["/usr/local/bin/svngod"]
CMD ["--config", "/etc/svngod/svngod.json"]