# syntax=docker/dockerfile:1

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingestor ./cmd/ingestor \
 && mkdir /data

# distroless: no shell, no package manager, runs as non-root (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingestor /usr/local/bin/ingestor
COPY --from=build --chown=65532:65532 /data /data

ENV LISTEN_ADDR=:8080 \
    ADMIN_ADDR=0.0.0.0:9090 \
    DATA_DIR=/data

# The spool must survive restarts: mount a volume here.
VOLUME /data
EXPOSE 8080

# SIGTERM starts the graceful shutdown, which takes up to SHUTDOWN_TIMEOUT
# (15s by default): give the container a longer stop grace period than that
# (docker stop -t 30, stop_grace_period: 30s, terminationGracePeriodSeconds: 30).
# A SIGKILL is still safe, only less tidy.
STOPSIGNAL SIGTERM

# distroless has no curl: the binary can probe itself.
HEALTHCHECK --interval=15s --timeout=4s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/ingestor", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/ingestor"]
