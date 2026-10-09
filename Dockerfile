# Multi-stage build for the single `eventworker:dev` image. Both binaries
# (cmd/ingest and cmd/worker) ship in ONE image; k8s picks the binary per
# container via command override, so one push/tag serves all deployments.
#
# syntax=docker/dockerfile:1

# ---- builder ----
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ingest ./cmd/ingest && \
    CGO_ENABLED=0 go build -o /out/worker ./cmd/worker

# ---- runtime ----
FROM alpine:3.20
COPY --from=builder /out/ingest /usr/local/bin/ingest
COPY --from=builder /out/worker /usr/local/bin/worker
# No ENTRYPOINT and no CMD: k8s manifests set command per container
# (/usr/local/bin/ingest or /usr/local/bin/worker).
