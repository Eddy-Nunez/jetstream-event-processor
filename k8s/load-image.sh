#!/bin/sh
# load-image.sh — make a locally built image available to a local k8s cluster's
# container runtime, whichever runtime that is. A cluster does NOT see images
# just because Docker built them (OrbStack is the exception: its k3s shares
# the Docker daemon). The worker/ingest manifests use
# `imagePullPolicy: IfNotPresent`, so a loaded image is used as-is and
# registry images (nats, localstack) still pull normally.
#
# Usage:   ./k8s/load-image.sh [image]        (default eventworker:dev)
#
# Detects: OrbStack (no-op), kind (kind load), k3s.io (k3s ctr images import).
# Unknown runtime: prints manual instructions.
set -u
image=${1:-eventworker:dev}

if [ -d /opt/orbstack ] || kubectl cluster-info 2>/dev/null | grep -qi orbstack; then
  echo "OrbStack detected: its k3s shares the Docker daemon — '$image' is already visible."
  exit 0
fi

# kind detection must NOT rely on `kubectl cluster-info` output: a kind control-plane
# URL is https://127.0.0.1:<port> and never contains the word "kind", so that grep
# always misses and the script falls through to the manual instructions. kind always
# names its kubectl contexts kind-<cluster>, which is the reliable signal.
if command -v kind >/dev/null 2>&1 && kubectl config current-context 2>/dev/null | grep -q '^kind-'; then
  echo "kind detected (context $(kubectl config current-context)): kind load docker-image $image"
  exec kind load docker-image "$image"
fi

if command -v k3s >/dev/null 2>&1; then
  echo "k3s.io detected: docker save $image | k3s ctr images import"
  docker save "$image" | k3s ctr images import
  exit $?
fi

echo "Unknown runtime. Load '$image' onto the node manually, e.g.:"
echo "  # k3s.io:   docker save $image | k3s ctr images import"
echo "  # kind:     kind load docker-image $image"
echo "  # crictl:   docker save $image | crictl load"
echo "  # registry: docker tag $image ghcr.io/you/eventworker:dev && docker push ..."
echo "The manifests keep imagePullPolicy: IfNotPresent so a loaded local image wins."
exit 1