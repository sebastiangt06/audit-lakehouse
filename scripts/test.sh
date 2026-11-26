#!/usr/bin/env bash
# Runs `dotnet test` for src/dotnet inside the pinned SDK image (no local .NET SDK needed).
# If the compose SQL Server is running, the container joins the compose network and the
# atomicity test runs against it; otherwise that test is reported as SKIPPED (never as passed).
# Test runner: Microsoft.Testing.Platform (src/dotnet/global.json); VSTest is not supported by
# xUnit v3 4.x on the .NET 10 SDK.
# Usage: bash scripts/test.sh   (or: make test)
set -euo pipefail

# Git Bash on Windows rewrites /src-style arguments into C:/... paths; disable that.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."

SDK_IMAGE="mcr.microsoft.com/dotnet/sdk:10.0.404-noble" # same tag as src/dotnet/Dockerfile
NETWORK="audit-lakehouse_default"
NUGET_VOLUME="audit-lakehouse_nuget-cache"

# Host path of src/dotnet in a form Docker understands (C:/... on Git Bash, /... elsewhere).
SRC_DIR="$(cd src/dotnet && (pwd -W 2>/dev/null || pwd))"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

docker_args=(--rm
  --mount "type=bind,source=${SRC_DIR},target=/src,readonly"
  --mount "type=volume,source=${NUGET_VOLUME},target=/root/.nuget/packages"
  -e DOTNET_CLI_TELEMETRY_OPTOUT=1
  -e DOTNET_NOLOGO=1)

if [ -n "$(docker compose ps --status running -q sqlserver 2>/dev/null)" ]; then
  echo "sqlserver is running: atomicity test runs against ${CORE_DB:-MakersCore} on ${NETWORK}"
  docker_args+=(--network "$NETWORK"
    -e "CORE_TEST_CONNECTION=Server=sqlserver,1433;Database=${CORE_DB:-MakersCore};User Id=sa;Password=${SA_PASSWORD:?SA_PASSWORD missing, cp .env.example .env};Encrypt=True;TrustServerCertificate=True")
else
  echo "WARNING: sqlserver is not running (make up). The atomicity test will be SKIPPED." >&2
fi

# Sources are copied inside the container: bin/obj stay out of the host tree and the build does
# not pay for a slow Windows bind mount.
docker run "${docker_args[@]}" "$SDK_IMAGE" bash -c '
  set -euo pipefail
  cp -r /src /work
  cd /work
  dotnet test --solution Makers.Core.sln -c Release --output Detailed
'
