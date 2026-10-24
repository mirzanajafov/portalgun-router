#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
branch="${DEPLOY_BRANCH:-main}"
force=false
[ "${1:-}" = "--force" ] && force=true

say() { printf '\n== %s\n' "$1"; }

[ -f .env ] || { echo "No .env here. Run deploy/init-env.sh first." >&2; exit 1; }
setting() { sed -n "s/^$1=//p" .env | tail -n 1 | tr -d "\"'"; }
if [ -n "$(setting PORTAL_REGISTRY)" ]; then
  compose+=(-f docker-compose.registry.yml)
  network=$(setting REGISTRY_NETWORK)
  network=${network:-fivefamilies-registry}
  docker network inspect "$network" > /dev/null 2>&1 || { echo "PORTAL_REGISTRY is set but there is no network $network. Start FiveFamilies first." >&2; exit 1; }
fi

deployed() { printf '%s %s\n' "$(git rev-parse HEAD)" "$(sha256sum .env | cut -d ' ' -f 1)"; }

release() {
  "${compose[@]}" build && "${compose[@]}" up -d --remove-orphans --wait --wait-timeout 180
}

previous=$(git rev-parse HEAD)
git fetch --quiet origin "$branch"
target=$(git rev-parse "origin/$branch")

if [ "$previous" = "$target" ] && [ "$force" = false ] && [ "$(deployed)" = "$(cat .deployed 2>/dev/null)" ]; then
  echo "Already at $(git log --oneline -1) with this .env. Use --force to rebuild anyway."
  exit 0
fi

if [ -n "$("${compose[@]}" ps -q postgres)" ]; then
  say "Backing up before the deploy"
  "${compose[@]}" run --rm backup once
fi

say "Deploying $(git log --oneline -1 "$target")"
git merge --ff-only --quiet "$target"
if ! bash deploy/init-env.sh || ! "${compose[@]}" config --quiet; then
  git reset --hard --quiet "$previous"
  say "The release's settings are not valid, back at $(git log --oneline -1)"
  exit 1
fi
stamp=$(deployed)

say "Starting it and waiting for every container to run and pass its health check"
if release; then
  echo "$stamp" > .deployed
  docker image prune -f --filter "label=com.docker.compose.project=portalgun" > /dev/null
  say "Live: $(git log --oneline -1)"
  exit 0
fi

if [ "$previous" = "$target" ]; then
  say "Not healthy, and this commit is the one already deployed, so there is nothing to roll back to. Check .env and run this again."
  exit 1
fi
say "Not healthy, rolling back to $(git log --oneline -1 "$previous")"
git reset --hard --quiet "$previous"
if release; then
  echo "Rolled back. Schema migrations from the failed release stay applied, and the dump taken above is in backups/db if one has to be undone."
else
  echo "Rollback is not healthy either. Check: ${compose[*]} ps" >&2
fi
exit 1
