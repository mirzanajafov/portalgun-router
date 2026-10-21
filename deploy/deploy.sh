#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
branch="${DEPLOY_BRANCH:-main}"
force=false
[ "${1:-}" = "--force" ] && force=true

say() { printf '\n== %s\n' "$1"; }

[ -f .env ] || { echo "No .env here. Run deploy/init-env.sh first." >&2; exit 1; }
if grep -q '^PORTAL_REGISTRY=.' .env; then
  compose+=(-f docker-compose.registry.yml)
  network=$(sed -n 's/^REGISTRY_NETWORK=//p' .env | tr -d "\"'")
  network=${network:-fivefamilies-registry}
  docker network inspect "$network" > /dev/null 2>&1 || { echo "PORTAL_REGISTRY is set but there is no network $network. Start FiveFamilies first." >&2; exit 1; }
fi

healthy() {
  for _ in $(seq 1 60); do
    local bad=0
    for service in controller gateway-1 gateway-2 web; do
      id=$("${compose[@]}" ps -q "$service")
      status=$(docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null || echo missing)
      [ "$status" = healthy ] || bad=1
    done
    [ "$bad" = 0 ] && return 0
    sleep 3
  done
  return 1
}

release() {
  "${compose[@]}" build
  "${compose[@]}" up -d --remove-orphans
}

previous=$(git rev-parse HEAD)
git fetch --quiet origin "$branch"
target=$(git rev-parse "origin/$branch")

if [ "$previous" = "$target" ] && [ "$force" = false ]; then
  echo "Already at $(git log --oneline -1). Use --force to rebuild anyway."
  exit 0
fi

if [ -n "$("${compose[@]}" ps -q postgres)" ]; then
  say "Backing up before the deploy"
  "${compose[@]}" run --rm backup once
fi

say "Deploying $(git log --oneline -1 "$target")"
git merge --ff-only --quiet "$target"
release

say "Waiting for the controller, both gateways and the web app"
if healthy; then
  docker image prune -f --filter "label=com.docker.compose.project=portalgun" > /dev/null
  say "Live: $(git log --oneline -1)"
  exit 0
fi

say "Not healthy, rolling back to $(git log --oneline -1 "$previous")"
git reset --hard --quiet "$previous"
release
if healthy; then
  echo "Rolled back. Schema migrations from the failed release stay applied;"
  echo "the dump taken above is in backups/db if one has to be undone."
else
  echo "Rollback is not healthy either. Check: ${compose[*]} logs controller gateway-1 web" >&2
fi
exit 1
