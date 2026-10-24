#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
branch="${DEPLOY_BRANCH:-main}"
force=false
[ "${1:-}" = "--force" ] && force=true

say() { printf '\n== %s\n' "$1"; }

exec 9> .deploy.lock
flock -n 9 || { echo "Another deploy is running." >&2; exit 1; }

[ -f .env ] || { echo "No .env here. Run deploy/init-env.sh first." >&2; exit 1; }
setting() { sed -n "s/^$1=//p" .env | tail -n 1 | tr -d "\"'"; }
if [ -n "$(setting PORTAL_REGISTRY)" ]; then
  compose+=(-f docker-compose.registry.yml)
  network=$(setting REGISTRY_NETWORK)
  network=${network:-fivefamilies-registry}
  docker network inspect "$network" > /dev/null 2>&1 || { echo "PORTAL_REGISTRY is set but there is no network $network. Start FiveFamilies first." >&2; exit 1; }
  IFS=, read -ra nodes <<< "$(setting PORTAL_REGISTRY)"
  answered=false
  for node in "${nodes[@]}"; do
    if docker run --rm --network "$network" portalgun wget -q -T 3 -O /dev/null "$node/v1/status" > /dev/null 2>&1; then
      answered=true
      break
    fi
  done
  [ "$answered" = true ] || { echo "No FiveFamilies node answers on $network, and a dimension is only healthy once it has registered. Start FiveFamilies first." >&2; exit 1; }
fi

deployed() { printf '%s %s\n' "$(git rev-parse HEAD)" "$(sha256sum .env | cut -d ' ' -f 1)"; }

record() {
  echo "$1" > .deployed
  rm -f .deploying
}

states() {
  local ids
  mapfile -t ids < <("${compose[@]}" ps -aq)
  [ "${#ids[@]}" -gt 0 ] || return 1
  docker inspect -f '{{index .Config.Labels "com.docker.compose.oneoff"}} {{slice .Name 1}} {{.State.Status}}{{if .State.Health}}/{{.State.Health.Status}}{{end}} {{.State.StartedAt}}' "${ids[@]}" | grep '^False ' | cut -d ' ' -f 2- | sort
}

healthy() {
  local now=""
  for _ in $(seq 1 60); do
    now=$(states) && ! grep -Evq '^[^ ]+ running(/healthy)? ' <<< "$now" && return 0
    sleep 3
  done
  if [ -n "$now" ]; then
    grep -Ev '^[^ ]+ running(/healthy)? ' <<< "$now" | cut -d ' ' -f 1,2 >&2
  else
    echo "no containers to check" >&2
  fi
  return 1
}

settled() {
  local before after
  before=$(states) || return 1
  sleep 15
  after=$(states) || { echo "the containers could not be inspected" >&2; return 1; }
  [ "$before" = "$after" ] && return 0
  grep -vxF -f <(echo "$before") <<< "$after" | sed 's/ [^ ]*$/ did not stay up/' >&2
  return 1
}

release() {
  "${compose[@]}" build && "${compose[@]}" up -d --remove-orphans && healthy && settled
}

git fetch --quiet origin "$branch"
target=$(git rev-parse "origin/$branch")
last=$(cut -d ' ' -f 1 .deployed 2>/dev/null || true)
previous=${last:-$(git rev-parse HEAD)}

if [ "$(git rev-parse HEAD)" = "$target" ] && [ "$force" = false ] && [ ! -e .deploying ] && [ "$(deployed)" = "$(cat .deployed 2>/dev/null)" ]; then
  echo "Already at $(git log --oneline -1) with this .env. Use --force to rebuild anyway."
  exit 0
fi

dumped=false
if [ -n "$("${compose[@]}" ps -q postgres)" ]; then
  say "Backing up before the deploy"
  "${compose[@]}" run --rm backup once
  dumped=true
fi

touch .deploying
say "Deploying $(git log --oneline -1 "$target")"
git merge --ff-only --quiet "$target"
if ! bash deploy/init-env.sh || ! "${compose[@]}" config --quiet || ! stamp=$(deployed); then
  git reset --hard --quiet "$previous"
  say "The release's settings are not valid, back at $(git log --oneline -1)"
  exit 1
fi

say "Starting it and waiting for every container to pass its health check and stay up"
if release; then
  record "$stamp"
  docker image prune -f --filter "label=com.docker.compose.project=portalgun" > /dev/null
  say "Live: $(git log --oneline -1)"
  exit 0
fi

if [ "$previous" = "$target" ]; then
  say "Not healthy, and this commit is the one already deployed, so there is nothing to roll back to. Fix what the output above names and run this again."
  exit 1
fi
say "Not healthy, rolling back to $(git log --oneline -1 "$previous")"
git reset --hard --quiet "$previous"
stamp=$(deployed)
if release; then
  record "$stamp"
  if [ "$dumped" = true ]; then
    echo "Rolled back. Schema migrations from the failed release stay applied, and the dump taken above is in backups/db if one has to be undone."
  else
    echo "Rolled back. Schema migrations from the failed release stay applied, and there is no dump of before, since Postgres wasn't running when the deploy started."
  fi
else
  echo "Rollback is not healthy either. Check: ${compose[*]} ps" >&2
fi
exit 1
