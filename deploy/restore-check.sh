#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

dump="${1:-$(find backups/db -name 'portal-*.dump' 2> /dev/null | sort | tail -n 1)}"
[ -n "$dump" ] && [ -f "$dump" ] || { echo "No dump found in backups/db." >&2; exit 1; }

name="portalgun-restore-check-$$"
cleanup() { docker rm -f "$name" > /dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" --network none --cpus 0.5 --memory 256m --tmpfs /var/lib/postgresql \
  -e POSTGRES_USER=portal -e POSTGRES_DB=portal -e POSTGRES_PASSWORD=check postgres:18-alpine > /dev/null
for _ in $(seq 1 60); do
  docker exec "$name" pg_isready -h 127.0.0.1 -U portal -q 2> /dev/null && break
  sleep 1
done

docker exec -i "$name" pg_restore --no-owner --no-privileges --exit-on-error -U portal -d portal < "$dump"

docker exec "$name" psql -U portal -d portal -At -F ' ' -c "
  select 'config_versions', count(*) from config_versions
  union all select 'rollouts', count(*) from rollouts
  union all select 'rollout_steps', count(*) from rollout_steps
  union all select 'chaos_events', count(*) from chaos_events
  union all select 'latest config', coalesce(max(version), 0) from config_versions"
echo "Restored $dump into a throwaway Postgres."
