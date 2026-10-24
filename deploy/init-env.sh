#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077

secret() { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

touch .env
chmod 600 .env
[ -z "$(tail -c 1 .env)" ] || echo >> .env

add() {
  grep -q "^$1=" .env || printf '%s=%s\n' "$1" "$2" >> .env
}

add POSTGRES_PASSWORD "$(secret)"
add PORTAL_ADMIN_TOKEN "$(secret)"
add PORTAL_PROXY_TOKEN "$(secret)"
add EDGE_NETWORK edge
add BACKUP_UID "$(id -u)"
add BACKUP_GID "$(id -g)"
add TRAFFIC_RATE 200

mkdir -p backups/db
echo ".env has $(grep -c '=' .env) settings; secrets are not printed."
