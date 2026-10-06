#!/bin/sh
set -eu

BACKUPS=/backups
KEEP_DAYS="${KEEP_DAYS:-14}"
RUN_AT="${RUN_AT:-04:30}"

log() {
  printf '{"event":"backup","time":"%s","message":"%s"}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1"
}

run_once() {
  stamp=$(date -u +%Y-%m-%dT%H-%M-%SZ)
  target="$BACKUPS/db/portal-$stamp.dump"
  mkdir -p "$BACKUPS/db"
  if pg_dump --format=custom --no-owner --no-privileges --file="$target.partial" && pg_restore --list "$target.partial" > /dev/null; then
    mv "$target.partial" "$target"
    find "$BACKUPS/db" -name 'portal-*.dump' -mtime +"$KEEP_DAYS" -delete
    date -u +%Y-%m-%dT%H:%M:%SZ > "$BACKUPS/last-success"
    log "database $(du -h "$target" | cut -f1) $target"
  else
    rm -f "$target.partial"
    log "FAILED"
    return 1
  fi
}

seconds_until_next_run() {
  now=$(date -u +%s)
  next=$(date -u -d "$(date -u +%Y-%m-%d) $RUN_AT" +%s)
  [ "$next" -gt "$now" ] || next=$((next + 86400))
  echo $((next - now))
}

if [ "${1:-}" = "once" ]; then
  run_once
  exit
fi

log "scheduled daily at $RUN_AT UTC, keeping $KEEP_DAYS days"
while true; do
  sleep "$(seconds_until_next_run)"
  run_once || true
done
