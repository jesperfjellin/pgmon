#!/usr/bin/env sh
set -eu

PGMON_DATA_DIR="${PGMON_DATA_DIR:-/var/lib/pgmon}"
UID_EXPECT=1000
GID_EXPECT=1000

echo "[entrypoint] ensuring ownership of $PGMON_DATA_DIR (uid=$UID_EXPECT gid=$GID_EXPECT)" >&2
if [ -d "$PGMON_DATA_DIR" ]; then
  # Only chown if current ownership differs to avoid expensive recursive change each start.
  OWNER_UID=$(stat -c %u "$PGMON_DATA_DIR" 2>/dev/null || echo 0)
  OWNER_GID=$(stat -c %g "$PGMON_DATA_DIR" 2>/dev/null || echo 0)
  if [ "$OWNER_UID" != "$UID_EXPECT" ] || [ "$OWNER_GID" != "$GID_EXPECT" ]; then
    chown pgmon:pgmon "$PGMON_DATA_DIR" || echo "[entrypoint] WARN: failed chown of $PGMON_DATA_DIR" >&2
  fi
else
  mkdir -p "$PGMON_DATA_DIR" && chown pgmon:pgmon "$PGMON_DATA_DIR"
fi

exec su -s /bin/sh -c "/usr/local/bin/pgmon" pgmon
