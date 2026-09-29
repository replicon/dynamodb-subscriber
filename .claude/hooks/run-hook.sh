#!/usr/bin/env sh
# Selects python3 or python without the stdin-drain bug of "python3 ... || python ...".
# When python3 exits 2 (block), the || causes python to read already-consumed stdin
# and exit 0, silently bypassing the block. Using exec avoids this entirely.
SCRIPT="$1"
shift
if python3 --version >/dev/null 2>&1; then
    exec python3 "$SCRIPT" "$@"
elif python --version >/dev/null 2>&1; then
    exec python "$SCRIPT" "$@"
else
    echo "[graphify] ERROR: python3 or python not found on PATH." >&2
    exit 1
fi
