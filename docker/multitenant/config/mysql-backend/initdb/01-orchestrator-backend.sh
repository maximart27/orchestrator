#!/bin/bash
# Runs once, when the MariaDB data directory is created (docker-entrypoint-initdb.d).
# Creates the backend account of orchestrator from the SAME credentials file orchestrator reads
# (/run/secrets/orchestrator_backend: [client] user / password), so the secret exists in one place only.
# The account may create and own every database called orc_*: orchestrator creates them itself.
set -euo pipefail
f=/run/secrets/orchestrator_backend
user="$(sed -n 's/^[[:space:]]*user[[:space:]]*=[[:space:]]*//p' "$f" | head -1 | tr -d '"')"
pass="$(sed -n 's/^[[:space:]]*password[[:space:]]*=[[:space:]]*//p' "$f" | head -1 | tr -d '"')"
[ -n "$user" ] && [ -n "$pass" ] || { echo "no user/password in $f" >&2; exit 1; }
# the entrypoint exports MARIADB_ROOT_PASSWORD (read from MARIADB_ROOT_PASSWORD_FILE) to the init scripts
mariadb -uroot -p"${MARIADB_ROOT_PASSWORD:?}" <<SQL
CREATE USER IF NOT EXISTS '${user}'@'%' IDENTIFIED BY '${pass}';
GRANT ALL PRIVILEGES ON \`orc\\_%\`.* TO '${user}'@'%';
FLUSH PRIVILEGES;
SQL
