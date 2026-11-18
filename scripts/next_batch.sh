#!/usr/bin/env bash
# Применяет к MariaDB следующий пакет изменений (6 пакетов = одни сутки, по 4 часа каждый).
set -euo pipefail
cd "$(dirname "$0")/.."
source .env
cur=$(cat state/BATCH 2>/dev/null || echo 0)
next=$((cur + 1))
f="sources/mariadb/batches/batch_${next}.sql"
if [ ! -f "$f" ]; then
  echo "Все пакеты уже применены (текущий: ${cur}). Начать заново: make reset" >&2
  exit 1
fi
docker compose exec -T mariadb mariadb -uroot -p"${MARIADB_ROOT_PASSWORD}" < "$f"
echo "$next" > state/BATCH
echo "Применён пакет ${next} из 6: $(head -1 "$f" | sed 's/^-- //')"
