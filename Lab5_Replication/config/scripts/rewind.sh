#!/usr/bin/env bash
# Повертає колишнього лідера в кластер фоловером: rewind.sh <хост нового лідера> <назва слота>.
# Запускається в одноразовому контейнері на томі зупиненого вузла (див. run_exp4.ps1).
set -euo pipefail

SOURCE="$1"
SLOT="$2"

echo "[rewind] ${NODE_NAME}: control data before rewind"
gosu postgres pg_controldata "$PGDATA" | grep -E "cluster state|TimeLineID|checkpoint location" || true

# pg_rewind знаходить точку, де історії WAL двох вузлів розійшлися, і замінює на цьому вузлі
# всі сторінки, змінені після неї, сторінками з нового лідера. Транзакції, які старий лідер
# підтвердив, але не встиг передати, при цьому зникають.
gosu postgres pg_rewind \
    --target-pgdata="$PGDATA" \
    --source-server="host=${SOURCE} port=5432 user=${POSTGRES_USER} password=${POSTGRES_PASSWORD} dbname=postgres" \
    --progress

gosu postgres bash /opt/replication/set-upstream.sh "$SOURCE" "$SLOT"
gosu postgres touch "$PGDATA/standby.signal"

echo "[rewind] ${NODE_NAME}: done, next start will be a follower of ${SOURCE}"
