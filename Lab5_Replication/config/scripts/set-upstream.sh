#!/usr/bin/env bash
# Записує, за ким іде цей вузол: set-upstream.sh <хост лідера> <назва слота>.
# Файл node.conf підключається з postgresql.conf через include_if_exists.
# Параметри діють лише коли в каталозі даних є standby.signal; на лідері вони ігноруються.
# Обидва параметри перечитуються через pg_reload_conf(), перезапуск вузла не потрібен.
set -euo pipefail

UPSTREAM="$1"
SLOT="$2"

cat > /var/lib/postgresql/node.conf <<EOF
# Згенеровано set-upstream.sh для вузла ${NODE_NAME}
# Куди підключається walreceiver. application_name — ім'я, під яким лідер бачить цей фоловер
# у pg_stat_replication і за яким його шукає synchronous_standby_names.
primary_conninfo = 'host=${UPSTREAM} port=5432 user=${REPL_USER} password=${REPL_PASSWORD} application_name=${NODE_NAME}'
# Слот на лідері, який утримує WAL для цього фоловера.
primary_slot_name = '${SLOT}'
EOF

echo "[set-upstream] ${NODE_NAME} -> ${UPSTREAM} (slot ${SLOT})"
