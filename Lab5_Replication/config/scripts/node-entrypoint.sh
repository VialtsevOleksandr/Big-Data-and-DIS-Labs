#!/usr/bin/env bash
# Точка входу вузла конфігурації A.
#
# Том порожній і задано UPSTREAM_HOST  -> вузол клонується з UPSTREAM_HOST і стартує фоловером.
# Том порожній і UPSTREAM_HOST немає   -> офіційний entrypoint робить initdb, вузол стартує лідером.
# У томі вже є каталог даних           -> вузол просто стартує в тій ролі, яка записана в даних
#                                         (є standby.signal -> фоловер, немає -> лідер).
set -euo pipefail

if [ -n "${UPSTREAM_HOST:-}" ] && [ ! -s "$PGDATA/PG_VERSION" ]; then
    SLOT="${NODE_NAME}_slot"
    echo "[bootstrap] ${NODE_NAME}: empty data directory, cloning from ${UPSTREAM_HOST}"

    until pg_isready -h "$UPSTREAM_HOST" -p 5432 -q; do
        echo "[bootstrap] ${NODE_NAME}: waiting for ${UPSTREAM_HOST}"
        sleep 1
    done

    export PGPASSWORD="$REPL_PASSWORD"

    # Якщо від попереднього життя цього вузла лишився неактивний слот (наприклад, утрачений
    # через max_slot_wal_keep_size), прибираємо його: pg_basebackup створить новий.
    psql -h "$UPSTREAM_HOST" -p 5432 -U "$REPL_USER" -d postgres -v ON_ERROR_STOP=1 -Atc \
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '${SLOT}' AND NOT active"

    mkdir -p "$PGDATA"
    chown -R postgres:postgres "$(dirname "$PGDATA")"
    chmod 700 "$PGDATA"

    # --wal-method=stream : паралельно з копіюванням файлів друге з'єднання тягне WAL, який лідер
    #                       пише під час копіювання, тому копія узгоджена, хоч лідер і приймає записи.
    # --create-slot --slot: слот створюється ДО початку копіювання, тож лідер не видалить WAL,
    #                       потрібний фоловеру від точки старту копії й далі.
    # --checkpoint=fast   : не чекати планової контрольної точки, почати копію одразу.
    # Позицію старту (START WAL LOCATION) pg_basebackup кладе у файл backup_label усередині копії;
    # з неї фоловер починає відтворення, а далі продовжує потоком через той самий слот.
    gosu postgres pg_basebackup \
        --host="$UPSTREAM_HOST" --port=5432 --username="$REPL_USER" --no-password \
        --pgdata="$PGDATA" \
        --wal-method=stream \
        --create-slot --slot="$SLOT" \
        --checkpoint=fast \
        --progress --verbose

    echo "[bootstrap] ${NODE_NAME}: backup_label of the copy:"
    cat "$PGDATA/backup_label" || true

    # Звідки і через який слот продовжувати після відтворення копії.
    gosu postgres bash /opt/replication/set-upstream.sh "$UPSTREAM_HOST" "$SLOT"
    # standby.signal: стартувати в режимі фоловера (постійне відновлення з primary_conninfo).
    gosu postgres touch "$PGDATA/standby.signal"

    unset PGPASSWORD
    echo "[bootstrap] ${NODE_NAME}: clone finished, starting as a follower of ${UPSTREAM_HOST}"
fi

exec docker-entrypoint.sh "$@"
