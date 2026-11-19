#!/usr/bin/env bash
# Ролі та доступ. Офіційний entrypoint образу виконує цей файл один раз під час initdb.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
-- Роль для реплікації: REPLICATION дозволяє відкривати з'єднання протоколу реплікації,
-- створювати й видаляти слоти. Даних через звичайні запити вона не змінює.
CREATE ROLE ${REPL_USER} WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD}';
GRANT SELECT ON notes TO ${REPL_USER};

-- Роль тестового застосунку: власник таблиці, плюс pg_monitor, щоб читати pg_stat_replication.
CREATE ROLE ${APP_USER} WITH LOGIN PASSWORD '${APP_PASSWORD}';
GRANT pg_monitor TO ${APP_USER};
ALTER TABLE notes OWNER TO ${APP_USER};
SQL

# Образ уже додав до pg_hba.conf рядок "host all all all scram-sha-256", але ключове слово all
# у полі бази не охоплює з'єднання фізичної реплікації: для них потрібен окремий рядок.
# pg_hba.conf лежить у каталозі даних, тому pg_basebackup переносить його і на фоловери.
echo "host replication ${REPL_USER} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
