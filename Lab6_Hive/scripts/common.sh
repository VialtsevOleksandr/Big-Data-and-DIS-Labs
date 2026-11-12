#!/usr/bin/env bash
# Спільні функції для запуску частин лабораторної всередині контейнера.

LAB=/lab
WH=/opt/hive/data/warehouse
BEELINE="/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000/"

# docker exec без TTY: beeline (jline) не може створити термінал і падає
# з "Unable to create a terminal". Примусово вмикаємо простий dumb-термінал.
export TERM=dumb
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Dorg.jline.terminal.type=dumb -Dorg.jline.terminal.dumb=true"

# Папка доказів: evidence/<частина>/<RUN_ID>.
init_run() {
  PART=$1
  RUN_ID=${RUN_ID:-$(date +%Y%m%d_%H%M%S)}
  OUT=$LAB/evidence/$PART/$RUN_ID
  mkdir -p "$OUT"
  echo "Докази: evidence/$PART/$RUN_ID"
}

# HiveServer2 після старту піднімається ще 1–2 хвилини.
wait_hive() {
  echo -n "Чекаю HiveServer2"
  for _ in $(seq 1 60); do
    if $BEELINE -e 'SELECT 1' >/dev/null 2>&1; then echo " — готовий"; return 0; fi
    echo -n "."; sleep 5
  done
  echo; echo "beeline не підключився до HiveServer2. Остання спроба:" >&2
  $BEELINE -e 'SELECT 1' 2>&1 | grep -v '^SLF4J' | tail -n 20 >&2
  return 1
}

# Виконати sql/<частина>/<name>.sql; повний вивід beeline — у <name>.log,
# у консоль — лише час і кількість прочитаних рядків.
# SQL подаємо через stdin, а не через -f: у beeline 4.2.1 без TTY режим -f
# читає файл через псевдотермінал і завершується, нічого не виконавши.
hive_sql() {
  local name=$1 log="$OUT/$1.log"
  echo ">>> $name"
  $BEELINE <"$LAB/sql/$PART/$name.sql" >"$log" 2>&1 || true
  # Через stdin beeline не зупиняється на помилці, тому перевіряємо лог:
  # помилок немає і хоча б один запит справді виконався.
  if grep -qE '^Error:|FAILED:' "$log" \
     || ! grep -qE 'rows? (selected|affected)' "$log"; then
    grep -v '^SLF4J' "$log" | tail -n 40; echo "ПОМИЛКА, лог: $log" >&2; return 1
  fi
  grep -E 'rows? (selected|affected)|RECORDS_IN_Map_1' "$log" || true
}

# Виконати shell-команду в контейнері й зберегти її разом із виводом.
step_sh() {
  local name=$1; shift
  echo ">>> $name"
  if ! { echo "\$ $*"; bash -c "$*"; } >"$OUT/$name.log" 2>&1; then
    cat "$OUT/$name.log"; return 1
  fi
  cat "$OUT/$name.log"
}
