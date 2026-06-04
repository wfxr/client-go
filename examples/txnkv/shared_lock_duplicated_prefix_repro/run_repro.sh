#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -z "${TIDB_X_DIR:-}" ]]; then
  echo "set TIDB_X_DIR to the fresh start-tidb-x data directory" >&2
  exit 1
fi

LOG_DIR="$TIDB_X_DIR"
shopt -s nullglob
log_files=("$LOG_DIR"/logs/tikv*.log)
shopt -u nullglob

if (( ${#log_files[@]} == 0 )); then
  echo "no tikv logs found under $LOG_DIR/logs" >&2
  exit 1
fi

first_log_match() {
  local pattern="$1"
  local files=()
  shopt -s nullglob
  files=("$LOG_DIR"/logs/tikv*.log)
  shopt -u nullglob
  rg -m 1 "$pattern" "${files[@]}" || true
}

has_log_match() {
  local pattern="$1"
  local files=()
  shopt -s nullglob
  files=("$LOG_DIR"/logs/tikv*.log)
  shopt -u nullglob
  rg -q -m 1 "$pattern" "${files[@]}"
}

echo "using cluster dir: $LOG_DIR"
export GOWORK=off

attempts="${REPRO_ATTEMPTS:-3}"
wait_seconds="${REPRO_WAIT_SECONDS:-40}"
seen_double_prefix=0
artifacts_dir="$(mktemp -d /tmp/shared-lock-duplicated-prefix-repro.XXXXXX)"
echo "attempt logs: $artifacts_dir"

for ((attempt = 1; attempt <= attempts; attempt++)); do
  table_log="$artifacts_dir/attempt-${attempt}-table-row.log"
  raw_log="$artifacts_dir/attempt-${attempt}-raw.log"

  echo
  echo "attempt $attempt/$attempts"
  go run . -mode warmup 2>&1 | tee "$table_log"

  go run . -mode repro > >(tee "$raw_log") 2>&1 &
  raw_pid=$!
  observed_out_of_order=0
  deadline=$((SECONDS + wait_seconds))

  while kill -0 "$raw_pid" 2>/dev/null; do
    if has_log_match 'TableError\(OutOfOrder'; then
      observed_out_of_order=1
      kill "$raw_pid" 2>/dev/null || true
      break
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done

  if kill -0 "$raw_pid" 2>/dev/null; then
    kill "$raw_pid" 2>/dev/null || true
  fi

  set +e
  wait "$raw_pid"
  raw_rc=$?
  set -e

  if (( observed_out_of_order != 0 )); then
    echo "observed OutOfOrder; terminated raw-txn phase early"
  fi
  if (( raw_rc != 0 )); then
    echo "raw-txn phase exited with status $raw_rc; continuing to inspect tikv log"
  fi

  prefix="$(sed -n 's/.* prefix=\([0-9a-f]*\)$/\1/p' "$raw_log" | tail -n 1)"
  if [[ -n "$prefix" ]] && grep -q "codec.EncodeKey(inner.Key)=${prefix}${prefix}" "$raw_log"; then
    seen_double_prefix=1
  fi

  echo
  echo "new relevant tikv log lines after attempt $attempt:"
  first_log_match 'TableError\(OutOfOrder'

  until has_log_match 'TableError\(OutOfOrder'; do
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep 1
  done

  if has_log_match 'TableError\(OutOfOrder'; then
    break
  fi
done

echo
echo "final relevant tikv log lines:"
first_log_match 'TableError\(OutOfOrder'

if [[ "$seen_double_prefix" != "1" ]]; then
  echo "missing duplicated-prefix evidence in raw-txn program output" >&2
  exit 1
fi

if ! has_log_match 'TableError\(OutOfOrder'; then
  echo "missing OutOfOrder evidence in tikv log after $attempts attempts" >&2
  exit 1
fi

echo
echo "reproduction succeeded"
