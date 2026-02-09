#!/usr/bin/env bash
# Executa Silver + Gold no ClickHouse (SQL nativo).
# Uso: ./scripts/run_clickhouse_silver_gold.sh [arquivo.sql]
# Variaveis: CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD (ou .env)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SQL_FILE="${1:-$SCRIPT_DIR/clickhouse_silver_gold.sql}"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi

CH_HOST="${CLICKHOUSE_HOST:-localhost}"
CH_PORT="${CLICKHOUSE_PORT:-9000}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

if [ ! -f "$SQL_FILE" ]; then
  echo "Arquivo nao encontrado: $SQL_FILE"
  exit 1
fi

if [ -n "$CH_PASSWORD" ]; then
  clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --user "$CH_USER" --password "$CH_PASSWORD" --secure < "$SQL_FILE"
else
  clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --user "$CH_USER" --secure < "$SQL_FILE"
fi

echo "Concluido: $SQL_FILE"
