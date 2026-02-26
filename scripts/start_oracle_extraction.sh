#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

cd "$PROJECT_ROOT"

if [ ! -f "$ENV_FILE" ]; then
    echo "✗ Arquivo .env não encontrado em $ENV_FILE"
    echo "  Copie .env.example para .env e configure as variáveis necessárias"
    exit 1
fi

echo "=================================================================================="
echo "INICIANDO SERVIÇOS PARA EXTRAÇÃO ORACLE → CLICKHOUSE"
echo "=================================================================================="
echo ""

echo "1. Verificando Docker..."
if ! command -v docker &> /dev/null; then
    echo "✗ Docker não está instalado ou não está no PATH"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "✗ Docker não está rodando"
    exit 1
fi
echo "✓ Docker está rodando"
echo ""

echo "2. Iniciando túnel SSH para Oracle..."
if docker compose ps oracle-tunnel | grep -q "Up"; then
    echo "✓ Túnel SSH já está rodando"
else
    docker compose --profile tunnel up -d oracle-tunnel
    echo "✓ Túnel SSH iniciado"
    sleep 3
fi
echo ""

echo "3. Iniciando cluster Spark..."
docker compose up -d spark-master spark-worker-1
echo "✓ Spark cluster iniciado"
echo ""

echo "4. Aguardando serviços ficarem prontos..."
echo "   Aguardando Spark Master..."
timeout=60
elapsed=0
while [ $elapsed -lt $timeout ]; do
    if curl -s http://localhost:8081 > /dev/null 2>&1; then
        echo "✓ Spark Master está respondendo"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
    echo "   Aguardando... (${elapsed}s)"
done

if [ $elapsed -ge $timeout ]; then
    echo "⚠ Spark Master não respondeu em ${timeout}s"
    echo "   Verifique os logs: docker compose logs spark-master"
fi
echo ""

echo "5. Verificando túnel SSH..."
if lsof -Pi :1521 -sTCP:LISTEN -t >/dev/null 2>&1; then
    PID=$(lsof -ti :1521)
    echo "✓ Túnel SSH ativo na porta 1521 (PID: $PID)"
else
    echo "⚠ Túnel SSH não está ativo na porta 1521"
    echo "   Verifique os logs: docker compose logs oracle-tunnel"
    echo "   Ou execute manualmente: ./scripts/setup_oracle_tunnel.sh"
fi
echo ""

echo "6. Status dos serviços:"
echo ""
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" | grep -E "oracle-tunnel|spark-master|spark-worker"
echo ""

echo "=================================================================================="
echo "SERVIÇOS PRONTOS PARA EXTRAÇÃO"
echo "=================================================================================="
echo ""
echo "Próximos passos:"
echo ""
echo "1. Executar pipeline via notebook:"
echo "   docker compose up -d jupyter"
echo "   Acesse: http://localhost:8888"
echo "   Abra: notebooks/oracle_bronze_extraction.ipynb"
echo ""
echo "2. Executar pipeline via script:"
echo "   docker compose up -d jupyter"
echo "   docker exec -it track-jupyter python apps/orchestrator/oracle_to_bronze_pipeline.py"
echo ""
echo "3. Verificar logs:"
echo "   docker compose logs -f oracle-tunnel"
echo "   docker compose logs -f spark-master"
echo ""
echo "4. Parar serviços:"
echo "   docker compose down"
echo "   docker compose --profile tunnel down"
echo ""
echo "=================================================================================="
