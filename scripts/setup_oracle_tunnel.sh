#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)
fi

ORACLE_HOST="${ORACLE_HOST:-10.255.150.11}"
ORACLE_PORT="${ORACLE_PORT:-1521}"
SSH_GATEWAY="${SSH_GATEWAY:-}"
SSH_USER="${SSH_USER:-}"
SSH_KEY="${SSH_KEY:-}"

if [ -z "$SSH_GATEWAY" ]; then
    echo "⚠ Variáveis de ambiente não configuradas para túnel SSH"
    echo ""
    echo "Configure no .env:"
    echo "  SSH_GATEWAY=gateway.example.com  # Servidor intermediário (jump host)"
    echo "  SSH_USER=seu_usuario"
    echo "  SSH_KEY=/path/to/ssh/key (opcional)"
    echo ""
    echo "IMPORTANTE:"
    echo "  - SSH_GATEWAY deve ser um servidor intermediário, NÃO o IP do Oracle"
    echo "  - O gateway deve ter acesso ao Oracle em ${ORACLE_HOST}:${ORACLE_PORT}"
    echo ""
    echo "Ou execute manualmente:"
    echo "  ssh -L 1521:${ORACLE_HOST}:${ORACLE_PORT} -N usuario@gateway.example.com"
    exit 1
fi

if [ "$SSH_GATEWAY" = "$ORACLE_HOST" ]; then
    echo "⚠ ATENÇÃO: SSH_GATEWAY está configurado com o mesmo IP do Oracle"
    echo "  Isso geralmente está incorreto."
    echo "  SSH_GATEWAY deve ser um servidor intermediário (jump host/gateway)"
    echo "  que tem acesso ao Oracle em ${ORACLE_HOST}:${ORACLE_PORT}"
    echo ""
    read -p "Deseja continuar mesmo assim? (s/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        exit 1
    fi
fi

LOCAL_PORT="${LOCAL_PORT:-1521}"
REMOTE_HOST="${ORACLE_HOST}"
REMOTE_PORT="${ORACLE_PORT}"

echo "=================================================================================="
echo "CONFIGURANDO TÚNEL SSH PARA ORACLE"
echo "=================================================================================="
echo "Gateway SSH: ${SSH_USER}@${SSH_GATEWAY}"
echo "Oracle remoto: ${REMOTE_HOST}:${REMOTE_PORT}"
echo "Porta local: ${LOCAL_PORT}"
echo "=================================================================================="
echo ""

if [ -f "$SSH_KEY" ]; then
    SSH_OPTS="-i $SSH_KEY"
    echo "✓ Usando chave SSH: $SSH_KEY"
else
    SSH_OPTS=""
    echo "⚠ Usando autenticação padrão SSH"
fi

if lsof -Pi :${LOCAL_PORT} -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "⚠ Porta ${LOCAL_PORT} já está em uso"
    echo "  Verificando se é o túnel SSH..."
    PID=$(lsof -ti :${LOCAL_PORT})
    if ps -p $PID -o command= | grep -q "ssh.*${REMOTE_HOST}"; then
        echo "  ✓ Túnel SSH já está ativo (PID: $PID)"
        exit 0
    else
        echo "  ✗ Porta ocupada por outro processo (PID: $PID)"
        echo "  Execute: kill $PID ou use outra porta (LOCAL_PORT=1522)"
        exit 1
    fi
fi

echo "Iniciando túnel SSH..."
echo "Comando: ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -L ${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT} -N -f ${SSH_OPTS} ${SSH_USER}@${SSH_GATEWAY}"
echo ""

ssh -o StrictHostKeyChecking=no \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    -L ${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT} \
    -N -f \
    ${SSH_OPTS} \
    ${SSH_USER}@${SSH_GATEWAY}

if [ $? -eq 0 ]; then
    sleep 2
    if lsof -Pi :${LOCAL_PORT} -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        PID=$(lsof -ti :${LOCAL_PORT})
        echo "✓ Túnel SSH criado com sucesso (PID: $PID)"
        echo ""
        echo "Para parar o túnel:"
        echo "  kill $PID"
        echo "  ou"
        echo "  pkill -f 'ssh.*${REMOTE_HOST}'"
    else
        echo "✗ Falha ao criar túnel SSH"
        exit 1
    fi
else
    echo "✗ Erro ao criar túnel SSH"
    exit 1
fi

