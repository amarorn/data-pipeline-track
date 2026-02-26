#!/bin/bash

LOCAL_PORT="${LOCAL_PORT:-1521}"

echo "Parando túnel SSH na porta ${LOCAL_PORT}..."

PID=$(lsof -ti :${LOCAL_PORT} 2>/dev/null)

if [ -z "$PID" ]; then
    echo "⚠ Nenhum processo encontrado na porta ${LOCAL_PORT}"
    exit 0
fi

if ps -p $PID -o command= | grep -q "ssh.*-L.*${LOCAL_PORT}"; then
    kill $PID
    echo "✓ Túnel SSH parado (PID: $PID)"
else
    echo "⚠ Processo na porta ${LOCAL_PORT} não é um túnel SSH (PID: $PID)"
    echo "  Não foi parado por segurança"
    exit 1
fi

