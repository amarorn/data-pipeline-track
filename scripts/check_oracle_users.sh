#!/bin/bash
# Script para verificar usuários Oracle no servidor
# Uso: ./check_oracle_users.sh [service_name] [username] [password]

SERVICE_NAME="${1:-dev.grupotracker.com.br}"
USERNAME="${2:-clickhouse}"
PASSWORD="${3:-qiU!E0oe}"

echo "======================================================================"
echo "VERIFICANDO USUÁRIOS ORACLE"
echo "======================================================================"
echo "Service: $SERVICE_NAME"
echo "User: $USERNAME"
echo ""

# Tenta conectar e executar queries
sqlplus -s "$USERNAME/$PASSWORD@$SERVICE_NAME" <<EOF
SET PAGESIZE 1000
SET LINESIZE 200

PROMPT ======================================================================
PROMPT 1. USUÁRIO ATUAL
PROMPT ======================================================================
SELECT USER as "Usuário Atual" FROM dual;
SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') as "Sessão" FROM dual;
SELECT SYS_CONTEXT('USERENV', 'AUTHENTICATION_METHOD') as "Método Auth" FROM dual;

PROMPT 
PROMPT ======================================================================
PROMPT 2. SCHEMAS ACESSÍVEIS
PROMPT ======================================================================
SELECT DISTINCT owner as "Schema", COUNT(*) as "Tabelas"
FROM all_tables
WHERE owner NOT IN ('SYS', 'SYSTEM', 'SYSAUX', 'XDB', 'CTXSYS', 'MDSYS')
GROUP BY owner
ORDER BY owner;

PROMPT 
PROMPT ======================================================================
PROMPT 3. PERMISSÕES DE SISTEMA
PROMPT ======================================================================
SELECT privilege as "Privilégio", admin_option as "Admin"
FROM user_sys_privs
ORDER BY privilege;

PROMPT 
PROMPT ======================================================================
PROMPT 4. ROLES ATRIBUÍDAS
PROMPT ======================================================================
SELECT granted_role as "Role", admin_option as "Admin", default_role as "Default"
FROM user_role_privs
ORDER BY granted_role;

PROMPT 
PROMPT ======================================================================
PROMPT 5. STATUS DA CONTA
PROMPT ======================================================================
SELECT username, account_status, created, default_tablespace
FROM all_users
WHERE username = USER;

EXIT;
EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================================================"
    echo "Consulta concluída com sucesso!"
    echo "======================================================================"
else
    echo ""
    echo "======================================================================"
    echo "ERRO: Falha na conexão ou autenticação"
    echo "======================================================================"
    echo "Verifique:"
    echo "  1. Service name: $SERVICE_NAME"
    echo "  2. Username: $USERNAME"
    echo "  3. Password: (verificar)"
    echo ""
    echo "Para testar outros usuários:"
    echo "  ./check_oracle_users.sh dev.grupotracker.com.br outro_usuario outra_senha"
fi
