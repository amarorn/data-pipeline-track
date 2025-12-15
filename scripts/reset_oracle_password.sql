-- ============================================================================
-- Script para redefinir senha de usuário Oracle
-- Execute como SYS ou SYSTEM (usuário com privilégios DBA)
-- ============================================================================

-- Conecte como SYS ou SYSTEM primeiro:
-- sqlplus sys/senha_do_sys@dev.grupotracker.com.br as sysdba
-- ou
-- sqlplus system/senha_do_system@dev.grupotracker.com.br

-- 1. Verificar status dos usuários
SELECT username, account_status, created, default_tablespace
FROM dba_users
WHERE username IN ('C##AMARO_BE', 'C##FALCON', 'CLICKHOUSE')
ORDER BY username;

-- 2. Desbloquear usuário (se estiver bloqueado)
-- ALTER USER C##AMARO_BE ACCOUNT UNLOCK;

-- 3. Redefinir senha (MÍNIMO 9 CARACTERES!)
-- IMPORTANTE: A senha deve ter pelo menos 9 caracteres
-- O usuário C##AMARO_BE já existe (status: OPEN, criado em 12-DEC-25)
-- Use ALTER USER em vez de CREATE USER:
ALTER USER C##AMARO_BE IDENTIFIED BY "qi#U!E0oe123";
-- 
-- Exemplos válidos:
-- ALTER USER C##AMARO_BE IDENTIFIED BY "qi#U!E0oe123";  -- 12 caracteres
-- ALTER USER C##AMARO_BE IDENTIFIED BY "ClickHouse123!";  -- 15 caracteres

-- 4. Verificar se funcionou
-- SELECT username, account_status FROM dba_users WHERE username = 'C##AMARO_BE';

-- 5. Testar conexão com nova senha
-- CONNECT C##AMARO_BE/nova_senha_aqui@dev.grupotracker.com.br

-- ============================================================================
-- NOTA: Senhas não podem ser recuperadas, apenas redefinidas
-- ============================================================================
