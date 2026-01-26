-- Query para visualizar código fonte da função Oracle
-- Execute esta query em qualquer cliente SQL conectado ao Oracle

-- Parâmetros (ajuste conforme necessário):
DEFINE schema_name = 'BISTAGE';
DEFINE function_name = 'F_CONSULTA_INST_BI';

-- Consulta do código fonte
SELECT 
    line AS "Linha",
    text AS "Código"
FROM all_source
WHERE owner = UPPER('&schema_name')
  AND name = UPPER('&function_name')
  AND type = 'FUNCTION'
ORDER BY line;

-- Listar todas as funções do schema (opcional)
-- SELECT DISTINCT 
--     name AS "Nome",
--     type AS "Tipo"
-- FROM all_source
-- WHERE owner = UPPER('&schema_name')
--   AND type IN ('FUNCTION', 'PROCEDURE')
-- ORDER BY name;
