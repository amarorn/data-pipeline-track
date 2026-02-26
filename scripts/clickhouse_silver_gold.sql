-- Silver + Gold no ClickHouse (exemplo SF2030)
-- Execucao: clickhouse-client --host ... --user ... --password ... < scripts/clickhouse_silver_gold.sql
-- Ou por tabela: definir ref_date e tabela; para outras tabelas, adaptar colunas e PKs.

-- Variaveis (substituir em runtime ou usar literal)
-- ref_date_current = today()
-- ref_date_previous = today() - 1

-- ========== SILVER: Bronze -> normalizacao + row_hash ==========
-- Idempotente: remove Silver do dia e recarrega a partir do Bronze.

ALTER TABLE track_silver.sf2030_silver_snapshot DELETE WHERE ref_date = today();

INSERT INTO track_silver.sf2030_silver_snapshot
SELECT
    CODEMP,
    CODFIL,
    NUMNOTA,
    CODCLI,
    CODSEQ,
    _ref_date AS ref_date,
    SHA256(
        concat(
            '|',
            coalesce(toString(CODEMP), ''),
            '|', coalesce(toString(CODFIL), ''),
            '|', coalesce(toString(NUMNOTA), ''),
            '|', coalesce(toString(CODCLI), ''),
            '|', coalesce(toString(CODSEQ), '')
        )
    ) AS row_hash,
    now() AS _silver_processing_timestamp
FROM track_bronze.sf2030_bronze_snapshot
WHERE _ref_date = today();

-- Nota: no pipeline Spark usamos todas as colunas de negocio no hash e na Silver.
-- Aqui o exemplo usa so as PKs; em producao inclua todas as colunas de negocio
-- (ordem alfabetica) no concat do SHA256 e na lista de colunas do SELECT.
-- Deduplicacao por PK: use argMax(col, _bronze_ingestion_timestamp) ... GROUP BY PK
-- se houver multiplas gravacoes no mesmo dia.

-- ========== GOLD: deltas (ref_date_current vs ref_date_previous) ==========

ALTER TABLE track_gold.sf2030_gold_deltas DELETE WHERE delta_date = today();

INSERT INTO track_gold.sf2030_gold_deltas
SELECT
    CODEMP,
    CODFIL,
    NUMNOTA,
    CODCLI,
    CODSEQ,
    operation,
    today() AS delta_date,
    now() AS delta_timestamp
FROM (
    SELECT
        coalesce(c.CODEMP, p.CODEMP) AS CODEMP,
        coalesce(c.CODFIL, p.CODFIL) AS CODFIL,
        coalesce(c.NUMNOTA, p.NUMNOTA) AS NUMNOTA,
        coalesce(c.CODCLI, p.CODCLI) AS CODCLI,
        coalesce(c.CODSEQ, p.CODSEQ) AS CODSEQ,
        multiIf(
            p.CODEMP IS NULL AND c.CODEMP IS NOT NULL, 'I',
            c.CODEMP IS NULL AND p.CODEMP IS NOT NULL, 'D',
            c.row_hash != p.row_hash, 'U',
            'N'
        ) AS operation
    FROM (
        SELECT CODEMP, CODFIL, NUMNOTA, CODCLI, CODSEQ, row_hash
        FROM track_silver.sf2030_silver_snapshot
        WHERE ref_date = today()
    ) AS c
    FULL OUTER JOIN (
        SELECT CODEMP, CODFIL, NUMNOTA, CODCLI, CODSEQ, row_hash
        FROM track_silver.sf2030_silver_snapshot
        WHERE ref_date = today() - 1
    ) AS p
    ON c.CODEMP = p.CODEMP AND c.CODFIL = p.CODFIL AND c.NUMNOTA = p.NUMNOTA AND c.CODCLI = p.CODCLI AND c.CODSEQ = p.CODSEQ
)
WHERE operation != 'N';

-- Ajuste: a subquery acima usa aliases c_/p_ e depois coalesce(c.CODEMP, p.CODEMP).
-- Em versoes sem FULL OUTER JOIN use dois SELECTs com LEFT JOIN + UNION ALL
-- (registros so em current = I, so em previous = D, em ambos com hash diferente = U).
