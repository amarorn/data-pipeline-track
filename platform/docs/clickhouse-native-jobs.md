# Jobs nativos no ClickHouse (Silver e Gold)

A **ingestão Oracle → Bronze** não existe nativa no ClickHouse (não há conector Oracle). Ela continua sendo feita por Spark (ou outro ETL). Já as etapas **Bronze → Silver** e **Silver → Gold** podem rodar 100% dentro do ClickHouse via SQL.

## Opções para rodar Silver e Gold no ClickHouse

| Opção | Descrição | Quando usar |
|-------|-----------|-------------|
| **cron + clickhouse-client** | Script SQL executado por `clickhouse-client` em horário fixo | Controle total, fácil debug, qualquer versão |
| **Refreshable Materialized View** | MV com `REFRESH EVERY X MINUTE/HOUR/DAY` (ClickHouse 24.x+) | Agendamento e dependências entre views sem cron |
| **CREATE TASK** | Jobs agendados no servidor (experimental, 23.7+) | Tudo dentro do ClickHouse, menos uso de cron |

## 1. Cron + clickhouse-client

Fluxo típico:

1. Spark (ou outro) grava o snapshot do dia em `track_bronze.{tabela}_bronze_snapshot`.
2. Um cron dispara `clickhouse-client` com um SQL que:
   - lê do Bronze (filtro `_ref_date = today()`),
   - aplica normalização (trim, upper) e calcula `row_hash` (SHA256),
   - insere em `track_silver.{tabela}_silver_snapshot`.
3. Outro cron (após o Silver) roda o SQL de deltas:
   - compara Silver em `ref_date = today()` e `ref_date = today() - 1`,
   - classifica I/U/D e insere em `track_gold.{tabela}_gold_deltas`.

Exemplo de cron (ajuste caminhos e datas):

```bash
# Silver: após a carga Bronze (ex.: 04:00)
0 4 * * * clickhouse-client --host ... --user ... --password ... < /path/to/scripts/clickhouse_silver_gold.sql

# Gold: após o Silver (ex.: 06:00)
0 6 * * * clickhouse-client --host ... --user ... --password ... < /path/to/scripts/clickhouse_gold_deltas.sql
```

Ou use o script `scripts/run_clickhouse_silver_gold.sh`, que aceita parâmetros e chama o client.

## 2. Refreshable Materialized View (ClickHouse 24.x+)

A MV é recalculada periodicamente sobre a tabela de destino (substitui o conteúdo) ou em modo APPEND.

Exemplo conceitual Silver (uma tabela; adapte colunas e nomes):

```sql
CREATE MATERIALIZED VIEW track_silver.mv_sf2030_silver
REFRESH EVERY 1 DAY
TO track_silver.sf2030_silver_snapshot
AS
SELECT
    CODEMP,
    CODFIL,
    NUMNOTA,
    CODCLI,
    CODSEQ,
    trim(upper(COALESCE(col_str, ''))) AS col_str,
    _ref_date AS ref_date,
    SHA256(concatAssumeInjective(/* colunas ordenadas */)) AS row_hash,
    now() AS _silver_processing_timestamp
FROM track_bronze.sf2030_bronze_snapshot
WHERE _ref_date = today();
```

Para Gold (deltas), a lógica é um SELECT que compara duas datas e insere I/U/D; pode ser uma segunda MV com `REFRESH EVERY 1 DAY` e `DEPENDS ON` na MV do Silver, ou um job separado.

Ver documentação: [Refreshable materialized view](https://clickhouse.com/docs/en/materialized-view/refreshable-materialized-view).

## 3. CREATE TASK (experimental)

Permite agendar uma query no servidor (sintaxe tipo cron). Útil se não quiser depender de cron externo. Consulte a documentação da sua versão (23.7+).

## Resumo

- **Oracle → Bronze:** sempre externo (Spark ou outro ETL).
- **Bronze → Silver e Silver → Gold:** podem ser feitos no ClickHouse com SQL, seja por **cron + clickhouse-client**, **Refreshable MV** ou **CREATE TASK**.
- Os exemplos de SQL para uma tabela (Silver + Gold) estão em **`scripts/clickhouse_silver_gold.sql`** (exemplo SF2030). Para outras tabelas, repita o padrão ou use um gerador a partir de `apps/orchestrator/configs/snapshot_pipeline.yaml`.
- Script de execução: **`./scripts/run_clickhouse_silver_gold.sh`** (usa `.env` para CLICKHOUSE_*). Exemplo: `./scripts/run_clickhouse_silver_gold.sh scripts/clickhouse_silver_gold.sql`.

**Requisito:** ClickHouse 22.8+ para `FULL OUTER JOIN` no Gold. Em versões anteriores use dois `LEFT JOIN` + `UNION ALL` para simular o full outer.
