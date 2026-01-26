# Pipeline de Extração Oracle → ClickHouse Bronze

## Descrição

Pipeline para extração de tabelas específicas do Oracle para a camada Bronze no ClickHouse, seguindo o modelo de snapshot diário.

## Tabelas Extraídas

- `ginf.depara_cliente`
- `ginf.BASE_CEP_COMPLETA`
- `ginf.TST_CONTRATOS_BI`
- `siga.SC5030`
- `siga.SC6030`
- `ginf.TST_HISTORICO_SOLICITACOES`
- `ginf.TST_SOLICIT_CADASTRADAS`
- `siga.SD2030`
- `siga.SF2030`
- `siga.ZTX030`
- `ginf.TST_CONTRATOS`

## Arquitetura

O pipeline extrai dados do Oracle e os armazena na tabela `bronze.snapshot_raw` do ClickHouse com a seguinte estrutura:

- `ref_date`: Data de referência do snapshot
- `table_name`: Nome completo da tabela (schema.tabela)
- `primary_key`: Chave primária concatenada
- `row_hash`: Hash SHA256 de todas as colunas
- `data`: Dados completos em formato JSON
- `ingestion_timestamp`: Timestamp da ingestão

## Execução

### Opção 1: Script Python

```bash
python apps/orchestrator/oracle_to_bronze_pipeline.py
```

### Opção 2: Notebook Jupyter

Abra e execute o notebook:
```
notebooks/oracle_bronze_extraction.ipynb
```

## Pré-requisitos

1. Conexão Oracle configurada no `.env`:
   - `ORACLE_HOST`
   - `ORACLE_PORT`
   - `ORACLE_SERVICE`
   - `ORACLE_USER`
   - `ORACLE_PASSWORD`

2. Conexão ClickHouse configurada no `.env`:
   - `CLICKHOUSE_HOST`
   - `CLICKHOUSE_PORT`
   - `CLICKHOUSE_USER`
   - `CLICKHOUSE_PASSWORD`
   - `CLICKHOUSE_DATABASE`

3. Spark configurado com:
   - Driver JDBC Oracle (`ojdbc8-21.9.0.0.jar`)
   - Driver JDBC ClickHouse (`clickhouse-jdbc-0.4.6-all.jar`)

4. Túnel SSH para Oracle (se necessário):
   ```bash
   ./scripts/setup_oracle_tunnel.sh
   ```

## Verificação dos Resultados

Após a execução, verifique os dados no ClickHouse:

```sql
SELECT 
    table_name,
    count(*) as total_rows,
    min(ingestion_timestamp) as first_ingestion,
    max(ingestion_timestamp) as last_ingestion
FROM bronze.snapshot_raw
WHERE ref_date = today()
GROUP BY table_name
ORDER BY table_name
```

## Configuração

As tabelas estão configuradas em:
- `domains/data-pipeline/configs/tables.yaml`

Para adicionar novas tabelas, edite o arquivo `tables.yaml` e atualize a lista `TABLES_TO_EXTRACT` no script ou notebook.
