"""
Backlog / TODOs - Data Pipeline Track (Domínio: data-pipeline)

Objetivo (ordem):
1) Bronze v2 no ClickHouse (payload JSON) - append-only
2) Silver v2 no ClickHouse (row_hash + ReplacingMergeTree)
3) Silver tipada no ClickHouse (tabelas por domínio: customers, orders, ...)
4) Gold (métricas/views) + orquestração
"""

# =========================
# PRIORIDADE MÁXIMA (P0)
# =========================

# TODO: Aplicar/validar DDLs no ClickHouse para bronze.oracle_snapshot_v2 (MergeTree) e load_control.
# TODO: Implementar loader Bronze (parquet) -> ClickHouse bronze.oracle_snapshot_v2 com payload JSON.
# TODO: Definir regra do payload JSON: incluir somente colunas de negócio (recomendado) e manter metadados fora do payload.
# TODO: Implementar batching/chunked inserts no loader (evitar estouro de memória/timeout).
# TODO: Implementar controle de carga em bronze.load_control:
# TODO:   - status STARTED ao iniciar
# TODO:   - status SUCCESS + row_count ao concluir
# TODO:   - status FAILED + error_message ao falhar
# TODO: Definir idempotência do Bronze v2:
# TODO:   - bloquear rerun quando (reference_date, _source, _source_table) já estiver SUCCESS
# TODO:   - permitir "force" para reprocessamento controlado

# =========================
# PRIORIDADE ALTA (P1)
# =========================

# TODO: Implementar Silver v2 no ClickHouse:
# TODO:   - ler bronze.oracle_snapshot_v2
# TODO:   - gerar row_hash (SHA-256) a partir de payload normalizado/estável
# TODO:   - inserir em silver.oracle_snapshot_v2 (ReplacingMergeTree por _ingestion_timestamp)
# TODO: Validar que o hash NÃO inclui _ingestion_timestamp (senão muda sempre).
# TODO: Processar por reference_date (D-1) e por _source_table para evitar leituras gigantes.
# TODO: Criar/ajustar runner(s) para rodar: Bronze ingestion -> Load CH Bronze v2 -> Silver v2 CH.

# =========================
# PRIORIDADE MÉDIA (P2)
# =========================

# TODO: Alinhar o YAML (configs/tables.yaml) com a execução real:
# TODO:   - dirigir a execução por tabela/entidade (customers, orders, etc.)
# TODO:   - aplicar transformações declaradas (clean_strings, validate_schema, deduplicate por chave)
# TODO: Implementar checagens de qualidade do YAML:
# TODO:   - row_count_min
# TODO:   - freshness_hours
# TODO: Registrar métricas de execução (tempo, contagem lida/grava, status) por tabela.

# =========================
# SILVER TIPADA (P2 -> P1 dependendo do uso)
# =========================

# TODO: Criar pipeline de Silver tipada no ClickHouse a partir de silver.oracle_snapshot_v2:
# TODO:   - parse do payload JSON -> colunas tipadas
# TODO:   - aplicar limpeza/normalização
# TODO:   - dedupe por chave de negócio (ex.: customer_id, order_id)
# TODO: Implementar tabelas tipadas prioritárias:
# TODO:   - silver.sales_customers
# TODO:   - silver.sales_orders
# TODO: Validar particionamento por reference_date e estratégia de reprocessamento por dia.

# =========================
# GOLD / MÉTRICAS (P3)
# =========================

# TODO: Implementar camada Gold (hoje é placeholder):
# TODO:   - criar views/tabelas agregadas (ex.: daily_sales_summary)
# TODO:   - garantir dependência: Gold sempre lê da Silver tipada
# TODO: Integrar agenda do YAML para rodar as métricas.

# =========================
# ORQUESTRAÇÃO (P3)
# =========================

# TODO: Criar/ajustar DAG no Airflow:
# TODO:   - task: bronze_ingestion_oracle
# TODO:   - task: load_clickhouse_bronze_v2
# TODO:   - task: build_clickhouse_silver_v2
# TODO:   - task: build_clickhouse_silver_typed
# TODO:   - task: build_gold_metrics
# TODO: Configurar retries, timeouts, alertas e ordem correta das dependências.