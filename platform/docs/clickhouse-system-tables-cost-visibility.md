# Tabelas system do ClickHouse para visibilidade de custo

Análise das tabelas em `system` que podem trazer informação adicional para o dashboard de custos e observabilidade.

## Situação atual

O pipeline de custos já utiliza:
- `system.parts` (via `system.tables`) para volumetria e storage
- `system.query_log` para compute (queries, read_bytes, ProfileEvents OSCPUVirtualTimeMicroseconds)
- API de usage cost do ClickHouse Cloud para backfill histórico

## Integração no pipeline

O notebook `clickhouse-cost-analysis.ipynb` (célula 7 e 8) cria e popula as tabelas agregadas em `observability`:

| Tabela observability | Fonte system | Responsável |
|---------------------|-------------|--------------|
| `query_metric_hourly` | query_metric_log | Pipeline (célula 8) |
| `part_log_daily` | part_log | Pipeline (requer part_log habilitado) |
| `blob_storage_daily` | blob_storage_log | Pipeline |

O Grafana usa `system.*` para janela recente (7 dias) ou `observability.*` para histórico persistido. Exemplo para pico de memória histórico:

```sql
SELECT toStartOfHour(toDateTime(event_date) + toIntervalHour(event_hour)) AS time,
       sum(total_peak_memory_gb) AS "Pico memoria GB", sum(total_queries) AS "Queries"
FROM observability.query_metric_hourly WHERE event_date >= today() - 30
GROUP BY event_date, event_hour ORDER BY time
```

---

## Tabelas system com potencial para custo/visibilidade

### 1. system.query_log (em uso)

Já utilizado no `compute_cost_hourly` e `cost_summary_daily`. Fornece:
- read_bytes, read_rows, memory_usage, query_duration_ms
- ProfileEvents['OSCPUVirtualTimeMicroseconds'] para estimativa de CPU
- databases[], tables[], columns[] para correlacionar queries com tabelas

### 2. system.query_metric_log (recomendado)

Métricas por query em intervalos (memory_usage, peak_memory_usage). Permite:
- Identificar queries que consomem mais RAM (pico de memória)
- Correlacionar memory_usage com custo de compute

**Query de exemplo:**
```sql
SELECT
    toDate(event_time) AS event_date,
    toHour(event_time) AS event_hour,
    count() AS queries,
    sum(peak_memory_usage) / 1024 / 1024 / 1024 AS total_peak_memory_gb,
    avg(memory_usage) / 1024 / 1024 AS avg_memory_mb
FROM system.query_metric_log
WHERE event_date >= today() - 7
GROUP BY event_date, event_hour
ORDER BY event_date DESC, event_hour DESC
```

### 3. system.part_log (recomendado)

Operações em parts (NewPart, MergeParts, MutatePart). Permite:
- Volume de merges por tabela (read_bytes, read_rows durante merge)
- Estimativa de workload de background (storage/compactação)
- Identificar tabelas com mais atividade de merge

**Requer:** `part_log` habilitado no servidor. A tabela é criada só se o setting estiver em uso. Em ClickHouse Cloud, verificar se o serviço já expõe a tabela; se não, solicitar via support ou configuração do serviço. Referência: [part_log](https://clickhouse.com/docs/operations/system-tables/part_log).

**Query de exemplo:**
```sql
SELECT
    toDate(event_time) AS event_date,
    database,
    table,
    event_type,
    count() AS events,
    sum(size_in_bytes) / 1024 / 1024 / 1024 AS total_size_gb,
    sum(read_bytes) / 1024 / 1024 / 1024 AS read_during_merge_gb
FROM system.part_log
WHERE event_date >= today() - 7
GROUP BY event_date, database, table, event_type
ORDER BY read_during_merge_gb DESC
```

### 4. system.blob_storage_log (recomendado para Cloud)

Operações de blob storage (Upload, Delete, MultiPartUpload*). Permite:
- Volume de dados enviados para storage remoto
- Correlação com custo de storage e transferência

**Query de exemplo:**
```sql
SELECT
    event_date,
    event_type,
    count() AS operations,
    sum(data_size) / 1024 / 1024 / 1024 AS total_data_gb
FROM system.blob_storage_log
WHERE event_date >= today() - 7
GROUP BY event_date, event_type
ORDER BY event_date DESC, total_data_gb DESC
```

### 5. system.query_thread_log (opcional)

Detalhes por thread de execução. Complementar ao query_log para análise de concorrência e threads.

### 6. system.clickpipes_log (se existir)

Pipelines de ingestão ClickPipes. No ClickHouse Cloud, ClickPipes é uma dimensão de custo separada. Se a tabela existir, pode trazer visibilidade de volume ingerido por pipeline.

### 7. system.asynchronous_insert_log (opcional)

Inserts assíncronos. Útil para entender carga de ingestão e otimização.

### 8. system.error_log (visibilidade operacional)

Erros e exceções. Não impacta custo diretamente, mas ajuda a correlacionar falhas com picos de uso.

---

## Painéis sugeridos para o Grafana

| Painel | Fonte | Valor |
|-------|-------|-------|
| Pico de memória por hora | query_metric_log | Identificar horários de alto consumo de RAM |
| Merges por tabela | part_log | Tabelas com mais atividade de background |
| Operações de blob storage | blob_storage_log | Upload/delete por dia |
| Top queries por read_bytes | query_log | Já possível; adicionar filtro por database/tabela |

---

## Resumo de prioridade

| Prioridade | Tabela | Motivo |
|------------|--------|--------|
| Alta | part_log | Workload de merges correlaciona com compute e storage |
| Alta | query_metric_log | Pico de memória complementa estimativa de compute |
| Média | blob_storage_log | Operações de storage no Cloud |
| Baixa | query_thread_log | Complementar |
| Verificar | clickpipes_log | Dimensão ClickPipes no Cloud |

---

## Nota sobre ClickHouse Cloud

Em ClickHouse Cloud, algumas tabelas `system` são locais a cada nó. Para visão completa, use `clusterAllReplicas`:

```sql
SELECT * FROM clusterAllReplicas('default', system.query_log)
WHERE event_date = today()
SETTINGS skip_unavailable_shards = 1;
```

---

## Resumo de responsabilidades

| Ator | Ação |
|------|------|
| Engenheiro de dados | Rodar notebook `clickhouse-cost-analysis.ipynb` diariamente (células 7 e 8) |
| ClickHouse Cloud | Configuração de part_log (se não habilitado por padrão) |
| Grafana | Dashboard configurado com queries em `system.*` ou `observability.*` |
