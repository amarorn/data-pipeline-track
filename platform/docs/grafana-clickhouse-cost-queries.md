# Queries ClickHouse para Grafana — Custo e Observabilidade

Use o data source **ClickHouse** no Grafana (plugin oficial ou vertamedia-clickhouse-datasource). Todas as queries usam as tabelas do database `observability`.

## Dashboard pronto para importar

Arquivo: **`grafana-dashboard-clickhouse-cost.json`** (mesmo diretório).

1. No Grafana: **Dashboards** → **New** → **Import** → **Upload JSON file** (ou cole o conteúdo do JSON).
2. Selecione o **Data source** ClickHouse e importe.
3. O dashboard inclui: stats do dia (custo, storage, compute, queries), série de custo diário, custo mensal, barra por database, tabela top tabelas, compute por hora, tabela resumo diário.

### Moeda (USD / BRL)

No topo do dashboard há a variável **Moeda**: escolha **USD ($)** ou **BRL (R$)**. Todos os painéis (incluindo os KPIs Custo, Storage, Compute) usam as colunas já gravadas (`*_usd` ou `*_brl`) e devem atualizar ao mudar o dropdown. O título dos KPIs mostra a moeda ativa (ex.: "Custo (último dia) — USD ($)"). O plugin substitui `${moeda}` pelo texto exibido ("USD ($)" ou "BRL (R$)"). As queries usam `if('${moeda}' = 'BRL (R$)', coluna_brl, coluna_usd)` para retornar o valor correto. Se os valores não atualizarem, use o botão **Refresh** do dashboard. O dashboard faz auto-refresh a cada 5s.

O dashboard usa intervalos fixos (últimos 90 dias para custo diário, últimos 365 dias para mensal, últimos 14 dias para compute por hora). O seletor de período (time picker) está oculto.

Se o tipo do datasource for outro (ex.: `vertamedia-clickhouse-datasource`), após importar edite cada painel e altere o datasource para o seu.

### Erro "Empty query" (Code: 62)

1. **Selecione o data source** no dropdown do dashboard (canto superior). Se a variável "Data source" estiver vazia, o plugin pode enviar query vazia.
2. **Abra cada painel** → **Edit** → na aba **Query**, confira se o SQL aparece no editor. Se estiver vazio, cole de novo a query do `grafana-clickhouse-cost-queries.md` correspondente ao painel.
3. **Plugin Altinity/VertaMedia:** o painel usa `rawSql`; se o seu plugin usar outro campo (ex. só `query`), edite o painel e mude para "Code" / "Raw SQL" e cole a query.
4. **Plugin oficial Grafana (grafana-clickhouse-datasource):** use modo "SQL" ou "Code" no editor de query e garanta que a caixa de texto não esteja vazia.

---

## 1. Custo total por dia (Time series)

**Tipo de painel:** Time series  
**Eixo X:** data | **Eixo Y:** USD

```sql
SELECT
    toStartOfDay(cost_date) AS time,
    sum(total_usd) AS "Total USD",
    sum(storage_usd) AS "Storage USD",
    sum(compute_usd) AS "Compute USD"
FROM observability.cost_summary_daily
WHERE cost_date >= toDate($__timeFrom()) AND cost_date <= toDate($__timeTo())
GROUP BY cost_date
ORDER BY time
```

O plugin ClickHouse costuma substituir as variáveis por strings em formato ISO (ex.: `'2025-11-12T00:00:00Z'`). Use `toDate($__timeFrom())` e `toDate($__timeTo())` (sem `/1000`) para colunas do tipo Date.

Se o Grafana não passar `$__timeFrom`/`$__timeTo`, use intervalo fixo:

```sql
SELECT
    toStartOfDay(cost_date) AS time,
    total_usd AS "Total USD",
    storage_usd AS "Storage USD",
    compute_usd AS "Compute USD"
FROM observability.cost_summary_daily
WHERE cost_date >= today() - 90
ORDER BY cost_date
```

---

## 1b. Custo por dia + previsão curto prazo (Time series)

**Tipo de painel:** Time series com duas séries: realizado e estimativa dos próximos 14 dias (média dos últimos 7 dias).

**Query A — Realizado (refId A):**

```sql
SELECT
    toStartOfDay(cost_date) AS time,
    total_usd AS "Total USD"
FROM observability.cost_summary_daily
WHERE cost_date >= today() - 90
ORDER BY time
```

**Query B — Previsão (média 7 dias, próximos 14 dias) (refId B):**

```sql
SELECT
    toStartOfDay(today() + number) AS time,
    (SELECT avg(total_usd) FROM observability.cost_summary_daily WHERE cost_date >= today() - 7 AND cost_date < today()) AS "Previsão (média 7d)"
FROM numbers(14)
ORDER BY time
```

No painel, use as duas queries no mesmo Time series. A previsão aparece como linha a partir de amanhã. O dashboard importado não inclui a série Holt para não falhar se a tabela não existir. Para previsão Holt: crie a tabela `observability.cost_forecast_daily` e popule-a pelo notebook (célula do forecast), depois adicione no painel a query abaixo como terceira série (opcional):

```sql
SELECT
    toStartOfDay(forecast_date) AS time,
    point_estimate_usd AS "Previsão Holt"
FROM observability.cost_forecast_daily
WHERE forecast_date >= today() AND forecast_date <= today() + 30
ORDER BY time
```

---

## 2. Custo mensal (Time series ou Bar chart)

**Tipo de painel:** Time series ou Bar chart

```sql
SELECT
    toStartOfMonth(month) AS time,
    total_cost_usd AS "Total USD",
    total_storage_usd AS "Storage USD",
    total_compute_usd AS "Compute USD",
    total_queries AS "Queries"
FROM observability.v_cost_monthly
WHERE month >= toStartOfMonth(today() - 365)
ORDER BY time
```

---

## 3. Custo por database (Bar chart horizontal ou Table)

**Tipo de painel:** Bar chart (horizontal) ou Table

```sql
SELECT
    database AS "Database",
    sum(storage_usd) AS "Storage USD",
    sum(storage_brl) AS "Storage BRL",
    sum(total_tables) AS "Tabelas"
FROM observability.v_cost_by_database_daily
WHERE cost_date >= $__timeFrom() AND cost_date <= $__timeTo()
GROUP BY database
ORDER BY "Storage USD" DESC
```

---

## 4. Top tabelas por storage (Table ou Bar chart)

**Tipo de painel:** Table ou Bar chart

```sql
SELECT
    database AS "Database",
    table_name AS "Tabela",
    storage_usd_day AS "USD/dia",
    storage_usd_month AS "USD/mês",
    (compressed_bytes / 1024 / 1024 / 1024) AS "GB comprimido",
    total_rows AS "Linhas"
FROM observability.cost_snapshot_daily
WHERE snapshot_date = (SELECT max(snapshot_date) FROM observability.cost_snapshot_daily)
ORDER BY storage_usd_day DESC
LIMIT 50
```

---

## 5. Compute por hora (Time series)

**Tipo de painel:** Time series  
**Eixo X:** data+hora (um ponto por hora)

```sql
SELECT
    toDateTime(event_date) + toIntervalHour(event_hour) AS time,
    compute_usd_hour AS "USD/hora",
    total_queries AS "Queries",
    (total_read_bytes / 1024 / 1024 / 1024) AS "GB lidos"
FROM observability.compute_cost_hourly
WHERE event_date >= $__timeFrom()::date AND event_date <= $__timeTo()::date
ORDER BY time
```

Alternativa sem variáveis de tempo do Grafana:

```sql
SELECT
    toDateTime(event_date) + toIntervalHour(event_hour) AS time,
    compute_usd_hour AS "USD/hora",
    total_queries AS "Queries"
FROM observability.compute_cost_hourly
WHERE event_date >= today() - 30
ORDER BY time
```

---

## 6. Resumo do dia (Stat)

**Tipo de painel:** Stat — um número por métrica

```sql
SELECT total_usd AS value FROM observability.cost_summary_daily ORDER BY cost_date DESC LIMIT 1
```

Título do painel: "Custo hoje (USD)".

Outros stats úteis (uma query por painel Stat):

- Storage hoje: `SELECT storage_usd AS value FROM observability.cost_summary_daily ORDER BY cost_date DESC LIMIT 1`
- Compute hoje: `SELECT compute_usd AS value FROM observability.cost_summary_daily ORDER BY cost_date DESC LIMIT 1`
- Queries hoje: `SELECT total_queries AS value FROM observability.cost_summary_daily ORDER BY cost_date DESC LIMIT 1`
- Custo acumulado (periodo 05 a 05): soma do dia 5 ate hoje. Query: `WHERE cost_date >= if(toDayOfMonth(today()) >= 5, toStartOfMonth(today()) + toIntervalDay(4), toStartOfMonth(addMonths(today(), -1)) + toIntervalDay(4)) AND cost_date <= today()`
- Periodo anterior (05 a 05): dia 5 do mes n-2 ate dia 4 do mes n-1. Query: `WHERE cost_date >= toStartOfMonth(addMonths(today(), -2)) + toIntervalDay(4) AND cost_date <= toStartOfMonth(addMonths(today(), -1)) + toIntervalDay(3)`

---

## 8. Mes anterior vs Mes atual (Time series)

**Custo por query:** Use `system.query_log` com `ProfileEvents['OSCPUVirtualTimeMicroseconds']` para estimar custo em USD: `(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6 / 3600) * 0.2181`. O painel "Top queries por custo" mostra as queries ordenadas por esse valor.

---

Compara o custo acumulado por dia do mes anterior com o do mes atual. Usa a view `observability.v_cost_month_comparison`.

**Criar a view:** execute `scripts/observability_views.sql` no console ClickHouse ou via `clickhouse-client < scripts/observability_views.sql`. Requer `observability.cost_summary_daily` populada.

- Query: `SELECT toStartOfDay(toStartOfMonth(today()) + (dia - 1)) AS time, if('${moeda}' = 'BRL (R$)', mes_anterior_brl, mes_anterior_usd) AS "Mes anterior", if('${moeda}' = 'BRL (R$)', mes_atual_brl, mes_atual_usd) AS "Mes atual" FROM observability.v_cost_month_comparison ORDER BY dia`

---

## 7. Tabela resumo diário (Table)

**Tipo de painel:** Table

```sql
SELECT
    cost_date AS "Data",
    round(total_usd, 4) AS "Total USD",
    round(storage_usd, 4) AS "Storage USD",
    round(compute_usd, 4) AS "Compute USD",
    total_queries AS "Queries",
    round(total_read_gb, 2) AS "GB lidos"
FROM observability.cost_summary_daily
WHERE cost_date >= today() - 90
ORDER BY cost_date DESC
```

---

## Variáveis de dashboard (opcional)

- **Database:** lista de databases para filtrar  
  Query: `SELECT DISTINCT database FROM observability.v_cost_by_database_daily ORDER BY 1`  
  Nome da variável: `database`

- **Últimos N dias:** variável do tipo "Custom" com valores `7, 30, 90`.

Use `$database` nas queries onde fizer sentido, por exemplo:

```sql
SELECT cost_date AS time, storage_usd AS "Storage USD"
FROM observability.v_cost_by_database_daily
WHERE database = '$database' AND cost_date >= today() - $dias
ORDER BY time
```

---

## Configuração do data source ClickHouse no Grafana

1. URL: endpoint do seu serviço (ex.: `https://xxx.us-central1.gcp.clickhouse.cloud:8443`).
2. Access: Server (default).
3. Auth: Basic auth ou usuário/senha; TLS/SSL se HTTPS.
4. No plugin vertamedia-clickhouse-datasource, em "Query", use "SQL" e cole as queries acima. O formato de tempo esperado é coluna `time` (DateTime ou Date convertido para DateTime).
