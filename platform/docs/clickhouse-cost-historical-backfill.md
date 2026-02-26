# Dados históricos de custo — ClickHouse Cloud

Como obter dados de faturas passadas e popular as tabelas `observability.*` para ter histórico no dashboard e no Grafana.

---

## 1. Onde pegar os dados históricos

### Opção A — Console ClickHouse Cloud (Billing / Usage)

1. Acesse [https://clickhouse.cloud](https://clickhouse.cloud) e faça login.
2. Vá em **Organization** → **Billing** → **Usage** (ou **Invoices**).
3. Use a visão por dia ou por mês e anote os valores (total USD, storage, compute quando disponível).
4. Se houver **Export** ou **Download CSV**, exporte e use o script de backfill abaixo com o CSV.

### Opção B — Billing API (ClickHouse Cloud) — recomendado

O ClickHouse Cloud expõe uma **API REST de usage/cost** que você pode chamar direto para trazer o histórico sem export manual.

- **Endpoint:** `GET https://api.clickhouse.cloud/v1/organizations/{organizationId}/usageCost`
- **Parâmetros obrigatórios:** `from_date` e `to_date` (formato `YYYY-MM-DD`). O período máximo por request é **31 dias** (to_date até 30 dias após from_date).
- **Autenticação:** Basic Auth com **API key** (criada no console: **Organization** → **API Keys** → **Create key**). Use o key ID como usuário e o secret como senha.
- **Resposta:** JSON com `result.costs` (lista de registros por dia e por entidade: `date`, `totalCHC`, `metrics.storageCHC`, `metrics.computeCHC`, etc.) e `result.grandTotalCHC`. Valores em CHC (equivalente a USD).
- **Referência:** [Usage Cost API](https://clickhouse.com/docs/cloud/manage/api/usageCost-api-reference), [API Overview](https://clickhouse.com/docs/cloud/manage/api/api-overview). Limite: 10 requests a cada 10 segundos.

Para popular as tabelas via API use o script `scripts/fetch_clickhouse_cloud_usage_api.py` (ver secção 3b).

### Opção C — Fatura em PDF ou e-mail

Se você só tem a fatura em PDF ou e-mail:

- Se a fatura tiver **custo por dia**: transcreva (ou extraia) para um CSV com coluna de data e valor.
- Se a fatura for só **total mensal**: crie uma linha por dia do mês com `total_usd = total_mês / dias_do_mês` (e opcionalmente `storage_usd` / `compute_usd` se a fatura detalhar).

---

## 2. Formato do CSV para backfill

O script `scripts/backfill_clickhouse_cost_history.py` espera um CSV com **cabeçalho** e pelo menos:

| Coluna        | Obrigatório | Exemplo   | Observação                    |
|---------------|-------------|-----------|--------------------------------|
| cost_date     | Sim         | 2025-01-15 | Data (YYYY-MM-DD)             |
| total_usd     | Sim         | 0.52      | Custo total do dia em USD     |

Colunas opcionais (se faltar, o script usa 0 ou valor derivado):

- `storage_usd`, `compute_usd` — se a fatura separar storage e compute.
- `total_queries` — número de queries no dia (se tiver).
- `total_read_gb` — GB lidos (se tiver).

Exemplo mínimo (`cost_history.csv`):

```csv
cost_date,total_usd
2025-01-01,0.48
2025-01-02,0.51
2025-01-03,0.49
```

Exemplo completo:

```csv
cost_date,total_usd,storage_usd,compute_usd,total_queries,total_read_gb
2025-01-01,0.48,0.001,0.479,1200,5.2
2025-01-02,0.51,0.001,0.509,1500,6.1
```

---

## 3. Rodar o backfill

Requisitos: mesmo ambiente do notebook (ou `clickhouse-connect`, `pandas`), variáveis de conexão do ClickHouse e câmbio USD→BRL.

```bash
# Na raiz do repositório, com .env ou variáveis de ambiente do ClickHouse
export CH_HOST="seu-host.clickhouse.cloud"
export CH_PORT=8443
export CH_USER="default"
export CH_PASSWORD="sua-senha"
export USD_TO_BRL=5.22

python scripts/backfill_clickhouse_cost_history.py path/to/cost_history.csv
```

O script:

- Lê o CSV.
- Completa colunas faltantes (total_tables, total_rows, total_compressed_gb, storage_brl, compute_brl, total_brl, usd_to_brl_rate, etc.) com valores padrão ou estimados.
- Insere em `observability.cost_summary_daily` (sem apagar dados já existentes).

Depois de rodar, o dashboard (notebook ou Grafana) passará a mostrar o histórico populado.

### 3b. Backfill via API (ClickHouse Cloud Usage Cost)

Se quiser puxar o histórico direto da API em vez de CSV:

```bash
export CH_CLOUD_API_KEY="seu-key-id"
export CH_CLOUD_API_SECRET="seu-secret"
export CH_HOST="seu-host.clickhouse.cloud"
export CH_PORT=8443
export CH_USER="default"
export CH_PASSWORD="sua-senha"
export USD_TO_BRL=5.22

# Opcional: se não definir, o script obtém da API GET /v1/organizations
export CH_CLOUD_ORGANIZATION_ID="uuid-da-organizacao"

python scripts/fetch_clickhouse_cloud_usage_api.py --from 2025-01-01 --to 2025-02-09
```

O script faz várias chamadas de 31 dias, agrega custo por dia (total, storage, compute), e insere em `observability.cost_summary_daily`. Requer `requests` além de `clickhouse-connect` e `pandas`.

---

## 4. Tabelas que recebem histórico

| Tabela                         | Backfill com faturas? | Observação |
|--------------------------------|------------------------|------------|
| `observability.cost_summary_daily` | Sim                   | Resumo por dia (total_usd, storage_usd, compute_usd, etc.). É o principal para histórico de fatura. |
| `observability.cost_snapshot_daily` | Não (só a partir de hoje) | Custo por tabela por dia; depende de snapshot de volumetria. Faturas antigas não têm esse detalhe. |
| `observability.compute_cost_hourly` | Não (só a partir de hoje) | Compute por hora; vem do `system.query_log`. Não há como recuperar por fatura antiga. |

Conclusão: use o backfill apenas em **cost_summary_daily**. As outras tabelas continuam sendo preenchidas pelo notebook a partir de agora (snapshot diário e compute por hora).
