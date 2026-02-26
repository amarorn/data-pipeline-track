# Como Executar o Pipeline Oracle → ClickHouse

## Pipeline Snapshot + Hash + Delta (Ingestão + Tratamento + Deltas)

Fluxo completo alinhado aos notebooks testados no ClickHouse: **Oracle → Bronze → Silver → Gold** (track_bronze, track_silver, track_gold).

### Configuração

- **Tabelas e PKs:** `apps/orchestrator/configs/snapshot_pipeline.yaml`
- **Variáveis de ambiente:** `.env` com `ORACLE_*`, `CLICKHOUSE_*`, `SPARK_*`. Opcional: `REF_DATE=YYYY-MM-DD`.

### Execução por camada ou completo

Na raiz do projeto (ou com `PYTHONPATH` apontando para a raiz):

```bash
# Todas as camadas (Bronze + Silver + Gold)
python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer all

# Apenas ingestão (Oracle → Bronze)
python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer bronze

# Apenas Silver (normalização + hash)
python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer silver

# Apenas Gold (deltas D-1 vs D-2)
python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer gold
```

### Opções adicionais

```bash
# Data de referência (default: hoje ou REF_DATE)
python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer all --ref-date 2026-02-07

# Uma tabela (pipeline_name do YAML)
python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer all --table SF2030

# Config customizado
python apps/orchestrator/snapshot_hash_delta_pipeline.py --config /path/to/snapshot_pipeline.yaml --layer bronze
```

### Agendamento (cron)

Exemplo para rodar Bronze às 02:00, Silver às 04:00 e Gold às 06:00 (conforme `apps/orchestrator/configs/pipelines.yaml`):

```bash
0 2 * * * cd /path/to/track-data-platform && REF_DATE=$(date +\%Y-\%m-\%d) python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer bronze
0 4 * * * cd /path/to/track-data-platform && REF_DATE=$(date +\%Y-\%m-\%d) python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer silver
0 6 * * * cd /path/to/track-data-platform && REF_DATE=$(date +\%Y-\%m-\%d) python apps/orchestrator/snapshot_hash_delta_pipeline.py --layer gold
```

### Orquestrador (runner por camada)

Se o domínio for importável como `domains.data_pipeline` (pasta `domains/data_pipeline` com underscore), é possível usar o orquestrador central:

```bash
python apps/orchestrator/runner.py --domain data-pipeline --layer bronze
python apps/orchestrator/runner.py --domain data-pipeline --layer silver
python apps/orchestrator/runner.py --domain data-pipeline --layer gold
```

Caso a pasta seja `domains/data-pipeline` (com hífen), renomeie para `data_pipeline` ou ajuste o `PYTHONPATH` para que o import funcione.

---

## Opção 1: Via Notebook Jupyter (Recomendado)

O notebook `oracle_bronze_extraction.ipynb` está pronto para execução:

1. **Inicie o Jupyter:**
   ```bash
   docker compose up -d jupyter
   ```

2. **Acesse o Jupyter:**
   - Abra o navegador em: `http://localhost:8888`
   - Token está nos logs: `docker logs track-jupyter`

3. **Execute o notebook:**
   - Abra `notebooks/oracle_bronze_extraction.ipynb`
   - Execute as células sequencialmente

## Opção 2: Via Docker Exec

Execute o pipeline diretamente no container:

```bash
docker compose up -d jupyter
docker exec -it track-jupyter python apps/orchestrator/oracle_to_bronze_pipeline.py
```

## Opção 3: Corrigir PySpark Local (Alternativa)

Se preferir executar localmente, use Python 3.10 ou instale PySpark 3.5.1+:

```bash
pip install pyspark==3.5.1
```

Ou use um ambiente virtual com Python 3.10:
```bash
pyenv install 3.10.13
pyenv local 3.10.13
pip install -r requirements.txt
```

## Pré-requisitos

1. **Túnel SSH ativo:**
   ```bash
   ./scripts/setup_oracle_tunnel.sh
   # ou
   docker compose --profile tunnel up -d oracle-tunnel
   ```

2. **Verificar túnel:**
   ```bash
   lsof -i :1521
   ```

3. **Configuração no .env:**
   - `ORACLE_HOST=localhost` (quando túnel ativo)
   - `SSH_GATEWAY` e `SSH_USER` configurados

## Verificação

Após executar, verifique os dados no ClickHouse:

```python
from connectors.clickhouse_client import ClickHouseClient

client = ClickHouseClient()
result = client.execute_query_with_result("""
    SELECT table_name, count(*) as total
    FROM bronze.snapshot_raw
    WHERE ref_date = today()
    GROUP BY table_name
""")
print(result.result_rows)
```
