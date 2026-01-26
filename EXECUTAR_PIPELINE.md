# Como Executar o Pipeline de Extração Oracle → ClickHouse

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
