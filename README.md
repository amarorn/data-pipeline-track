# Track Data Platform

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/) [![PySpark](https://img.shields.io/badge/pyspark-3.5.0-orange.svg)](https://spark.apache.org/) [![Docker Compose](https://img.shields.io/badge/docker--compose-3.8-informational.svg)](https://docs.docker.com/compose/) ![Stack](https://img.shields.io/badge/pipeline-Oracle%20to%20ClickHouse-9cf) ![Status](https://img.shields.io/badge/status-Internal%20%2F%20WIP-lightgrey)

Plataforma de dados da Track para ingestão, modelagem e catálogo de dados com orquestração PySpark e entrega em ClickHouse Cloud. Inclui serviços auxiliares de metadados, notebooks para exploração e infraestrutura empacotada em Docker Compose.

## Visão geral
- Orquestração única para domínios e camadas (`bronze`, `silver`, `gold`) via `apps/orchestrator`.
- Pipelines de referência para ingestão Oracle ➜ ClickHouse (`domains/data-pipeline`).
- Catálogo, lineage e contratos versionados (`apps/metadata-service`, `platform/docs`).
- Ambiente local pronto com Docker Compose (orquestrador, metadata service, Spark e notebooks).
- Bibliotecas compartilhadas reutilizáveis (`platform/shared-libs/track_platform`).

## Arquitetura de dados (alto nível)
```
Oracle (fonte) -> [Bronze / S3 + ClickHouse] -> [Silver / modelagem] -> [Gold / métricas + MVs]
                                                 \-> [Metadata Service / catálogos e lineage]
```

## Componentes principais
- **Orchestrator** (`apps/orchestrator`): lê `configs/pipelines.yaml`, executa domínios/camadas e gera logs por execução.
- **Metadata Service** (`apps/metadata-service`): serviço FastAPI (porta 8000) para catálogo e lineage baseado em arquivos JSONL.
- **Domínio data-pipeline** (`domains/data-pipeline`): ingestão Oracle (`bronze/oracle_ingestion.py`), configs de tabelas (`configs/tables.yaml`) e DDL/policies de ClickHouse (`clickhouse/ddl`, `policies`).
- **Shared libs** (`platform/shared-libs/track_platform`): sessão Spark, coleta de métricas e utilitários.
- **Infra** (`platform/infra/docker` e `platform/infra/terraform`): imagens dos serviços e esqueleto de provisionamento.
- **CI utilitários** (`ci/scripts/validate_configs.py`, `ci/github-actions/*.yml`): validação de configs e pipelines de lint/teste.

## Estrutura do repositório
```
apps/
  orchestrator/           # Runner CLI para domínios e camadas
  metadata-service/       # API e serviços de catálogo/lineage
connectors/               # Clientes de dados (ex.: ClickHouse)
config/settings.py        # Variáveis e tipagem de ambiente
domains/
  data-pipeline/
    bronze/               # Pipelines de ingestão
    clickhouse/ddl/       # SQL de criação em ClickHouse
    configs/tables.yaml   # Configuração declarativa das tabelas
platform/
  shared-libs/            # track_platform (libs comuns)
  infra/docker/           # Dockerfiles dos serviços
  docs/                   # Arquitetura, SLAs e runbooks
docker-compose.yml
requirements*.txt
```

## Pré-requisitos
- Python 3.11+ e `pip`.
- Docker e Docker Compose (v3.8+).
- Java 17+ para rodar Spark localmente (containers já trazem).
- Acesso às fontes/destinos (Oracle, ClickHouse Cloud, S3) com credenciais válidas.

## Configuração inicial
1) **Variáveis de ambiente**  
   Copie o template e ajuste com credenciais reais:
   ```bash
   cp .env.example .env
   ```
   Campos principais:
   - `ORACLE_*`: host, porta, service, usuário/senha da origem.
   - `CLICKHOUSE_*`: endpoint, porta, usuário/senha e base de destino.
   - `S3_*` e `USE_S3_STORAGE`: habilitam escrita no data lake.
   - `SPARK_*`: paralelismo e memória.
   - `METADATA_API_PORT`: porta do serviço de metadados (default 8000).

2) **Dependências locais (opcional se usar apenas Docker)**  
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   pip install -e platform/shared-libs          # track_platform
   ```

3) **Validar configs de domínios**  
   ```bash
   python ci/scripts/validate_configs.py
   ```

## Executando com Docker Compose
1) Suba os serviços necessários (orquestrador, metadata, notebooks e cluster Spark de apoio):
   ```bash
   docker compose up -d orchestrator metadata-service jupyter spark-master spark-worker-1
   ```
2) Acompanhe os logs:
   ```bash
   docker compose logs -f orchestrator
   docker compose logs -f metadata-service
   ```
3) Endpoints úteis:
   - Orchestrator: execução automatizada conforme `apps/orchestrator/configs/pipelines.yaml`.
   - Metadata Service: `http://localhost:8000` (porta ajustável via `.env`).
   - Jupyter: `http://localhost:8888` (token desabilitado para uso local).

> Obs.: o `docker-compose.yml` inclui um par Spark master/worker para desenvolvimento e serviços auxiliares de ETL (etl_app/jupyter_spark) caso precise de um ambiente completo de testes.

## Executando localmente (sem containers)
1) Garanta que o Python enxergue os módulos:
   ```bash
   export PYTHONPATH=$PWD
   ```
2) Rode o orquestrador para um domínio/camada específicos ou para todas:
   ```bash
   # Todas as camadas configuradas
   python -m apps.orchestrator.runner --domain data-pipeline --layer all

   # Somente bronze
   python -m apps.orchestrator.runner --domain data-pipeline --layer bronze

   # Usando config customizada
   python -m apps.orchestrator.runner --config apps/orchestrator/configs/pipelines.yaml
   ```
3) Execute ingestões diretamente (útil para testes unitários):
   ```python
   from domains.data_pipeline.bronze.oracle_ingestion import OracleBronzeIngestion
   ing = OracleBronzeIngestion()
   ing.ingest_table(table="CUSTOMERS", schema="SALES", partition_column="ID", num_partitions=10)
   ```

## Domínio `data-pipeline`
- **Configuração declarativa**: `domains/data-pipeline/configs/tables.yaml` define origem, modo incremental, colunas de partição, SLAs e owners.
- **Ingestão Oracle ➜ Bronze**: `bronze/oracle_ingestion.py` cria sessão Spark, lê Oracle com paralelização e grava partições diárias.
- **Camadas subsequentes**: placeholders para `silver_transformations` e `gold_metrics` no YAML, mantendo padrão de camadas.
- **Modelagem ClickHouse**:
  - DDLs: `clickhouse/ddl/01_databases.sql`, `02_bronze_tables.sql`, `03_silver_tables.sql`, `04_gold_tables.sql`.
  - Controles: `clickhouse/policies` (retention/users) e `clickhouse/migrations` (passos de deploy).
- **SLA e contratos**: `platform/docs/contracts/data-pipeline-sla.md` documenta janelas e métricas de fresheness/qualidade.

## Metadata Service e catálogo
- Código base em `apps/metadata-service/service.py` com registros de tabelas e lineage em JSON/JSONL (diretório `platform/metadata`).
- Em containers, publicado via FastAPI (porta 8000); ajuste `METADATA_API_PORT` ou expanda a API conforme necessário.
- Geração de catálogo consolidado: `MetadataService.generate_catalog()` escreve `catalog.json` com snapshot das tabelas registradas.

## Conectores, configurações e utilitários
- **ClickHouse client**: `connectors/clickhouse_client.py` encapsula operações básicas de consulta e insert via `clickhouse-connect`.
- **Settings tipados**: `config/settings.py` carrega `.env` usando Pydantic (`OracleSettings`, `ClickHouseSettings`, `SparkSettings`, `AppSettings`).
- **Metrics e Spark**: `track_platform.monitoring.metrics.MetricsCollector` e `track_platform.spark.session_manager.SparkSessionManager` padronizam sessão e coleta de métricas.

## Qualidade, testes e lint
- Rodar validação de configs: `python ci/scripts/validate_configs.py`.
- Pipelines CI em `ci/github-actions/` (lint e testes) podem ser replicados localmente após instalar as dependências de `domains/data-pipeline/pyproject.toml` (`pip install -e domains/data-pipeline[dev]`).

## Documentação adicional
- Arquitetura: `platform/docs/architecture/data-flow.md`.
- Contratos/SLA: `platform/docs/contracts/data-pipeline-sla.md`.
- Runbooks: `platform/docs/runbooks/clickhouse-ddl-deployment.md`.

Sinta-se à vontade para adaptar as seções conforme os próximos domínios entrarem na plataforma.
## 🏗️ Arquitetura de Fluxo de Dados

## 🤝 Contribuição e Pull Requests
- Todos os PRs devem usar o template em `.github/pull_request_template.md`. Não remova seções obrigatórias.
- O workflow **Validate Pull Request Template** falha se qualquer seção obrigatória for removida ou renomeada.
- Proteja a branch principal exigindo a aprovação desse check para garantir que o template seja seguido antes do merge.
