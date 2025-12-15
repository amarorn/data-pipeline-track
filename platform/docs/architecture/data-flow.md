# Track Platform - Arquitetura de Fluxo de Dados

## 📊 Visão Geral

```mermaid
flowchart TB
    subgraph "Boundary: Snapshot + Hash + Delta Architecture"
        analyst([Analista / Dashboard Consumer])
        sourceOracle[[Oracle (Fonte Instável)]]
        etlPython[[ETL Python]]
        clickhouse[[ClickHouse]]
        airflow[[Airflow / Scheduler]]
        configsvc[[Config-Service]]

        analyst -- "Consulta histórico e indicadores via" --> clickhouse
        sourceOracle -- "Envia snapshot diário D-1 para" --> etlPython
        etlPython -- "Carrega snapshot normalizado (Silver)" --> clickhouse
        clickhouse -- "Exposição da camada Gold" --> analyst
        airflow -- "Aciona execução diária" --> etlPython
        configsvc -- "Fornece secrets e configurações para" --> etlPython
        configsvc -- "Fornece secrets e parâmetros para" --> airflow
    end
```