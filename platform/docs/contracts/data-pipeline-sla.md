# SLA - Data Pipeline Domain

## 📋 Service Level Agreement

### Camadas e Horários

| Camada | Horário Início | Duração Máxima | Frequência |
|--------|----------------|----------------|------------|
| Bronze | 02:00 UTC-3    | 2 horas        | Diário     |
| Silver | 04:00 UTC-3    | 2 horas        | Diário     |
| Gold   | 06:00 UTC-3    | 1 hora         | Diário     |

### Tabelas e Freshness

Baseado em: `domains/data-pipeline/configs/tables.yaml`

#### Bronze Tables

| Tabela | Freshness Max | Row Count Min | Owner |
|--------|---------------|---------------|-------|
| oracle_customers | 24h | 1,000 | data-team@track.com |
| oracle_orders | 6h | 5,000 | data-team@track.com |

#### Silver Tables

| Tabela | Data Quality Score Min | Duplicates Max | Owner |
|--------|------------------------|----------------|-------|
| customers | 95% | 0 | data-team@track.com |
| orders | 95% | 0 | data-team@track.com |

#### Gold Metrics

| Métrica | Refresh | Latência Max | Owner |
|---------|---------|--------------|-------|
| daily_sales_summary | Real-time (MV) | 5 min | data-team@track.com |
| customer_ltv | Real-time (MV) | 10 min | data-team@track.com |

## 🚨 Alertas

### Condições de Alerta
