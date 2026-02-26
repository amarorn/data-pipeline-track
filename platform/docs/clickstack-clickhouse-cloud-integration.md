# ClickStack + ClickHouse Cloud (Opção B)

Integração do ClickStack com o ClickHouse Cloud existente. Telemetria OTel é enviada ao Cloud; sem necessidade de rodar ClickHouse local.

## Arquitetura

```
[Pipeline Spark / Jupyter / Scripts]
        |
        | OTEL_EXPORTER_OTLP_ENDPOINT
        v
[ClickStack OTel Collector] (Docker local)
  - 4317 gRPC
  - 4318 HTTP
        |
        | HTTPS
        v
[ClickHouse Cloud] (e1a1lieug8.us-central1.gcp.clickhouse.cloud:8443)
        |
        v
[HyperDX UI] (Managed no Console ou self-hosted)
```

## Pré-requisitos

- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` no `.env`
- Endpoint HTTP: `https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}`

## 1. OTel Collector (gateway)

O collector recebe logs, traces e métricas OTel e encaminha ao ClickHouse Cloud.

### Docker standalone

```bash
export CLICKHOUSE_ENDPOINT="https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}"

docker run -d \
  --name clickstack-otel-collector \
  -e CLICKHOUSE_ENDPOINT="${CLICKHOUSE_ENDPOINT}" \
  -e CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
  -e CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
  -p 4317:4317 \
  -p 4318:4318 \
  clickhouse/clickstack-otel-collector:latest
```

### Via Docker Compose

```bash
docker compose --profile observability up -d clickstack
```

O container `clickstack` inclui HyperDX UI (8080) e OTel Collector (4317/4318). Acesse http://localhost:8080.

## 2. UI (visualização)

### Opção A: ClickStack All-in-One (local)

Com `docker compose --profile observability up -d clickstack`:

1. Acesse http://localhost:8080
2. Crie usuário (primeira vez)
3. Vá em **Team Settings** e adicione conexão com ClickHouse Cloud:
   - Host: `${CLICKHOUSE_HOST}`
   - Port: `8443`
   - User/Password do `.env`
4. Crie data sources para `otel_logs`, `otel_traces`, `otel_metrics` (database `default`)

### Opção B: Managed ClickStack (Console Cloud)

1. Acesse o [ClickHouse Cloud Console](https://clickhouse.cloud)
2. Selecione o serviço (e1a1lieug8...)
3. Menu lateral: **ClickStack**
4. Se o recurso estiver habilitado, use "Launch ClickStack" para acessar a UI

Recurso em beta. Se não aparecer, use a Opção A (All-in-One).

### Opção C: HyperDX self-hosted (sem all-in-one)

Requer MongoDB para estado (dashboards, alertas).

```bash
docker run -d \
  -e MONGO_URI=mongodb://host.docker.internal:27017/hyperdx \
  -p 8080:8080 \
  clickhouse/clickstack-hyperdx:latest
```

1. Acesse http://localhost:8080
2. Crie usuário
3. Em "Complete connection credentials", informe:
   - Host: `e1a1lieug8.us-central1.gcp.clickhouse.cloud`
   - Port: `8443`
   - User: `default`
   - Password: (do .env)
   - Secure: sim

## 3. Instrumentação do pipeline

Defina o endpoint OTel nas aplicações:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_SERVICE_NAME=track-data-pipeline
```

Exemplo Python (OpenTelemetry SDK):

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="localhost:4317", insecure=True))
)
tracer = trace.get_tracer("track-pipeline", "1.0.0")

with tracer.start_as_current_span("process_table") as span:
    span.set_attribute("table", "ginf.depara_cliente")
    # ... processamento
```

## 4. Portas

| Porta | Serviço        | Uso                          |
|-------|----------------|------------------------------|
| 4317  | OTel gRPC      | Receber traces/metrics       |
| 4318  | OTel HTTP      | Receber logs (HTTP OTLP)     |
| 8080  | HyperDX UI     | Apenas se self-hosted        |

## 5. Produção

- Criar usuário dedicado no ClickHouse Cloud para ingestão (não usar `default`)
- Usar rede interna para o collector (não expor 4317/4318 publicamente)
- Considerar TLS no endpoint OTLP em produção

## Troubleshooting

### Erro: "Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and Authorization HTTP header simultaneously"

Ocorre quando o cliente envia `Authorization: Basic` e headers `X-ClickHouse-*` ao mesmo tempo. O ClickHouse Cloud rejeita a mistura.

**Alternativas:**

1. **Managed ClickStack** (recomendado): No [Console ClickHouse Cloud](https://clickhouse.cloud) → serviço → menu **ClickStack**. A UI hospedada usa o auth integrado do Cloud.

2. **Collector-only + Grafana**: Use apenas o OTel collector (sem HyperDX):
   ```bash
   docker run -d -e CLICKHOUSE_ENDPOINT="https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}" \
     -e CLICKHOUSE_USER="${CLICKHOUSE_USER}" -e CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
     -p 4317:4317 -p 4318:4318 clickhouse/clickstack-otel-collector:latest
   ```
   Consulte `otel_logs`, `otel_traces` e `otel_metrics` no SQL Console do Cloud ou no Grafana.

3. **Verificar IP**: O erro pode ser de rede. Confirme que o IP do servidor está na lista de IPs permitidos do serviço Cloud (Settings → IP Access List).

## Referências

- [ClickStack OTel Collector](https://clickhouse.com/docs/use-cases/observability/clickstack/ingesting-data/otel-collector)
- [Managed ClickStack](https://clickhouse.com/docs/use-cases/observability/clickstack/deployment/hyperdx-clickhouse-cloud)
- [HyperDX Only](https://clickhouse.com/docs/use-cases/observability/clickstack/deployment/hyperdx-only)
