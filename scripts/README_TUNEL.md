# Configuração de Túnel SSH para Oracle

Existem duas formas de criar o túnel SSH para acesso ao Oracle:

## Opção 1: Docker Compose (Recomendado)

O `docker-compose.yml` já tem um serviço `oracle-tunnel` configurado:

```bash
docker compose --profile tunnel up -d oracle-tunnel
```

**Vantagens:**
- Reconexão automática (restart: unless-stopped)
- Gerenciado pelo Docker
- Mesma configuração usada em produção

**Verificar status:**
```bash
docker logs oracle-tunnel
docker ps | grep oracle-tunnel
```

**Parar:**
```bash
docker compose --profile tunnel stop oracle-tunnel
```

## Opção 2: Script Manual

Execute o script diretamente:

```bash
./scripts/setup_oracle_tunnel.sh
```

**Vantagens:**
- Funciona sem Docker
- Mais controle sobre o processo

**Parar:**
```bash
./scripts/stop_oracle_tunnel.sh
# ou
pkill -f 'ssh.*10.255.150.11'
```

## Configuração no .env

Ambas as opções usam as mesmas variáveis do `.env`:

```bash
SSH_GATEWAY=seu-gateway.com      # Servidor intermediário (jump host)
SSH_USER=amaro.neto.beanalytic   # Usuário SSH
SSH_KEY=/path/to/key.pem         # Opcional: chave SSH
LOCAL_PORT=1521                   # Porta local (padrão: 1521)
ORACLE_HOST=10.255.150.11        # IP do Oracle (remoto)
ORACLE_PORT=1521                 # Porta do Oracle
```

## Após Criar o Túnel

Quando o túnel estiver ativo, o Oracle será acessível via `localhost:1521`.

**IMPORTANTE:** Se usar túnel, atualize o `.env`:
```bash
ORACLE_HOST=localhost  # Use localhost quando o túnel estiver ativo
```

Ou mantenha `ORACLE_HOST=10.255.150.11` se o código detectar automaticamente o túnel.

## Verificação

Teste a conectividade:
```bash
python scripts/test_oracle_connection.py
```

Ou no notebook:
```python
from connectors.clickhouse_client import ClickHouseClient
from config.settings import oracle_config

# O código deve conectar via localhost:1521 quando o túnel estiver ativo
```
