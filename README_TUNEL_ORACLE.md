# Configuração de Túnel SSH para Oracle

Se o servidor Oracle está em uma rede privada/VPN e não é acessível diretamente, é necessário criar um túnel SSH.

## Opção 1: Túnel via Docker Compose (Recomendado)

### 1. Configure as variáveis no `.env`:

```bash
# Configurações do túnel SSH
SSH_GATEWAY=gateway.example.com
SSH_USER=seu_usuario
SSH_KEY=/path/to/ssh/key  # Opcional, se usar chave SSH
LOCAL_PORT=1521            # Porta local (padrão: 1521)

# Configurações do Oracle (via túnel, use localhost)
ORACLE_HOST=localhost      # Use localhost quando o túnel estiver ativo
ORACLE_PORT=1521
ORACLE_SERVICE=bi.grupotracker.com.br
ORACLE_USER=clickhouse
ORACLE_PASSWORD=sua_senha
```

### 2. Inicie o túnel junto com os serviços:

```bash
docker compose --profile tunnel up -d
```

### 3. Verifique se o túnel está ativo:

```bash
docker logs oracle-tunnel
```

## Opção 2: Túnel Manual (Script)

### 1. Configure as variáveis no `.env`:

```bash
SSH_GATEWAY=gateway.example.com
SSH_USER=seu_usuario
SSH_KEY=/path/to/ssh/key  # Opcional
```

### 2. Execute o script:

```bash
./scripts/setup_oracle_tunnel.sh
```

### 3. Ajuste o `.env` para usar localhost:

```bash
ORACLE_HOST=localhost
ORACLE_PORT=1521
```

### 4. Para parar o túnel:

```bash
./scripts/stop_oracle_tunnel.sh
```

## Opção 3: Túnel Manual (Comando Direto)

Execute manualmente:

```bash
ssh -L 1521:10.255.150.11:1521 -N usuario@gateway.example.com
```

Depois ajuste o `.env`:

```bash
ORACLE_HOST=localhost
ORACLE_PORT=1521
```

## Verificação

Após configurar o túnel, teste a conectividade:

```bash
docker exec track-jupyter python -c "
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
result = sock.connect_ex(('localhost', 1521))
sock.close()
print('✓ Conexão OK' if result == 0 else '✗ Falha na conexão')
"
```

## Notas Importantes

1. **Quando usar túnel**: Se `ORACLE_HOST` aponta para `localhost`, o código assume que há um túnel ativo
2. **Porta local**: Se usar uma porta diferente de 1521, ajuste `LOCAL_PORT` e `ORACLE_PORT` no `.env`
3. **Reconexão automática**: O container `oracle-tunnel` tem `restart: unless-stopped` para reconectar automaticamente
4. **Segurança**: Use chaves SSH quando possível em vez de senhas

