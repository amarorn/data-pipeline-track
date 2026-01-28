# Configuração de Túnel SSH para Oracle

Se o servidor Oracle está em uma rede privada/VPN e não é acessível diretamente, é necessário criar um túnel SSH.

## Opção 1: Túnel via Docker Compose (Recomendado)

### 1. Configure as variáveis no `.env`:

```bash
# Configurações do túnel SSH
SSH_GATEWAY=gateway.example.com
SSH_USER=seu_usuario
SSH_KEY=/path/to/ssh/key  # Opcional, se usar chave SSH
SSH_PASSWORD=sua_senha   # Opcional, se usar senha (não recomendado)
LOCAL_PORT=1521            # Porta local (padrão: 1521)

# Configurações do Oracle (via túnel, use localhost)
ORACLE_HOST=localhost      # Use localhost quando o túnel estiver ativo
ORACLE_PORT=1521
ORACLE_SERVICE=bi.grupotracker.com.br
ORACLE_USER=clickhouse
ORACLE_PASSWORD=sua_senha
ORACLE_TUNNEL_HOST=10.255.150.11  # Host remoto do Oracle usado pelo túnel
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
SSH_PASSWORD=sua_senha   # Opcional, se usar senha (não recomendado)
ORACLE_TUNNEL_HOST=10.255.150.11  # Host remoto do Oracle usado pelo túnel
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

## Configuração da Chave SSH no Servidor

Para usar autenticação por chave SSH (recomendado), você precisa:

### 1. Gerar par de chaves (se ainda não tiver)

No seu ambiente local:

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/oracle_tunnel_key -N ""
```

Isso cria dois arquivos:
- `~/.ssh/oracle_tunnel_key` (chave privada - fica no seu computador)
- `~/.ssh/oracle_tunnel_key.pub` (chave pública - vai para o servidor)

### 2. Copiar a chave pública para o servidor gateway

**Opção A: Usando ssh-copy-id (mais fácil)**

```bash
ssh-copy-id -i ~/.ssh/oracle_tunnel_key.pub amaro.neto.beanalytic@10.255.150.11
```

**Opção B: Manual**

1. Copie o conteúdo da chave pública:
```bash
cat ~/.ssh/oracle_tunnel_key.pub
```

2. Conecte no servidor gateway:
```bash
ssh amaro.neto.beanalytic@10.255.150.11
```

3. No servidor, adicione a chave pública ao arquivo `~/.ssh/authorized_keys`:
```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
echo "conteúdo_da_chave_pública" >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

**Onde colocar no servidor:**
- Arquivo: `~/.ssh/authorized_keys` (ou `/home/amaro.neto.beanalytic/.ssh/authorized_keys`)
- Permissões: `600` (apenas leitura/escrita para o dono)
- Diretório `.ssh`: `700` (apenas acesso para o dono)

### 3. Configurar no .env

No arquivo `.env` do projeto:

```bash
SSH_KEY=/Users/amaro/.ssh/oracle_tunnel_key
SSH_GATEWAY=10.255.150.11
SSH_USER=amaro.neto.beanalytic
```

**Importante:** Use o caminho completo para a chave privada (não a pública).

### 4. Testar a conexão

```bash
ssh -i ~/.ssh/oracle_tunnel_key amaro.neto.beanalytic@10.255.150.11
```

Se conectar sem pedir senha, está configurado corretamente.

## Notas Importantes

1. **Quando usar túnel**: Se `ORACLE_HOST` aponta para `localhost`, o código assume que há um túnel ativo
2. **Porta local**: Se usar uma porta diferente de 1521, ajuste `LOCAL_PORT` e `ORACLE_PORT` no `.env`
3. **Reconexão automática**: O container `oracle-tunnel` tem `restart: unless-stopped` para reconectar automaticamente
4. **Segurança**: Use chaves SSH quando possível em vez de senhas
5. **Chave privada**: Nunca compartilhe a chave privada. Ela fica apenas no seu ambiente local
6. **Chave pública**: A chave pública pode ser compartilhada e deve estar no servidor gateway
