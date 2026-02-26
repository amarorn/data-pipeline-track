# 🔐 Túnel SSH para Jupyter no Servidor Remoto

## 📊 Situação Atual

- **Servidor remoto**: `10.255.150.11`
- **Usuário**: `amaro.neto.beanalytic`
- **Jupyter rodando em**: Docker container porta `8889`
- **Objetivo**: Acessar Jupyter do Mac local

## ❌ Comando Incorreto (seu atual)

```bash
ssh -L 8888:localhost:8889 -p 22 amaro.neto.beanalytic@10.255.150.11
```

**Problemas:**
1. Está fazendo forward de `localhost:8889` no servidor, mas o Jupyter está em um container Docker
2. A porta `-p 22` é redundante (porta padrão SSH)

## ✅ Comando Correto

### **Opção 1: Túnel Simples (Recomendado)**

```bash
# Formato: ssh -L [porta_local]:[host_remoto]:[porta_remota] usuario@servidor
ssh -L 8888:localhost:8889 amaro.neto.beanalytic@10.255.150.11
```

**Explicação:**
- `-L 8888:localhost:8889` = Porta 8888 no SEU Mac → Porta 8889 no servidor remoto
- `localhost` aqui se refere ao servidor remoto (10.255.150.11), não seu Mac

**Após conectar:**
- Abrir no Mac: `http://localhost:8888`
- Vai conectar no Jupyter do servidor remoto porta 8889

### **Opção 2: Túnel em Background**

```bash
# Mantém túnel ativo em background
ssh -f -N -L 8888:localhost:8889 amaro.neto.beanalytic@10.255.150.11
```

**Flags:**
- `-f` = Vai para background após conectar
- `-N` = Não executa comandos remotos (só túnel)
- `-L` = Port forwarding local

**Para fechar túnel:**
```bash
# Encontrar processo
ps aux | grep "ssh.*8888"

# Matar processo
kill [PID]

# Ou usar pkill
pkill -f "ssh.*8888.*10.255.150.11"
```

### **Opção 3: Túnel com Keepalive**

```bash
# Mantém conexão ativa mesmo se ficar inativo
ssh -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    -L 8888:localhost:8889 \
    amaro.neto.beanalytic@10.255.150.11
```

**Keepalive:**
- Envia pacote a cada 60 segundos
- Desconecta após 3 falhas (3 minutos)

## 🔍 Verificar se Jupyter está Rodando no Servidor

### **1. Conectar no servidor primeiro:**

```bash
ssh amaro.neto.beanalytic@10.255.150.11
```

### **2. Verificar containers Docker:**

```bash
# Ver containers rodando
docker ps | grep jupyter

# Deve mostrar:
# CONTAINER ID   IMAGE                    ... PORTS                    NAMES
# xxxxx          jupyter/pyspark...       ... 0.0.0.0:8889->8889/tcp   track-jupyter
```

### **3. Verificar porta 8889:**

```bash
# Ver se porta está escutando
netstat -tulpn | grep 8889

# Ou com ss
ss -tulpn | grep 8889

# Ou testar localmente
curl http://localhost:8889
```

### **4. Se Jupyter não estiver rodando:**

```bash
# Ir para diretório do projeto
cd /u01/track-data-platform

# Verificar status
docker-compose ps

# Subir containers
docker-compose up -d jupyter

# Ver logs
docker-compose logs -f jupyter
```

## 🚀 Passo a Passo Completo

### **Terminal 1: Criar Túnel SSH**

```bash
# Conectar com túnel
ssh -L 8888:localhost:8889 amaro.neto.beanalytic@10.255.150.11

# Após conectar, verificar se Jupyter está rodando
docker ps | grep jupyter

# Se não estiver, subir:
cd /u01/track-data-platform
docker-compose up -d jupyter
docker-compose logs jupyter

# Manter este terminal aberto!
```

### **Terminal 2 (ou Browser): Acessar Jupyter**

```bash
# Abrir no browser
open http://localhost:8888

# Ou testar primeiro com curl
curl http://localhost:8888
```

## 🔧 Troubleshooting

### **Erro: "bind: Address already in use"**

```bash
# Porta 8888 já está em uso no Mac
# Verificar o que está usando
lsof -i :8888

# Usar outra porta local
ssh -L 9999:localhost:8889 amaro.neto.beanalytic@10.255.150.11

# Acessar: http://localhost:9999
```

### **Erro: "Connection refused"**

```bash
# 1. Verificar se Jupyter está rodando no servidor
ssh amaro.neto.beanalytic@10.255.150.11
docker ps | grep jupyter

# 2. Verificar se porta 8889 está exposta
docker port track-jupyter

# 3. Ver logs do Jupyter
docker-compose logs jupyter | tail -50
```

### **Erro: "Permission denied (publickey)"**

```bash
# Se precisar de senha
ssh -o PreferredAuthentications=password \
    -L 8888:localhost:8889 \
    amaro.neto.beanalytic@10.255.150.11

# Ou configurar chave SSH
ssh-copy-id amaro.neto.beanalytic@10.255.150.11
```

### **Jupyter não aceita conexão sem token**

```bash
# Ver token do Jupyter
docker exec track-jupyter jupyter server list

# Ou no docker-compose logs
docker-compose logs jupyter | grep token

# Usar token na URL:
# http://localhost:8888/?token=XXXXXXXX
```

## 📝 Configuração Permanente (Opcional)

Criar alias no `~/.zshrc` ou `~/.bashrc`:

```bash
# Adicionar ao arquivo
echo 'alias jupyter-tunnel="ssh -L 8888:localhost:8889 amaro.neto.beanalytic@10.255.150.11"' >> ~/.zshrc

# Recarregar
source ~/.zshrc

# Usar
jupyter-tunnel
```

## 🔐 Configuração SSH Config (Recomendado)

Criar/editar `~/.ssh/config`:

```bash
# Abrir editor
nano ~/.ssh/config

# Adicionar:
Host beanalytic-server
    HostName 10.255.150.11
    User amaro.neto.beanalytic
    LocalForward 8888 localhost:8889
    ServerAliveInterval 60
    ServerAliveCountMax 3

# Salvar: Ctrl+O, Enter, Ctrl+X
```

**Usar:**
```bash
# Conectar (já cria túnel automaticamente!)
ssh beanalytic-server

# Acessar: http://localhost:8888
```

## 🎯 Solução Rápida (Copy-Paste)

```bash
# 1. Criar túnel
ssh -L 8888:localhost:8889 amaro.neto.beanalytic@10.255.150.11

# 2. Em outro terminal, testar
curl http://localhost:8888

# 3. Se funcionar, abrir browser
open http://localhost:8888

# 4. Se não funcionar, verificar no servidor:
# (no terminal SSH que você abriu)
docker ps | grep jupyter
docker-compose -f /u01/track-data-platform/docker-compose.yml logs jupyter
```

## 📊 Portas do Sistema

| Serviço | Porta Servidor | Porta Local (Túnel) |
|---------|---------------|---------------------|
| Jupyter | 8889 | 8888 |
| Spark Master UI | 8081 | - |
| Spark Worker UI | 8082 | - |
| Spark History | 18080 | - |

**Para acessar Spark UI também:**
```bash
# Múltiplos port forwards
ssh -L 8888:localhost:8889 \
    -L 8081:localhost:8081 \
    -L 8082:localhost:8082 \
    amaro.neto.beanalytic@10.255.150.11
```

---

**Teste agora e me avise qual erro aparece!** 🚀
