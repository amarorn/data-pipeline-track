#!/usr/bin/env python3
"""
Diagnóstico de conexão Oracle - testa diferentes formatos de URL JDBC.

Ajuda a identificar:
- Se o túnel SSH está funcionando
- Qual service name ou SID usar
- Se as credenciais estão corretas
"""
import os
import sys
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

# Carrega .env
from track_platform.tunnel import load_env_file

env_path = repo_root / ".env"
if env_path.exists():
    load_env_file(env_path)

# Importa SSHTunnelManager
from track_platform import SSHTunnelManager

print("=" * 70)
print("DIAGNÓSTICO DE CONEXÃO ORACLE")
print("=" * 70)

# Verifica túnel SSH
print("\n[1] Verificando túnel SSH...")
tunnel_manager = SSHTunnelManager.from_env()

if tunnel_manager:
    print(f"  ✓ Túnel SSH configurado")
    print(f"    - Bastion: {tunnel_manager.ssh_host}:{tunnel_manager.ssh_port}")
    print(f"    - Remote: {tunnel_manager.remote_host}:{tunnel_manager.remote_port}")
    
    try:
        with tunnel_manager.start() as tunnel:
            print(f"  ✓ Túnel SSH ativo na porta local: {tunnel.local_bind_port}")
            local_port = tunnel.local_bind_port
            remote_host = tunnel.remote_host
            remote_port = tunnel.remote_port
    except Exception as e:
        print(f"  ✗ Erro ao iniciar túnel: {e}")
        local_port = None
        remote_host = os.getenv("ORACLE_SCOT_HOST", "10.255.150.11")
        remote_port = 1521
else:
    print("  ⚠ Túnel SSH não configurado")
    local_port = None
    remote_host = os.getenv("ORACLE_SCOT_HOST", "10.255.150.11")
    remote_port = 1521

# Configurações Oracle
print("\n[2] Configurações Oracle...")
service = os.getenv("ORACLE_SCOT_SERVICE", "bi.grupotracker.com.br")
sid = os.getenv("ORACLE_SCOT_SID")
user = os.getenv("ORACLE_SCOT_USER", "clickhouse")
password = os.getenv("ORACLE_SCOT_PASSWORD", "")

print(f"  - Service Name: {service}")
if sid:
    print(f"  - SID: {sid}")
print(f"  - User: {user}")
print(f"  - Password: {'*' * len(password) if password else 'NÃO CONFIGURADO'}")

# Testa diferentes formatos de URL
print("\n[3] Testando formatos de URL JDBC...")

host = "127.0.0.1" if local_port else remote_host
port = str(local_port) if local_port else str(remote_port)

urls_to_test = []

# Formato 1: Service Name (EZConnect)
url1 = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
urls_to_test.append(("Service Name (EZConnect)", url1))

# Formato 2: SID (se configurado)
if sid:
    url2 = f"jdbc:oracle:thin:@{host}:{port}:{sid}"
    urls_to_test.append(("SID", url2))

# Formato 3: Service Name alternativo (sem /)
url3 = f"jdbc:oracle:thin:@{host}:{port}/{service}"
urls_to_test.append(("Service Name (alternativo)", url3))

for name, url in urls_to_test:
    print(f"\n  Testando: {name}")
    print(f"    URL: {url}")
    
    # Testa conectividade básica (socket)
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        if result == 0:
            print(f"    ✓ Porta {port} está acessível")
        else:
            print(f"    ✗ Porta {port} não está acessível (código: {result})")
    except Exception as e:
        print(f"    ✗ Erro ao testar porta: {e}")

print("\n" + "=" * 70)
print("RECOMENDAÇÕES:")
print("=" * 70)
print("1. Verifique se o service name está correto no listener Oracle")
print("2. Se service name não funcionar, tente usar SID:")
print("   - Configure ORACLE_SCOT_SID no .env")
print("3. Verifique as credenciais (usuário/senha)")
print("4. Teste a conexão diretamente no servidor Oracle:")
print(f"   sqlplus {user}/{password}@{service}")
print("=" * 70)
