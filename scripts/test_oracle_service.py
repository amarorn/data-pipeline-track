#!/usr/bin/env python3
"""
Testa diferentes service names e SIDs para encontrar o correto.
"""
import os
import sys
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

from track_platform.tunnel import load_env_file

env_path = repo_root / ".env"
if env_path.exists():
    load_env_file(env_path)

from track_platform import SSHTunnelManager

print("=" * 70)
print("TESTE DE SERVICE NAMES E SIDs ORACLE")
print("=" * 70)

# Inicia túnel SSH
tunnel_manager = SSHTunnelManager.from_env()
if not tunnel_manager:
    print("[ERRO] Túnel SSH não configurado")
    sys.exit(1)

print("\n[1] Iniciando túnel SSH...")
try:
    tunnel_manager.tunnel = None
    tunnel_manager.start().__enter__()
    print(f"  ✓ Túnel ativo na porta: {tunnel_manager.local_bind_port}")
    print(f"  ✓ Redirecionando para: {tunnel_manager.remote_host}:{tunnel_manager.remote_port}")
except Exception as e:
    print(f"  ✗ Erro: {e}")
    sys.exit(1)

# Testa service names comuns
service_names = [
    "bi.grupotracker.com.br",
    "ORCL",
    "ORCLPDB1",
    "XE",
    "ORCLCDB",
]

sid_names = [
    "ORCL",
    "XE",
]

print("\n[2] Testando Service Names...")
print("    (Nota: Isso requer driver JDBC Oracle instalado)")

host = "127.0.0.1"
port = tunnel_manager.local_bind_port
user = os.getenv("ORACLE_SCOT_USER", "clickhouse")
password = os.getenv("ORACLE_SCOT_PASSWORD", "")

for service in service_names:
    url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
    print(f"\n  Testando: {service}")
    print(f"    URL: {url}")
    
    # Testa conectividade de socket primeiro
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            print(f"    ✓ Porta {port} acessível via socket")
        else:
            print(f"    ✗ Porta {port} não acessível (código: {result})")
            continue
    except Exception as e:
        print(f"    ✗ Erro socket: {e}")
        continue

print("\n" + "=" * 70)
print("PRÓXIMOS PASSOS:")
print("=" * 70)
print("1. Verifique no servidor Oracle os service names disponíveis:")
print("   lsnrctl services")
print("2. Ou verifique o SID:")
print("   echo $ORACLE_SID")
print("3. Configure o service name ou SID correto no .env")
print("=" * 70)

# Mantém túnel aberto
print("\n[INFO] Túnel SSH mantido aberto. Pressione Ctrl+C para encerrar.")
try:
    import time
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[INFO] Encerrando túnel...")
    if tunnel_manager.tunnel:
        tunnel_manager.tunnel.stop()
