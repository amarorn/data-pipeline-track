#!/usr/bin/env python3
"""
Testa se o Oracle está acessível via SSH em diferentes endereços/portas.
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
import socket

print("=" * 70)
print("TESTE DE PORTAS ORACLE VIA SSH")
print("=" * 70)

# Cria túnel SSH
tunnel_manager = SSHTunnelManager.from_env()
if not tunnel_manager:
    print("[ERRO] Túnel SSH não configurado")
    sys.exit(1)

print(f"\n[1] Criando túnel SSH...")
try:
    with tunnel_manager.start() as tunnel:
        print(f"  ✓ Túnel ativo na porta local: {tunnel.local_bind_port}")
        
        # Testa diferentes endereços remotos
        remote_addresses = [
            ("localhost", 1521),
            ("127.0.0.1", 1521),
            ("10.255.150.11", 1521),
        ]
        
        print(f"\n[2] Testando conectividade Oracle via túnel...")
        print(f"    (Testando diferentes endereços remotos)")
        
        for remote_host, remote_port in remote_addresses:
            print(f"\n  Testando: {remote_host}:{remote_port}")
            
            # Cria novo túnel para este endereço
            try:
                from sshtunnel import SSHTunnelForwarder
                
                test_tunnel = SSHTunnelForwarder(
                    (tunnel_manager.ssh_host, tunnel_manager.ssh_port),
                    ssh_username=tunnel_manager.ssh_user,
                    remote_bind_address=(remote_host, remote_port),
                    local_bind_address=("127.0.0.1", 0),
                    ssh_password=tunnel_manager.ssh_password,
                    set_keepalive=30,
                )
                test_tunnel.start()
                local_port = test_tunnel.local_bind_port
                
                # Testa conectividade
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex(("127.0.0.1", local_port))
                sock.close()
                
                if result == 0:
                    print(f"    ✓ Porta {remote_port} em {remote_host} está acessível!")
                    print(f"    ✓ Use: ORACLE_SSH_REMOTE_HOST={remote_host}")
                    test_tunnel.stop()
                    break
                else:
                    print(f"    ✗ Porta {remote_port} em {remote_host} não acessível (código: {result})")
                
                test_tunnel.stop()
            except Exception as e:
                print(f"    ✗ Erro: {e}")
        else:
            print(f"\n[AVISO] Nenhum endereço funcionou")
            print(f"        Verifique se o Oracle está rodando no servidor")
            print(f"        Execute no servidor: netstat -tlnp | grep 1521")
            
except Exception as e:
    print(f"[ERRO] {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
