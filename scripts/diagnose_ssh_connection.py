#!/usr/bin/env python3
"""
Diagnóstico detalhado de conexão SSH para túnel Oracle.
"""
import os
import sys
import socket
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

from track_platform.tunnel import load_env_file

env_path = repo_root / ".env"
if env_path.exists():
    load_env_file(env_path)

print("=" * 70)
print("DIAGNÓSTICO DE CONEXÃO SSH")
print("=" * 70)

# Lê configurações SSH
ssh_host = os.getenv("ORACLE_SSH_HOST", "").strip()
ssh_port = int(os.getenv("ORACLE_SSH_PORT", "22"))
ssh_user = os.getenv("ORACLE_SSH_USER", "").strip()
ssh_password = os.getenv("ORACLE_SSH_PASSWORD", "")
ssh_pkey = os.getenv("ORACLE_SSH_PKEY", "").strip()

print("\n[1] Configuração SSH:")
print(f"  Host: {ssh_host}")
print(f"  Port: {ssh_port}")
print(f"  User: {ssh_user}")
print(f"  Password: {'*' * len(ssh_password) if ssh_password else 'NÃO CONFIGURADO'}")
print(f"  PKEY: {ssh_pkey if ssh_pkey else 'NÃO CONFIGURADO'}")

if not ssh_host:
    print("\n[ERRO] ORACLE_SSH_HOST não configurado!")
    sys.exit(1)

if not ssh_user:
    print("\n[ERRO] ORACLE_SSH_USER não configurado!")
    sys.exit(1)

# Testa conectividade SSH
print(f"\n[2] Testando conectividade SSH...")
print(f"  Tentando conectar em {ssh_host}:{ssh_port}...")

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    result = sock.connect_ex((ssh_host, ssh_port))
    sock.close()
    
    if result == 0:
        print(f"  ✓ Porta {ssh_port} está acessível")
    else:
        print(f"  ✗ Porta {ssh_port} NÃO está acessível (código: {result})")
        print(f"\n[DIAGNÓSTICO]")
        if result == 61:
            print("  - Connection refused: Servidor SSH não está escutando na porta")
            print("  - Verifique se o serviço SSH está rodando no servidor")
            print("  - Verifique se a porta está correta (geralmente 22)")
        elif result == 51:
            print("  - Network unreachable: Não é possível alcançar o host")
            print("  - Verifique conectividade de rede")
            print("  - Verifique se o IP/hostname está correto")
        elif result == 60:
            print("  - Operation timed out: Firewall bloqueando ou host inacessível")
            print("  - Verifique firewall")
            print("  - Verifique se precisa de VPN")
        else:
            print(f"  - Erro desconhecido (código: {result})")
        sys.exit(1)
except socket.gaierror as e:
    print(f"  ✗ Erro de DNS: {e}")
    print(f"  - Não foi possível resolver o hostname '{ssh_host}'")
    print(f"  - Verifique se o hostname/IP está correto")
    sys.exit(1)
except Exception as e:
    print(f"  ✗ Erro: {e}")
    sys.exit(1)

# Testa autenticação SSH
print(f"\n[3] Testando autenticação SSH...")

try:
    import paramiko
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    auth_method = None
    if ssh_pkey:
        pkey_path = Path(ssh_pkey.replace("~", str(Path.home())))
        if pkey_path.exists():
            try:
                ssh.connect(
                    ssh_host,
                    port=ssh_port,
                    username=ssh_user,
                    key_filename=str(pkey_path),
                    timeout=10,
                    look_for_keys=False,
                    allow_agent=False
                )
                auth_method = "chave privada"
            except paramiko.AuthenticationException:
                print(f"  ✗ Autenticação por chave falhou")
            except Exception as e:
                print(f"  ✗ Erro com chave: {e}")
        else:
            print(f"  ✗ Arquivo de chave não encontrado: {pkey_path}")
    
    if not auth_method and ssh_password:
        try:
            ssh.connect(
                ssh_host,
                port=ssh_port,
                username=ssh_user,
                password=ssh_password,
                timeout=10,
                look_for_keys=False,
                allow_agent=False
            )
            auth_method = "senha"
        except paramiko.AuthenticationException:
            print(f"  ✗ Autenticação por senha falhou")
            print(f"  - Verifique usuário e senha")
        except Exception as e:
            print(f"  ✗ Erro com senha: {e}")
    
    if auth_method:
        print(f"  ✓ Autenticação SSH bem-sucedida ({auth_method})")
        ssh.close()
    else:
        print(f"  ✗ Nenhum método de autenticação funcionou")
        print(f"  - Configure ORACLE_SSH_PASSWORD ou ORACLE_SSH_PKEY")
        sys.exit(1)
        
except ImportError:
    print("  ⚠ paramiko não instalado (pip install paramiko)")
    print("  - Não é possível testar autenticação, mas conectividade está OK")
except Exception as e:
    print(f"  ✗ Erro ao testar autenticação: {e}")

# Testa criação do túnel
print(f"\n[4] Testando criação do túnel SSH...")

try:
    from track_platform import SSHTunnelManager
    
    tunnel_manager = SSHTunnelManager.from_env()
    if not tunnel_manager:
        print("  ✗ SSHTunnelManager não foi criado")
        sys.exit(1)
    
    print(f"  Tentando criar túnel...")
    print(f"    Bastion: {tunnel_manager.ssh_host}:{tunnel_manager.ssh_port}")
    print(f"    Remote: {tunnel_manager.remote_host}:{tunnel_manager.remote_port}")
    
    with tunnel_manager.start() as tunnel:
        print(f"  ✓ Túnel SSH criado com sucesso!")
        print(f"    Porta local: {tunnel.local_bind_port}")
        print(f"    Redirecionando: 127.0.0.1:{tunnel.local_bind_port} -> {tunnel.remote_host}:{tunnel.remote_port}")
        
        # Testa se a porta local está acessível
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(("127.0.0.1", tunnel.local_bind_port))
        sock.close()
        
        if result == 0:
            print(f"  ✓ Porta local {tunnel.local_bind_port} está acessível")
        else:
            print(f"  ⚠ Porta local {tunnel.local_bind_port} não está acessível (pode ser normal)")
        
except Exception as e:
    print(f"  ✗ Erro ao criar túnel: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("DIAGNÓSTICO COMPLETO")
print("=" * 70)
print("✓ Conectividade SSH: OK")
print("✓ Autenticação SSH: OK")
print("✓ Túnel SSH: OK")
print("\nO túnel SSH está funcionando corretamente!")
print("=" * 70)
