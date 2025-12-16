#!/usr/bin/env python3
"""
Debug do túnel SSH - mostra configuração e tenta conexão com mais detalhes.
"""
import os
import sys
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

# Carrega .env
env_path = repo_root / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        
        # Remove comentários inline (tudo após # que não está dentro de aspas)
        if "#" in line:
            key_part, value_part = line.split("=", 1)
            value_stripped = value_part.strip()
            
            in_quotes = False
            quote_char = None
            comment_pos = -1
            
            for i, char in enumerate(value_part):
                if char in ('"', "'") and (i == 0 or value_part[i-1] != '\\'):
                    if not in_quotes:
                        in_quotes = True
                        quote_char = char
                    elif char == quote_char:
                        in_quotes = False
                        quote_char = None
                elif char == '#' and not in_quotes:
                    comment_pos = i
                    break
            
            if comment_pos >= 0:
                value_part = value_part[:comment_pos].rstrip()
                line = f"{key_part}={value_part}"
        
        k, v = line.split("=", 1)
        value = v.strip()
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[k.strip()] = value

print("=== Configuração SSH (variáveis de ambiente) ===")
ssh_host = os.getenv('ORACLE_SSH_HOST', '')
ssh_port = os.getenv('ORACLE_SSH_PORT', '')
ssh_pkey = os.getenv('ORACLE_SSH_PKEY', '')

print(f"ORACLE_SSH_ENABLED: {os.getenv('ORACLE_SSH_ENABLED')}")
print(f"ORACLE_SSH_HOST: {repr(ssh_host)}")
print(f"ORACLE_SSH_PORT: {repr(ssh_port)}")
print(f"ORACLE_SSH_USER: {os.getenv('ORACLE_SSH_USER')}")
password = os.getenv('ORACLE_SSH_PASSWORD', '')
print(f"ORACLE_SSH_PASSWORD: {'*' * len(password)} (tamanho: {len(password)})")
print(f"ORACLE_SSH_PKEY: {repr(ssh_pkey)}")
print(f"ORACLE_SSH_REMOTE_HOST: {os.getenv('ORACLE_SSH_REMOTE_HOST')}")
print(f"ORACLE_SSH_REMOTE_PORT: {os.getenv('ORACLE_SSH_REMOTE_PORT')}")

print("\n=== Validação de Configuração ===")
issues = []

if ssh_host:
    if ":" in ssh_host:
        parts = ssh_host.rsplit(":", 1)
        if len(parts) == 2:
            potential_host, potential_port = parts
            try:
                int(potential_port)
                issues.append(
                    f"⚠️  ORACLE_SSH_HOST contém porta: {repr(ssh_host)}\n"
                    f"   Correção: ORACLE_SSH_HOST={potential_host}\n"
                    f"            ORACLE_SSH_PORT={potential_port}"
                )
            except ValueError:
                pass
    
    if ssh_host.startswith("~") or ssh_host.startswith("/") or "/" in ssh_host:
        issues.append(
            f"❌ ORACLE_SSH_HOST parece ser um caminho de arquivo: {repr(ssh_host)}\n"
            f"   Correção: ORACLE_SSH_HOST deve ser o hostname/IP (ex: 10.255.150.11)\n"
            f"            Use ORACLE_SSH_PKEY para o caminho da chave privada"
        )

if ssh_pkey and (ssh_pkey.startswith("~") or ssh_pkey.startswith("/")):
    pkey_path = Path(ssh_pkey.replace("~", str(Path.home())))
    if not pkey_path.exists():
        issues.append(
            f"⚠️  ORACLE_SSH_PKEY aponta para arquivo inexistente: {repr(ssh_pkey)}"
        )

if issues:
    print("\n".join(issues))
else:
    print("✓ Configuração parece válida")

from track_platform import SSHTunnelManager

print("\n=== Criando SSHTunnelManager ===")
tunnel = SSHTunnelManager.from_env()

if tunnel:
    print(f"SSH Host: {tunnel.ssh_host}:{tunnel.ssh_port}")
    print(f"SSH User: {tunnel.ssh_user}")
    print(f"SSH Password configurada: {bool(tunnel.ssh_password)}")
    print(f"Remote: {tunnel.remote_host}:{tunnel.remote_port}")
    
    print("\n=== Tentando estabelecer túnel ===")
    try:
        with tunnel.start() as active_tunnel:
            print(f"✓ Túnel estabelecido na porta: {active_tunnel.local_bind_port}")
            import time
            time.sleep(2)
    except Exception as e:
        print(f"✗ Erro: {e}")
        import traceback
        traceback.print_exc()
else:
    print("✗ SSHTunnelManager não foi criado")
