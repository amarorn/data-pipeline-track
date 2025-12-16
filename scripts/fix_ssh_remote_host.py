#!/usr/bin/env python3
"""
Corrige ORACLE_SSH_REMOTE_HOST para localhost quando Oracle está no mesmo servidor SSH.
"""
import os
import sys
from pathlib import Path

repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

from track_platform.tunnel import load_env_file

env_path = repo_root / ".env"
if not env_path.exists():
    print("[ERRO] Arquivo .env não encontrado")
    sys.exit(1)

# Carrega .env
load_env_file(env_path)

ssh_host = os.getenv("ORACLE_SSH_HOST", "").strip()
oracle_host = os.getenv("ORACLE_SCOT_HOST", os.getenv("ORACLE_GINF_HOST", "")).strip()
remote_host = os.getenv("ORACLE_SSH_REMOTE_HOST", "").strip()

print("=" * 70)
print("CORREÇÃO DE ORACLE_SSH_REMOTE_HOST")
print("=" * 70)

print(f"\nConfiguração atual:")
print(f"  ORACLE_SSH_HOST: {ssh_host}")
print(f"  ORACLE_SCOT_HOST: {oracle_host}")
print(f"  ORACLE_SSH_REMOTE_HOST: {remote_host}")

# Se SSH host e Oracle host são iguais, remote_host deve ser localhost
if ssh_host and oracle_host and ssh_host == oracle_host:
    if remote_host != "localhost":
        print(f"\n[INFO] SSH host e Oracle host são iguais ({ssh_host})")
        print(f"[INFO] ORACLE_SSH_REMOTE_HOST deve ser 'localhost'")
        print(f"[INFO] Valor atual: '{remote_host}'")
        
        # Lê o arquivo .env
        lines = env_path.read_text(encoding="utf-8").splitlines()
        updated = False
        
        with open(env_path, "w", encoding="utf-8") as f:
            for line in lines:
                if line.strip().startswith("ORACLE_SSH_REMOTE_HOST="):
                    # Remove comentários inline
                    if "#" in line:
                        key_part = line.split("=")[0]
                        f.write(f"{key_part}=localhost\n")
                    else:
                        f.write("ORACLE_SSH_REMOTE_HOST=localhost\n")
                    updated = True
                else:
                    f.write(line + "\n")
        
        if updated:
            print(f"\n[OK] Arquivo .env atualizado!")
            print(f"     ORACLE_SSH_REMOTE_HOST=localhost")
        else:
            print(f"\n[INFO] Adicionando ORACLE_SSH_REMOTE_HOST=localhost ao .env...")
            with open(env_path, "a", encoding="utf-8") as f:
                f.write("\n# Túnel SSH - remote host (localhost quando Oracle está no mesmo servidor)\n")
                f.write("ORACLE_SSH_REMOTE_HOST=localhost\n")
            print(f"[OK] Configuração adicionada!")
    else:
        print(f"\n[OK] ORACLE_SSH_REMOTE_HOST já está configurado como 'localhost'")
elif ssh_host and not oracle_host:
    print(f"\n[INFO] ORACLE_SCOT_HOST não configurado")
    print(f"[INFO] Assumindo que Oracle está no mesmo servidor SSH")
    print(f"[INFO] Configurando ORACLE_SSH_REMOTE_HOST=localhost...")
    
    # Adiciona ou atualiza
    lines = env_path.read_text(encoding="utf-8").splitlines()
    found = False
    
    with open(env_path, "w", encoding="utf-8") as f:
        for line in lines:
            if line.strip().startswith("ORACLE_SSH_REMOTE_HOST="):
                f.write("ORACLE_SSH_REMOTE_HOST=localhost\n")
                found = True
            else:
                f.write(line + "\n")
        
        if not found:
            f.write("\n# Túnel SSH - remote host (localhost quando Oracle está no mesmo servidor)\n")
            f.write("ORACLE_SSH_REMOTE_HOST=localhost\n")
    
    print(f"[OK] Configuração atualizada!")
else:
    print(f"\n[INFO] SSH host ({ssh_host}) e Oracle host ({oracle_host}) são diferentes")
    print(f"[INFO] ORACLE_SSH_REMOTE_HOST deve apontar para o host do Oracle")
    print(f"[INFO] Valor atual: '{remote_host}'")
    if not remote_host or remote_host == "localhost":
        print(f"[AVISO] Se Oracle está em outro servidor, configure:")
        print(f"        ORACLE_SSH_REMOTE_HOST={oracle_host}")

print("\n" + "=" * 70)
print("PRÓXIMOS PASSOS:")
print("=" * 70)
print("1. Execute novamente o diagnóstico:")
print("   python scripts/diagnose_ssh_connection.py")
print("2. Teste a conexão Oracle no notebook")
print("=" * 70)
