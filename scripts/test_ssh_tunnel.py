#!/usr/bin/env python3
"""
Teste rápido do túnel SSH - estabelece conexão e verifica se está funcionando.
"""
import os
import sys
import time
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

from track_platform import SSHTunnelManager
from track_platform.logging import setup_logger

logger = setup_logger("ssh_tunnel_test")


def main():
    """Testa configuração de túnel SSH."""
    logger.info("Verificando configuração de túnel SSH...")

    tunnel = SSHTunnelManager.from_env()

    if not tunnel:
        logger.error(
            "Túnel SSH não configurado. Configure as variáveis no .env:"
            "\n  ORACLE_SSH_ENABLED=true"
            "\n  ORACLE_SSH_HOST=10.255.150.11"
            "\n  ORACLE_SSH_USER=amaro.neto.beanalytic"
            "\n  ORACLE_SSH_PASSWORD=qiU!EOoe"
            "\n  ORACLE_SSH_REMOTE_HOST=localhost"
            "\n  ORACLE_SSH_REMOTE_PORT=1521"
        )
        return 1

    logger.info(f"Configuração detectada:")
    logger.info(f"  SSH Host: {tunnel.ssh_host}:{tunnel.ssh_port}")
    logger.info(f"  SSH User: {tunnel.ssh_user}")
    logger.info(f"  Remote: {tunnel.remote_host}:{tunnel.remote_port}")

    try:
        logger.info("Iniciando túnel SSH...")
        with tunnel.start() as active_tunnel:
            logger.info(
                f"✓ Túnel estabelecido com sucesso!"
                f"\n  Conexão Oracle disponível em: localhost:{active_tunnel.local_bind_port}"
                f"\n  Testando conexão por 3 segundos..."
            )

            time.sleep(3)

            if active_tunnel.is_active:
                logger.info("✓ Túnel está ativo e funcionando!")
                logger.info(
                    f"\nPara usar em código:"
                    f"\n  ORACLE_HOST=127.0.0.1"
                    f"\n  ORACLE_PORT={active_tunnel.local_bind_port}"
                )
                return 0
            else:
                logger.error("✗ Túnel não está ativo")
                return 1

    except KeyboardInterrupt:
        logger.info("Teste interrompido pelo usuário")
        return 0
    except Exception as e:
        logger.error(f"✗ Erro ao estabelecer túnel: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
