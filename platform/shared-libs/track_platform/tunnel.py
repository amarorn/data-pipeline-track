"""
Gerenciamento de túnel SSH para conexões seguras com bancos de dados.

Implementa SSHTunnelForwarder para criar túneis SSH que redirecionam
conexões locais para servidores remotos através de um bastion host.
"""
from __future__ import annotations

import os
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def load_env_file(env_path: Path) -> None:
    """Carrega variáveis de ambiente de um arquivo .env, removendo comentários inline.
    
    Args:
        env_path: Caminho para o arquivo .env
    """
    if not env_path.exists():
        return
    
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

try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = None
    logger.warning(
        "sshtunnel não está instalado. Instale com: pip install sshtunnel"
    )


class SSHTunnelManager:
    """Gerencia túneis SSH para conexões com bancos de dados remotos.

    Uso típico:
        with SSHTunnelManager.create_oracle_tunnel() as tunnel:
            # Conexão Oracle agora usa localhost:tunnel.local_bind_port
            oracle_conn = OracleConnection(spark)
    """

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_user: str,
        ssh_password: Optional[str] = None,
        ssh_pkey: Optional[str] = None,
        remote_host: str = "localhost",
        remote_port: int = 1521,
        local_bind_port: int = 0,
    ):
        """Inicializa o gerenciador de túnel SSH.

        Args:
            ssh_host: Host do servidor SSH (bastion)
            ssh_port: Porta SSH (geralmente 22)
            ssh_user: Usuário SSH
            ssh_password: Senha SSH (alternativa a ssh_pkey)
            ssh_pkey: Caminho para chave privada SSH (alternativa a ssh_password)
            remote_host: Host remoto acessível via SSH (default: localhost)
            remote_port: Porta remota a ser redirecionada
            local_bind_port: Porta local para bind (0 = porta aleatória)
        """
        if SSHTunnelForwarder is None:
            raise ImportError(
                "sshtunnel não está instalado. Instale com: pip install sshtunnel"
            )

        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password
        self.ssh_pkey = ssh_pkey
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.local_bind_port = local_bind_port
        self.tunnel: Optional[SSHTunnelForwarder] = None

    @classmethod
    def from_ssh_config(
        cls, host_alias: str, remote_host: str = "localhost", remote_port: int = 1521
    ) -> Optional[SSHTunnelManager]:
        """Cria instância a partir de configuração do arquivo ~/.ssh/config.

        Args:
            host_alias: Alias do host no arquivo ~/.ssh/config
            remote_host: Host remoto acessível via SSH (default: localhost)
            remote_port: Porta remota a ser redirecionada (default: 1521)

        Returns:
            SSHTunnelManager se configurado, None caso contrário
        """
        ssh_config_path = Path.home() / ".ssh" / "config"
        if not ssh_config_path.exists():
            logger.warning(f"Arquivo ~/.ssh/config não encontrado.")
            return None

        try:
            config = {}
            current_host = None
            in_target_host = False

            with open(ssh_config_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue

                    if line.lower().startswith("host "):
                        if current_host == host_alias:
                            break
                        current_host = line.split(None, 1)[1].split()[0]
                        in_target_host = current_host == host_alias
                        if in_target_host:
                            config = {}
                    elif in_target_host:
                        parts = line.split(None, 1)
                        if len(parts) == 2:
                            key = parts[0].lower()
                            value = parts[1].strip().strip('"').strip("'")
                            config[key] = value

            if not config:
                logger.warning(f"Host '{host_alias}' não encontrado em ~/.ssh/config")
                return None

            ssh_host = config.get("hostname", host_alias)
            ssh_port = int(config.get("port", "22"))
            ssh_user = config.get("user")
            ssh_pkey = config.get("identityfile")

            if not ssh_user:
                logger.warning(f"User não configurado para host '{host_alias}'")
                return None

            if ssh_pkey and ssh_pkey.startswith("~"):
                ssh_pkey = str(Path.home() / ssh_pkey[2:])

            return cls(
                ssh_host=ssh_host,
                ssh_port=ssh_port,
                ssh_user=ssh_user,
                ssh_password=None,
                ssh_pkey=ssh_pkey,
                remote_host=remote_host,
                remote_port=remote_port,
                local_bind_port=0,
            )
        except Exception as e:
            logger.error(f"Erro ao ler ~/.ssh/config: {e}")
            return None

    @classmethod
    def from_env(cls, prefix: str = "ORACLE_SSH") -> Optional[SSHTunnelManager]:
        """Cria instância a partir de variáveis de ambiente.

        Variáveis esperadas:
            {prefix}_ENABLED: 'true' para habilitar túnel
            {prefix}_HOST: Host SSH
            {prefix}_PORT: Porta SSH (default: 22)
            {prefix}_USER: Usuário SSH
            {prefix}_PASSWORD: Senha SSH (ou {prefix}_PKEY para chave)
            {prefix}_PKEY: Caminho para chave privada SSH
            {prefix}_REMOTE_HOST: Host remoto (default: localhost)
            {prefix}_REMOTE_PORT: Porta remota (default: 1521)
            {prefix}_LOCAL_PORT: Porta local (default: 0 = aleatória)
            {prefix}_SSH_CONFIG: Alias do host em ~/.ssh/config (alternativa)

        Args:
            prefix: Prefixo das variáveis de ambiente

        Returns:
            SSHTunnelManager se habilitado, None caso contrário
        """
        def _clean_env_value(value: Optional[str]) -> str:
            """Remove comentários inline e espaços extras de valores de env."""
            if not value:
                return ""
            value = value.strip()
            if "#" in value:
                comment_pos = value.find("#")
                if comment_pos >= 0:
                    value = value[:comment_pos].rstrip()
            return value

        enabled = os.getenv(f"{prefix}_ENABLED", "false").lower() == "true"
        if not enabled:
            return None

        ssh_config_alias = _clean_env_value(os.getenv(f"{prefix}_SSH_CONFIG"))
        if ssh_config_alias:
            remote_host = _clean_env_value(os.getenv(f"{prefix}_REMOTE_HOST", "localhost"))
            remote_port_raw = os.getenv(f"{prefix}_REMOTE_PORT", "1521")
            try:
                remote_port = int(_clean_env_value(remote_port_raw))
            except ValueError:
                logger.warning(
                    f"{prefix}_REMOTE_PORT inválido ({remote_port_raw}). Usando porta padrão 1521."
                )
                remote_port = 1521
            return cls.from_ssh_config(ssh_config_alias, remote_host, remote_port)

        ssh_host_raw = os.getenv(f"{prefix}_HOST")
        if not ssh_host_raw:
            logger.warning(f"{prefix}_HOST não configurado. Túnel desabilitado.")
            return None

        ssh_host = _clean_env_value(ssh_host_raw)
        
        if ":" in ssh_host:
            parts = ssh_host.rsplit(":", 1)
            if len(parts) == 2:
                potential_host, potential_port = parts
                try:
                    int(potential_port)
                    logger.warning(
                        f"{prefix}_HOST contém porta ({ssh_host}). "
                        f"Separando automaticamente: HOST={potential_host}, PORT={potential_port}. "
                        f"Recomenda-se configurar {prefix}_HOST={potential_host} e {prefix}_PORT={potential_port}"
                    )
                    ssh_host = potential_host
                    if os.getenv(f"{prefix}_PORT") is None:
                        os.environ[f"{prefix}_PORT"] = potential_port
                except ValueError:
                    pass
        
        if ssh_host.startswith("~") or ssh_host.startswith("/") or "/" in ssh_host:
            logger.error(
                f"{prefix}_HOST parece ser um caminho de arquivo ({ssh_host}). "
                f"Configure {prefix}_HOST com o hostname/IP do servidor SSH (ex: 10.255.150.11) e "
                f"{prefix}_PKEY com o caminho da chave privada (ex: ~/.ssh/id_rsa)."
            )
            return None

        ssh_port_raw = os.getenv(f"{prefix}_PORT", "22")
        try:
            ssh_port = int(_clean_env_value(ssh_port_raw))
        except ValueError:
            logger.warning(
                f"{prefix}_PORT inválido ({ssh_port_raw}). Usando porta padrão 22."
            )
            ssh_port = 22

        ssh_user = _clean_env_value(os.getenv(f"{prefix}_USER"))
        ssh_password = _clean_env_value(os.getenv(f"{prefix}_PASSWORD"))
        ssh_pkey = _clean_env_value(os.getenv(f"{prefix}_PKEY"))
        remote_host = _clean_env_value(os.getenv(f"{prefix}_REMOTE_HOST", "localhost"))
        
        remote_port_raw = os.getenv(f"{prefix}_REMOTE_PORT", "1521")
        try:
            remote_port = int(_clean_env_value(remote_port_raw))
        except ValueError:
            logger.warning(
                f"{prefix}_REMOTE_PORT inválido ({remote_port_raw}). Usando porta padrão 1521."
            )
            remote_port = 1521
        
        local_bind_port_raw = os.getenv(f"{prefix}_LOCAL_PORT", "0")
        try:
            local_bind_port = int(_clean_env_value(local_bind_port_raw))
        except ValueError:
            logger.warning(
                f"{prefix}_LOCAL_PORT inválido ({local_bind_port_raw}). Usando porta padrão 0."
            )
            local_bind_port = 0

        if not ssh_user:
            logger.warning(f"{prefix}_USER não configurado. Túnel desabilitado.")
            return None

        if not ssh_password and not ssh_pkey:
            logger.warning(
                f"{prefix}_PASSWORD ou {prefix}_PKEY deve ser configurado. "
                "Túnel desabilitado."
            )
            return None

        return cls(
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_user=ssh_user,
            ssh_password=ssh_password,
            ssh_pkey=ssh_pkey,
            remote_host=remote_host,
            remote_port=remote_port,
            local_bind_port=local_bind_port,
        )

    @contextmanager
    def start(self):
        """Inicia o túnel SSH como context manager.

        Yields:
            SSHTunnelManager: Instância para acesso a local_bind_port

        Example:
            with tunnel_manager.start() as tunnel:
                port = tunnel.local_bind_port
        """
        ssh_auth = {}
        if self.ssh_password:
            ssh_auth["ssh_password"] = self.ssh_password
        elif self.ssh_pkey:
            pkey_path = Path(self.ssh_pkey) if isinstance(self.ssh_pkey, str) else self.ssh_pkey
            if isinstance(pkey_path, Path) and pkey_path.exists():
                ssh_auth["ssh_pkey"] = str(pkey_path)
            else:
                ssh_auth["ssh_pkey"] = self.ssh_pkey

        self.tunnel = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            remote_bind_address=(self.remote_host, self.remote_port),
            local_bind_address=("127.0.0.1", self.local_bind_port),
            ssh_config_file=None,
            set_keepalive=30,
            **ssh_auth,
        )

        try:
            self.tunnel.start()
            self.local_bind_port = self.tunnel.local_bind_port
            logger.info(
                f"Túnel SSH estabelecido: "
                f"{self.ssh_host}:{self.ssh_port} -> "
                f"127.0.0.1:{self.local_bind_port} -> "
                f"{self.remote_host}:{self.remote_port}"
            )
            yield self
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Erro ao iniciar túnel SSH: {error_msg}")
            
            # Diagnóstico detalhado do erro
            if "Connection refused" in error_msg or "could not establish" in error_msg.lower():
                logger.error(
                    f"\n[DIAGNÓSTICO SSH] Connection refused:\n"
                    f"  - Verifique se o servidor SSH está acessível: {self.ssh_host}:{self.ssh_port}\n"
                    f"  - Teste manualmente: ssh {self.ssh_user}@{self.ssh_host} -p {self.ssh_port}\n"
                    f"  - Verifique firewall/rede/VPN\n"
                    f"  - Verifique se a porta SSH está correta (geralmente 22)"
                )
            elif "Authentication failed" in error_msg or "authentication" in error_msg.lower():
                logger.error(
                    f"\n[DIAGNÓSTICO SSH] Falha de autenticação:\n"
                    f"  - Verifique usuário: {self.ssh_user}\n"
                    f"  - Verifique senha ou chave privada\n"
                    f"  - Teste manualmente: ssh {self.ssh_user}@{self.ssh_host} -p {self.ssh_port}"
                )
            elif "Name or service not known" in error_msg or "could not resolve" in error_msg.lower():
                logger.error(
                    f"\n[DIAGNÓSTICO SSH] Host não encontrado:\n"
                    f"  - Verifique o hostname/IP: {self.ssh_host}\n"
                    f"  - Verifique DNS/conectividade de rede"
                )
            
            raise
        finally:
            if self.tunnel:
                self.tunnel.stop()
                logger.info("Túnel SSH encerrado")

    @property
    def is_active(self) -> bool:
        """Verifica se o túnel está ativo."""
        return self.tunnel is not None and self.tunnel.is_active
