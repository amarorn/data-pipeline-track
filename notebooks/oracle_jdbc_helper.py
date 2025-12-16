"""
Helper functions para configuração JDBC Oracle com suporte a Service Name e SID.
"""
import os
from typing import Dict, Optional


def oracle_jdbc_config(prefix: str, tunnel=None, use_sid: bool = False) -> Dict[str, str]:
    """Configura JDBC Oracle, usando túnel SSH se disponível.
    
    Args:
        prefix: Prefixo das variáveis de ambiente (ex: "SCOT", "GINF")
        tunnel: Instância de SSHTunnelManager (opcional)
        use_sid: Se True, usa SID em vez de Service Name
    
    Returns:
        Dicionário com 'url' e 'properties' para JDBC
    """
    if tunnel and tunnel.is_active:
        host = "127.0.0.1"
        port = str(tunnel.local_bind_port)
        print(f"[INFO] Usando túnel SSH: {host}:{port} (redirecionando para {tunnel.remote_host}:{tunnel.remote_port})")
    else:
        host = os.getenv(f"ORACLE_{prefix}_HOST", "10.255.150.11")
        port = os.getenv(f"ORACLE_{prefix}_PORT", "1521")
        if tunnel:
            print(f"[WARN] Túnel SSH configurado mas não está ativo!")
        print(f"[INFO] Usando conexão direta: {host}:{port}")
    
    user = os.getenv(f"ORACLE_{prefix}_USER", "clickhouse")
    password = os.getenv(f"ORACLE_{prefix}_PASSWORD", "qiU!E0oe")
    
    # Suporta tanto Service Name quanto SID
    if use_sid:
        sid = os.getenv(f"ORACLE_{prefix}_SID")
        if not sid:
            raise ValueError(f"ORACLE_{prefix}_SID não configurado. Configure no .env ou use use_sid=False")
        jdbc_url = f"jdbc:oracle:thin:@{host}:{port}:{sid}"
        print(f"[INFO] Usando SID: {sid}")
    else:
        service = os.getenv(f"ORACLE_{prefix}_SERVICE", "bi.grupotracker.com.br")
        jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
        print(f"[INFO] Usando Service Name: {service}")
    
    print(f"[INFO] URL JDBC: {jdbc_url}")
    
    return {
        "url": jdbc_url,
        "properties": {
            "user": user,
            "password": password,
            "driver": "oracle.jdbc.driver.OracleDriver",
        },
    }


def test_oracle_connection(cfg: Dict[str, str]) -> bool:
    """Testa conectividade básica com Oracle (sem autenticação).
    
    Args:
        cfg: Configuração JDBC retornada por oracle_jdbc_config
    
    Returns:
        True se a porta está acessível, False caso contrário
    """
    import socket
    from urllib.parse import urlparse
    
    try:
        # Extrai host e porta da URL JDBC
        url = cfg["url"]
        if "@//" in url:
            # Formato: jdbc:oracle:thin:@//host:port/service
            parts = url.split("@//")[1].split("/")[0]
        elif "@" in url:
            # Formato: jdbc:oracle:thin:@host:port:sid
            parts = url.split("@")[1].split(":")[:2]
            parts = ":".join(parts)
        else:
            return False
        
        host, port = parts.rsplit(":", 1)
        port = int(port)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f"[OK] Porta {port} em {host} está acessível")
            return True
        else:
            print(f"[ERRO] Porta {port} em {host} não está acessível (código: {result})")
            return False
    except Exception as e:
        print(f"[ERRO] Erro ao testar conectividade: {e}")
        return False
