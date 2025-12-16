"""
Helper para conexão Oracle com diagnóstico e suporte a múltiplos formatos.
"""
import os
import socket
from typing import Dict, Optional, List, Tuple


def test_port_connectivity(host: str, port: int, timeout: int = 5) -> bool:
    """Testa se uma porta está acessível."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def oracle_jdbc_config_with_fallback(
    prefix: str,
    tunnel=None,
    service_names: Optional[List[str]] = None,
    sids: Optional[List[str]] = None,
) -> Tuple[Dict[str, str], str]:
    """Configura JDBC Oracle com fallback automático para diferentes service names/SIDs.
    
    Args:
        prefix: Prefixo das variáveis de ambiente (ex: "SCOT", "GINF")
        tunnel: Instância de SSHTunnelManager (opcional)
        service_names: Lista de service names para tentar (opcional)
        sids: Lista de SIDs para tentar (opcional)
    
    Returns:
        Tupla (config_dict, info_message) onde config_dict contém 'url' e 'properties'
    """
    # Determina host e porta
    if tunnel and tunnel.is_active:
        host = "127.0.0.1"
        port = int(tunnel.local_bind_port)
        remote_info = f"{tunnel.remote_host}:{tunnel.remote_port}"
        print(f"[INFO] Usando túnel SSH: {host}:{port} -> {remote_info}")
    else:
        host = os.getenv(f"ORACLE_{prefix}_HOST", "10.255.150.11")
        port = int(os.getenv(f"ORACLE_{prefix}_PORT", "1521"))
        if tunnel:
            print(f"[WARN] Túnel SSH configurado mas não está ativo!")
        print(f"[INFO] Usando conexão direta: {host}:{port}")
    
    # Testa conectividade da porta
    print(f"[INFO] Testando conectividade na porta {port}...")
    if not test_port_connectivity(host, port):
        return None, f"[ERRO] Porta {port} em {host} não está acessível. Verifique o túnel SSH ou firewall."
    print(f"[OK] Porta {port} está acessível")
    
    user = os.getenv(f"ORACLE_{prefix}_USER", "clickhouse")
    password = os.getenv(f"ORACLE_{prefix}_PASSWORD", "qiU!E0oe")
    
    # Lista de service names para tentar
    if service_names is None:
        service_names = [
            os.getenv(f"ORACLE_{prefix}_SERVICE", "bi.grupotracker.com.br"),
            "ORCL",
            "ORCLPDB1",
            "XE",
        ]
    
    # Lista de SIDs para tentar
    if sids is None:
        sid_env = os.getenv(f"ORACLE_{prefix}_SID")
        sids = [sid_env] if sid_env else ["ORCL", "XE"]
    
    # Tenta service names primeiro
    for service in service_names:
        if not service:
            continue
        url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
        print(f"[INFO] Tentando Service Name: {service}")
        print(f"       URL: {url}")
        return {
            "url": url,
            "properties": {
                "user": user,
                "password": password,
                "driver": "oracle.jdbc.driver.OracleDriver",
            },
        }, f"Service Name: {service}"
    
    # Se nenhum service name funcionar, tenta SID
    for sid in sids:
        if not sid:
            continue
        url = f"jdbc:oracle:thin:@{host}:{port}:{sid}"
        print(f"[INFO] Tentando SID: {sid}")
        print(f"       URL: {url}")
        return {
            "url": url,
            "properties": {
                "user": user,
                "password": password,
                "driver": "oracle.jdbc.driver.OracleDriver",
            },
        }, f"SID: {sid}"
    
    return None, "[ERRO] Nenhum service name ou SID configurado"


def diagnose_oracle_connection_error(error_message: str) -> str:
    """Analisa mensagem de erro Oracle e fornece diagnóstico."""
    if "ORA-12514" in error_message:
        return """
[DIAGNÓSTICO ORA-12514]
O listener Oracle não reconhece o service name fornecido.

Soluções:
1. Verifique o service name correto no servidor Oracle:
   ssh usuario@10.255.150.11
   lsnrctl services
   
2. Ou verifique o SID:
   echo $ORACLE_SID
   
3. Configure o service name ou SID correto no .env:
   ORACLE_SCOT_SERVICE=service_correto
   # OU
   ORACLE_SCOT_SID=sid_correto
"""
    elif "ORA-01017" in error_message:
        return """
[DIAGNÓSTICO ORA-01017]
Credenciais inválidas (usuário/senha).

Verifique:
- ORACLE_SCOT_USER
- ORACLE_SCOT_PASSWORD
"""
    elif "Network Adapter" in error_message or "could not establish" in error_message:
        return """
[DIAGNÓSTICO: Erro de Rede]
Não foi possível estabelecer conexão de rede.

Verifique:
1. Túnel SSH está ativo?
2. Firewall bloqueando a porta?
3. Host e porta corretos?
"""
    else:
        return f"[ERRO] {error_message}"
