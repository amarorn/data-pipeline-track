#!/usr/bin/env python3
"""
Executa as células principais do notebook oracle_exploratorio_scot_ginf.ipynb
"""
import os
import sys
from pathlib import Path

# Configura caminhos
repo_root = Path(__file__).parent.parent
os.chdir(repo_root)
sys.path.insert(0, str(repo_root))
sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

print("=" * 70)
print("EXECUTANDO NOTEBOOK: oracle_exploratorio_scot_ginf.ipynb")
print("=" * 70)

# Célula 1: Verificação de dependências
print("\n[1] Verificando dependências...")
import sys
print(f"Python: {sys.version}")
print(f"Executável: {sys.executable}")

try:
    import pyspark
    print(f"✓ PySpark: {pyspark.__version__}")
except Exception as e:
    print(f"✗ PySpark não encontrado: {e}")
    sys.exit(1)

try:
    from track_platform import SSHTunnelManager
    print("✓ track_platform importado com sucesso")
except Exception as e:
    print(f"✗ Erro ao importar track_platform: {e}")
    sys.exit(1)

# Célula 2: Carrega ambiente e cria SparkSession
print("\n[2] Carregando ambiente e criando SparkSession...")
exec(open("notebooks/LoadEnvAndSetupSession.py").read())

# Célula 3: Configuração de túnel SSH
print("\n[3] Configurando túnel SSH...")
import atexit
from pathlib import Path

repo_root = Path.cwd()
while repo_root.name != "data-pipeline-track" and repo_root.parent != repo_root:
    repo_root = repo_root.parent

if str(repo_root / "platform" / "shared-libs") not in sys.path:
    sys.path.insert(0, str(repo_root / "platform" / "shared-libs"))

# Carrega .env com parser corrigido
env_path = repo_root / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        
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
    from track_platform import SSHTunnelManager
except ImportError:
    from track_platform.tunnel import SSHTunnelManager

ssh_tunnel_manager = SSHTunnelManager.from_env()
ssh_tunnel = None

if ssh_tunnel_manager:
    oracle_host = os.getenv("ORACLE_SCOT_HOST") or os.getenv("ORACLE_GINF_HOST")
    if oracle_host and ssh_tunnel_manager.remote_host == "localhost":
        if oracle_host == ssh_tunnel_manager.ssh_host:
            print(f"[INFO] Oracle está no mesmo IP do bastion ({oracle_host})")
            print(f"[INFO] Usando remote_host='localhost'")

if ssh_tunnel_manager:
    print(f"[INFO] Túnel SSH configurado:")
    print(f"  - Bastion: {ssh_tunnel_manager.ssh_host}:{ssh_tunnel_manager.ssh_port}")
    print(f"  - Usuário: {ssh_tunnel_manager.ssh_user}")
    print(f"  - Host remoto: {ssh_tunnel_manager.remote_host}:{ssh_tunnel_manager.remote_port}")
    print(f"[INFO] Iniciando túnel SSH...")
    try:
        from sshtunnel import SSHTunnelForwarder
        
        ssh_auth = {}
        if ssh_tunnel_manager.ssh_password:
            ssh_auth["ssh_password"] = ssh_tunnel_manager.ssh_password
            print(f"[INFO] Usando autenticação por senha")
        elif ssh_tunnel_manager.ssh_pkey:
            pkey_path = Path(ssh_tunnel_manager.ssh_pkey) if isinstance(ssh_tunnel_manager.ssh_pkey, str) else ssh_tunnel_manager.ssh_pkey
            if isinstance(pkey_path, Path) and pkey_path.exists():
                ssh_auth["ssh_pkey"] = str(pkey_path)
            else:
                ssh_auth["ssh_pkey"] = ssh_tunnel_manager.ssh_pkey
            print(f"[INFO] Usando autenticação por chave: {ssh_auth.get('ssh_pkey', 'N/A')}")
        
        print(f"[INFO] Criando túnel: {ssh_tunnel_manager.ssh_host}:{ssh_tunnel_manager.ssh_port} -> {ssh_tunnel_manager.remote_host}:{ssh_tunnel_manager.remote_port}")
        ssh_tunnel_manager.tunnel = SSHTunnelForwarder(
            (ssh_tunnel_manager.ssh_host, ssh_tunnel_manager.ssh_port),
            ssh_username=ssh_tunnel_manager.ssh_user,
            remote_bind_address=(ssh_tunnel_manager.remote_host, ssh_tunnel_manager.remote_port),
            local_bind_address=("127.0.0.1", ssh_tunnel_manager.local_bind_port),
            set_keepalive=30,
            **ssh_auth,
        )
        print(f"[INFO] Iniciando túnel...")
        ssh_tunnel_manager.tunnel.start()
        ssh_tunnel_manager.local_bind_port = ssh_tunnel_manager.tunnel.local_bind_port
        ssh_tunnel = ssh_tunnel_manager
        
        print(f"[OK] Túnel SSH estabelecido!")
        print(f"  - Porta local: {ssh_tunnel.local_bind_port}")
        print(f"  - Status: {'ATIVO' if ssh_tunnel.is_active else 'INATIVO'}")
        print(f"  - Conexão Oracle: localhost:{ssh_tunnel.local_bind_port} -> {ssh_tunnel_manager.remote_host}:{ssh_tunnel_manager.remote_port}")
        
        def cleanup_tunnel():
            if ssh_tunnel and ssh_tunnel.is_active:
                ssh_tunnel.tunnel.stop()
                print("[INFO] Túnel SSH encerrado")
        atexit.register(cleanup_tunnel)
    except Exception as e:
        print(f"[ERRO] Falha ao iniciar túnel SSH: {e}")
        import traceback
        traceback.print_exc()
        print("[WARN] Continuando sem túnel...")
        ssh_tunnel = None
        ssh_tunnel_manager = None
else:
    print("[INFO] Túnel SSH não configurado. Usando conexão direta.")
    ssh_tunnel = None

# Célula 4: Configuração JDBC Oracle
print("\n[4] Configurando conexão JDBC Oracle...")
from typing import Dict

def _oracle_jdbc_config(prefix: str, tunnel=None, direct_config=None) -> Dict[str, str]:
    """
    Configura JDBC Oracle, usando túnel SSH se disponível.
    
    Args:
        prefix: Prefixo para variáveis de ambiente (SCOT, GINF, etc.)
        tunnel: Túnel SSH (opcional)
        direct_config: Dict com configuração direta {'host', 'port', 'service', 'user', 'password'} (opcional)
    """
    if direct_config:
        host = direct_config.get('host', '10.255.150.11')
        port = str(direct_config.get('port', '1521'))
        service = direct_config.get('service', 'bi.grupotracker.com.br')
        user = direct_config.get('user', 'C##AMARO_BE')
        password = direct_config.get('password', 'qiU!E0oe123')
        print(f"[INFO] Usando configuração direta: {host}:{port}/{service}")
    elif tunnel and tunnel.is_active:
        host = "127.0.0.1"
        port = str(tunnel.local_bind_port)
        service = os.getenv(f"ORACLE_{prefix}_SERVICE", "bi.grupotracker.com.br")
        user = os.getenv(f"ORACLE_{prefix}_USER", "C##AMARO_BE")
        password = os.getenv(f"ORACLE_{prefix}_PASSWORD", "qiU!E0oe123")
        print(f"[INFO] Usando túnel SSH: {host}:{port} (redirecionando para {tunnel.remote_host}:{tunnel.remote_port})")
    else:
        host = os.getenv(f"ORACLE_{prefix}_HOST", "10.255.150.11")
        port = os.getenv(f"ORACLE_{prefix}_PORT", "1521")
        service = os.getenv(f"ORACLE_{prefix}_SERVICE", "bi.grupotracker.com.br")
        user = os.getenv(f"ORACLE_{prefix}_USER", "C##AMARO_BE")
        password = os.getenv(f"ORACLE_{prefix}_PASSWORD", "qiU!E0oe123")
        print(f"[INFO] Usando conexão direta: {host}:{port}")
    
    jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
    return {
        "url": jdbc_url,
        "properties": {
            "user": user,
            "password": password,
            "driver": "oracle.jdbc.driver.OracleDriver",
        },
    }

# Configuração direta fornecida pelo usuário (usada apenas se túnel não estiver disponível)
direct_oracle_config = {
    'host': '10.255.150.11',
    'port': '1521',
    'service': 'bi.grupotracker.com.br',
    'user': 'C##AMARO_BE',
    'password': 'qi#U!E0oe123'
}

# Usa túnel se disponível, senão usa configuração direta
if ssh_tunnel and ssh_tunnel.is_active:
    oracle_scot_cfg = _oracle_jdbc_config("SCOT", tunnel=ssh_tunnel)
    oracle_ginf_cfg = _oracle_jdbc_config("GINF", tunnel=ssh_tunnel)
    # Atualiza credenciais mesmo usando túnel
    oracle_scot_cfg["properties"]["user"] = direct_oracle_config['user']
    oracle_scot_cfg["properties"]["password"] = direct_oracle_config['password']
    oracle_ginf_cfg["properties"]["user"] = direct_oracle_config['user']
    oracle_ginf_cfg["properties"]["password"] = direct_oracle_config['password']
else:
    oracle_scot_cfg = _oracle_jdbc_config("SCOT", tunnel=None, direct_config=direct_oracle_config)
    oracle_ginf_cfg = _oracle_jdbc_config("GINF", tunnel=None, direct_config=direct_oracle_config)

print(f"\n[INFO] URL SCOT: {oracle_scot_cfg['url']}")
print(f"[INFO] URL GINF: {oracle_ginf_cfg['url']}")
print(f"[INFO] User: {direct_oracle_config['user']}")
print(f"[INFO] Service: {direct_oracle_config['service']}")

# Célula 5: Testa conexão Oracle
print("\n[5] Testando conexão Oracle...")
from pyspark.sql import DataFrame

def list_oracle_tables(owner: str, cfg: Dict[str, Dict[str, str]], like: str = None) -> DataFrame:
    """Lista tabelas do schema (owner) Oracle."""
    owner_up = owner.upper()
    base_query = f"(SELECT table_name FROM all_tables WHERE owner = '{owner_up}'"
    if like:
        base_query += f" AND table_name LIKE '{like.upper()}'"
    base_query += ") tables_alias"

    return (
        spark.read.jdbc(cfg["url"], table=base_query, properties=cfg["properties"])
        .orderBy("table_name")
    )

try:
    print(f"[INFO] Tentando listar tabelas do schema GINF...")
    print(f"[DEBUG] URL: {oracle_ginf_cfg['url']}")
    print(f"[DEBUG] User: {oracle_ginf_cfg['properties']['user']}")
    print(f"[DEBUG] Password: {'*' * len(oracle_ginf_cfg['properties']['password'])}")
    
    # Primeiro testa uma query simples
    print(f"[INFO] Testando query simples...")
    test_query = "SELECT USER as current_user, SYSDATE as current_time FROM dual"
    test_df = spark.read.jdbc(
        oracle_ginf_cfg["url"],
        table=f"({test_query}) test_alias",
        properties=oracle_ginf_cfg["properties"]
    )
    test_results = test_df.collect()
    if test_results:
        test_row = test_results[0]
        print(f"[OK] Conexão Oracle estabelecida!")
        # Acessa por índice ou nome da coluna
        try:
            user = test_row['CURRENT_USER'] if 'CURRENT_USER' in test_row.asDict() else test_row[0]
            time = test_row['CURRENT_TIME'] if 'CURRENT_TIME' in test_row.asDict() else test_row[1]
            print(f"  Usuário conectado: {user}")
            print(f"  Hora do servidor: {time}")
        except:
            print(f"  Resultado: {test_row}")
    
    # Agora lista tabelas
    print(f"\n[INFO] Listando tabelas do schema GINF...")
    df_tables = list_oracle_tables("GINF", oracle_ginf_cfg)
    table_count = df_tables.count()
    print(f"[OK] Encontradas {table_count} tabelas no schema GINF:")
    df_tables.show(20, truncate=False)
except Exception as e:
    error_msg = str(e)
    print(f"[ERRO] Falha ao conectar ao Oracle: {error_msg[:500]}")
    
    if "ORA-12514" in error_msg:
        print("\n[DIAGNÓSTICO] Service name não reconhecido pelo listener Oracle")
        print("Soluções:")
        print("1. Verifique o service name correto no servidor:")
        print("   ssh amaro.neto.beanalytic@10.255.150.11")
        print("   lsnrctl services")
        print("2. Ou configure ORACLE_SCOT_SID no .env se souber o SID")
    elif "ORA-01017" in error_msg:
        print("\n[DIAGNÓSTICO] Credenciais inválidas (ORA-01017)")
        print(f"Credenciais usadas:")
        print(f"  User: {oracle_ginf_cfg['properties']['user']}")
        print(f"  Password: {'*' * len(oracle_ginf_cfg['properties']['password'])}")
        print(f"  Service: {oracle_ginf_cfg['url'].split('/')[-1]}")
        print("\nSoluções:")
        print("1. Verifique se a senha do usuário C##AMARO_BE foi redefinida:")
        print("   sqlplus system/senha@bi.grupotracker.com.br")
        print('   ALTER USER C##AMARO_BE IDENTIFIED BY "qi#U!E0oe123";')
        print("2. Verifique se o usuário está desbloqueado:")
        print("   ALTER USER C##AMARO_BE ACCOUNT UNLOCK;")
        print("3. Teste a conexão diretamente no servidor:")
        print('   sqlplus C##AMARO_BE/"qi#U!E0oe123"@bi.grupotracker.com.br')
    elif "Connection refused" in error_msg or "could not establish" in error_msg:
        print("\n[DIAGNÓSTICO] Erro de conexão de rede")
        print("Verifique se o túnel SSH está ativo e funcionando")
        if ssh_tunnel:
            print(f"  Túnel status: {'ATIVO' if ssh_tunnel.is_active else 'INATIVO'}")
            print(f"  Porta local: {ssh_tunnel.local_bind_port if ssh_tunnel.is_active else 'N/A'}")

print("\n" + "=" * 70)
print("EXECUÇÃO DO NOTEBOOK CONCLUÍDA")
print("=" * 70)
