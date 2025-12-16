"""
Célula de diagnóstico para testar diferentes service names Oracle.
Execute esta célula no notebook para descobrir o service name correto.
"""
import os
from typing import Dict

def test_oracle_service_names(prefix: str = "SCOT", tunnel=None, service_names: list = None):
    """Testa diferentes service names até encontrar um que funcione."""
    if service_names is None:
        service_names = [
            "bi.grupotracker.com.br",  # Original
            "ORCL",
            "ORCLPDB1",
            "XE",
            "ORCLCDB",
        ]
    
    print("=" * 70)
    print("TESTANDO DIFERENTES SERVICE NAMES")
    print("=" * 70)
    
    # Configura host e porta
    if tunnel and tunnel.is_active:
        host = "127.0.0.1"
        port = str(tunnel.local_bind_port)
        print(f"\n[INFO] Usando túnel SSH: {host}:{port}")
    else:
        host = os.getenv(f"ORACLE_{prefix}_HOST", "10.255.150.11")
        port = os.getenv(f"ORACLE_{prefix}_PORT", "1521")
        print(f"\n[INFO] Usando conexão direta: {host}:{port}")
    
    user = os.getenv(f"ORACLE_{prefix}_USER", "clickhouse")
    password = os.getenv(f"ORACLE_{prefix}_PASSWORD", "qiU!E0oe")
    
    working_config = None
    
    for service in service_names:
        if not service:
            continue
            
        print(f"\n{'='*70}")
        print(f"[TESTE] Service Name: {service}")
        print('='*70)
        
        jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
        print(f"URL: {jdbc_url}")
        
        cfg = {
            "url": jdbc_url,
            "properties": {
                "user": user,
                "password": password,
                "driver": "oracle.jdbc.driver.OracleDriver",
            },
        }
        
        try:
            # Testa com query simples (DUAL é uma tabela especial do Oracle)
            test_df = spark.read.jdbc(
                cfg["url"],
                table="(SELECT 'OK' as status, sysdate as data_hora FROM dual)",
                properties=cfg["properties"]
            )
            result = test_df.collect()
            print(f"\n[✓ SUCESSO] Service '{service}' funciona!")
            print(f"Resultado do teste: {result[0]}")
            working_config = cfg
            break
        except Exception as e:
            error_msg = str(e)
            if "ORA-12514" in error_msg:
                print(f"[✗ FALHOU] Service '{service}' não reconhecido pelo listener")
            elif "ORA-01017" in error_msg:
                print(f"[✗ FALHOU] Credenciais inválidas (usuário/senha)")
                print(f"          Verifique ORACLE_{prefix}_USER e ORACLE_{prefix}_PASSWORD")
            elif "ORA-12541" in error_msg or "TNS:no listener" in error_msg:
                print(f"[✗ FALHOU] Listener não está rodando ou não acessível")
            else:
                print(f"[✗ FALHOU] {error_msg[:200]}")
    
    if working_config:
        print(f"\n{'='*70}")
        print("[SUCESSO] Service name encontrado!")
        print('='*70)
        print(f"Service Name: {service}")
        print(f"URL JDBC: {working_config['url']}")
        print(f"\nConfigure no .env:")
        print(f"ORACLE_{prefix}_SERVICE={service}")
        return working_config
    else:
        print(f"\n{'='*70}")
        print("[ERRO] Nenhum service name funcionou!")
        print('='*70)
        print("\nPróximos passos:")
        print("1. Verifique o service name correto no servidor Oracle:")
        print("   ssh amaro.neto.beanalytic@10.255.150.11")
        print("   lsnrctl services")
        print("\n2. Ou verifique o SID:")
        print("   echo $ORACLE_SID")
        print("\n3. Se souber o SID, configure no .env:")
        print(f"   ORACLE_{prefix}_SID=SEU_SID")
        print("   E use formato SID na URL JDBC")
        return None

# Para usar no notebook:
# from notebooks.test_oracle_service_names import test_oracle_service_names
# oracle_scot_cfg = test_oracle_service_names("SCOT", tunnel=ssh_tunnel)
