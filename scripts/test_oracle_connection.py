import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import oracle_config

print("=" * 80)
print("TESTE DE CONEXÃO ORACLE")
print("=" * 80)
print(f"Host: {oracle_config.host}")
print(f"Port: {oracle_config.port}")
print(f"Service: {oracle_config.service}")
print(f"User: {oracle_config.user}")
print(f"Password: {'*' * len(oracle_config.password)}")
print()

try:
    import oracledb
    import socket
    
    print("Tentando conectar...")
    
    try:
        conn = oracledb.connect(
            user=oracle_config.user,
            password=oracle_config.password,
            host=oracle_config.host,
            port=oracle_config.port,
            service_name=oracle_config.service
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        
        print("✓ Conexão Oracle estabelecida com sucesso!")
        print(f"  Resultado do teste: {result[0]}")
        
        cursor.execute("SELECT sysdate FROM DUAL")
        sysdate = cursor.fetchone()[0]
        print(f"  Data/hora do servidor: {sysdate}")
        
        cursor.close()
        conn.close()
        
    except oracledb.Error as e:
        error_msg = str(e)
        print(f"✗ Erro Oracle: {error_msg}")
        print()
        print("Possíveis causas:")
        if "4011" in error_msg or "closed the connection" in error_msg.lower():
            print("  - Conexão fechada pelo servidor")
            print("  - Pode ser problema de autenticação ou serviço não disponível")
        elif "1017" in error_msg or "invalid username/password" in error_msg.lower():
            print("  - Credenciais incorretas")
        elif "12514" in error_msg or "service name" in error_msg.lower():
            print("  - Service name incorreto ou não encontrado")
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            print("  - Problema de rede/conectividade")
            print("  - Verifique se o túnel SSH está ativo")
        print()
        print("Tentando com formato alternativo (DSN)...")
        
        try:
            dsn = oracledb.makedsn(
                host=oracle_config.host,
                port=oracle_config.port,
                service_name=oracle_config.service
            )
            conn = oracledb.connect(
                user=oracle_config.user,
                password=oracle_config.password,
                dsn=dsn
            )
            print("✓ Conexão estabelecida com DSN!")
            conn.close()
        except Exception as e2:
            print(f"✗ Também falhou com DSN: {str(e2)}")
    
except ImportError:
    print("⚠ Módulo oracledb não encontrado.")
    print("  Instale com: pip install oracledb")
except Exception as e:
    print(f"✗ Erro na conexão: {str(e)}")
    print()
    print("Verifique:")
    print("  1. Se o túnel SSH está ativo (se necessário)")
    print("  2. Se as credenciais estão corretas no .env")
    print("  3. Se o serviço Oracle está acessível")
