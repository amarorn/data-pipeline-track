import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config.settings import oracle_config
except ImportError:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
        
        class OracleConfig:
            def __init__(self):
                self.host = os.getenv('ORACLE_HOST', 'localhost')
                self.port = int(os.getenv('ORACLE_PORT', '1521'))
                self.service = os.getenv('ORACLE_SERVICE', '')
                self.user = os.getenv('ORACLE_USER', '')
                self.password = os.getenv('ORACLE_PASSWORD', '')
        
        oracle_config = OracleConfig()
    except ImportError:
        print("Erro: Instale as dependências:")
        print("  pip install python-dotenv oracledb")
        print("\nOu instale todas as dependências:")
        print("  pip install -r requirements.txt")
        sys.exit(1)

def get_function_source(schema, function_name):
    try:
        import oracledb
        
        print(f"Conectando ao Oracle: {oracle_config.host}:{oracle_config.port}/{oracle_config.service}")
        
        try:
            try:
                oracledb.init_oracle_client()
                print("✓ Modo thick ativado (Oracle Client)")
            except Exception as init_err:
                if "DPI-1047" in str(init_err) or "Cannot locate" in str(init_err):
                    print("⚠ Oracle Client não encontrado. Tentando modo thin...")
                    print("  (Se falhar com erro de criptografia, instale Oracle Instant Client)")
                else:
                    print(f"⚠ Aviso ao inicializar cliente: {str(init_err)}")
        except:
            pass
        
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
        except Exception as e1:
            error_msg = str(e1)
            if "DPY-3001" in error_msg or "thick mode" in error_msg.lower():
                print(f"\n✗ Erro: Oracle exige criptografia nativa (modo thick)")
                print("\nSolução:")
                print("1. Instale Oracle Instant Client:")
                print("   https://www.oracle.com/database/technologies/instant-client/downloads.html")
                print("\n2. Ou use o notebook Jupyter (usa Spark/JDBC):")
                print("   docker compose up -d jupyter")
                print("   Abra: notebooks/view_oracle_function.ipynb")
                print("\n3. Ou execute a query SQL manualmente:")
                print(f"   SELECT line, text FROM all_source")
                print(f"   WHERE owner = UPPER('{schema}')")
                print(f"     AND name = UPPER('{function_name}')")
                print(f"     AND type = 'FUNCTION' ORDER BY line;")
                return
            else:
                print(f"Tentativa 1 falhou: {error_msg}")
                print("Tentando conexão direta...")
                conn = oracledb.connect(
                    user=oracle_config.user,
                    password=oracle_config.password,
                    host=oracle_config.host,
                    port=oracle_config.port,
                    service_name=oracle_config.service
                )
        
        print("✓ Conexão estabelecida")
        
        cursor = conn.cursor()
        
        query = """
        SELECT line, text
        FROM all_source
        WHERE owner = UPPER(:schema)
        AND name = UPPER(:function_name)
        AND type = 'FUNCTION'
        ORDER BY line
        """
        
        cursor.execute(query, schema=schema, function_name=function_name)
        rows = cursor.fetchall()
        
        if not rows:
            print(f"Função {schema}.{function_name} não encontrada ou sem código fonte disponível.")
            print("\nVerificando funções disponíveis no schema...")
            cursor.execute("""
                SELECT DISTINCT name, type
                FROM all_source
                WHERE owner = UPPER(:schema)
                AND type IN ('FUNCTION', 'PROCEDURE')
                ORDER BY name
            """, schema=schema)
            available = cursor.fetchall()
            if available:
                print(f"\nFunções/Procedures disponíveis em {schema}:")
                for name, obj_type in available:
                    print(f"  - {name} ({obj_type})")
            cursor.close()
            conn.close()
            return
        
        print("=" * 80)
        print(f"CÓDIGO FONTE: {schema}.{function_name}")
        print("=" * 80)
        print()
        
        for line_num, text in rows:
            print(f"{line_num:4d} | {text.rstrip()}")
        
        print()
        print("=" * 80)
        
        cursor.close()
        conn.close()
        
    except ImportError:
        print("Módulo oracledb não encontrado.")
        print("Instale com: pip install oracledb")
    except Exception as e:
        error_msg = str(e)
        if "DPY-3001" in error_msg or "thick mode" in error_msg.lower() or "Native Network Encryption" in error_msg:
            print(f"\n✗ Erro: Oracle exige criptografia nativa (modo thick)")
            print("\nSolução:")
            print("1. Instale Oracle Instant Client:")
            print("   https://www.oracle.com/database/technologies/instant-client/downloads.html")
            print("   macOS: brew install instantclient-basic")
            print("\n2. Ou use o notebook Jupyter (usa Spark/JDBC):")
            print("   docker compose up -d jupyter")
            print("   Abra: notebooks/view_oracle_function.ipynb")
            print("\n3. Ou execute a query SQL manualmente (veja opções acima)")
        elif "DPY-4011" in error_msg or "connection closed" in error_msg.lower():
            print(f"\n⚠ Conexão fechada pelo servidor Oracle")
            print("\nPossíveis causas:")
            print("- Túnel SSH não está funcionando corretamente")
            print("- Oracle rejeitou a conexão")
            print("\nSoluções:")
            print("1. Verifique o túnel SSH: lsof -i :1521")
            print("2. Use o notebook Jupyter (usa Spark/JDBC)")
            print("3. Execute a query SQL manualmente")
        else:
            print(f"Erro ao consultar função: {str(e)}")
            if "traceback" not in error_msg.lower():
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    schema = "BISTAGE"
    function_name = "F_CONSULTA_INST_BI"
    
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
    if len(sys.argv) > 2:
        schema = sys.argv[2]
    
    print("\n" + "=" * 80)
    print("VISUALIZAR CÓDIGO FONTE DA FUNÇÃO ORACLE")
    print("=" * 80)
    print(f"\nFunção: {schema}.{function_name}")
    print("\nOPÇÕES:")
    print("\n1. Notebook Jupyter (recomendado):")
    print("   - Abra: notebooks/view_oracle_function.ipynb")
    print("   - Execute as células no Jupyter Lab")
    print("   - Requer: docker compose up -d jupyter")
    print("\n2. Query SQL manual:")
    print("   - Arquivo: scripts/view_oracle_function_sql.sql")
    print("   - Ou execute no seu cliente SQL:")
    print(f"     SELECT line, text")
    print(f"     FROM all_source")
    print(f"     WHERE owner = UPPER('{schema}')")
    print(f"       AND name = UPPER('{function_name}')")
    print(f"       AND type = 'FUNCTION'")
    print(f"     ORDER BY line;")
    print("\n3. Tentando conexão automática...")
    print("=" * 80 + "\n")
    
    try:
        get_function_source(schema, function_name)
    except Exception as e:
        print(f"\n⚠ Conexão automática falhou: {str(e)}")
        print("\nUse uma das opções acima para visualizar a função.")
