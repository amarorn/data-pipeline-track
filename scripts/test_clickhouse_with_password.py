import sys
from pathlib import Path
import clickhouse_connect
import os

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import clickhouse_config

if len(sys.argv) > 1:
    password = sys.argv[1]
else:
    password = os.getenv("CLICKHOUSE_PASSWORD_TEST", clickhouse_config.password)

try:
    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=password,
        database=clickhouse_config.database,
        secure=clickhouse_config.secure,
        verify=clickhouse_config.verify if clickhouse_config.secure else None
    )
    
    result = client.query("SELECT 1")
    if result.result_set and result.result_set[0][0] == 1:
        print("✓ Conexão com ClickHouse estabelecida com sucesso!")
        print(f"Host: {clickhouse_config.host}")
        print(f"Database: {clickhouse_config.database}")
        client.close()
        sys.exit(0)
    else:
        print("✗ Resposta inesperada do ClickHouse")
        client.close()
        sys.exit(1)
        
except Exception as error:
    print(f"✗ Erro na conexão: {error}")
    sys.exit(1)

