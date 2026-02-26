import sys
import os
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from connectors.clickhouse_client import ClickHouseClient
except ImportError as e:
    if "pydantic_settings" in str(e) or "config.settings" in str(e):
        from dotenv import load_dotenv
        load_dotenv(os.path.join(project_root, '.env'))
        
        import clickhouse_connect
        
        class ClickHouseClient:
            def __init__(self):
                client_params = {
                    "host": os.getenv('CLICKHOUSE_HOST', ''),
                    "port": int(os.getenv('CLICKHOUSE_PORT', '8443')),
                    "username": os.getenv('CLICKHOUSE_USER', 'default'),
                    "password": os.getenv('CLICKHOUSE_PASSWORD', ''),
                    "database": os.getenv('CLICKHOUSE_DATABASE', 'default'),
                    "secure": os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true'
                }
                if client_params["secure"]:
                    client_params["verify"] = os.getenv('CLICKHOUSE_VERIFY', 'true').lower() == 'true'
                self.client = clickhouse_connect.get_client(**client_params)
            
            def execute_query_with_result(self, query: str):
                return self.client.query(query)
            
            def close(self):
                self.client.close()
    else:
        raise


def test_clickhouse_connection():
    try:
        client = ClickHouseClient()
        response = client.execute_query_with_result("SELECT 1")
        if response.result_set and response.result_set[0][0] == 1:
            print("ClickHouse connection validation succeeded")
            return 0
        print("ClickHouse connection validation failed: unexpected result", response.result_set)
        return 2
    except Exception as error:
        print("ClickHouse connection validation failed:", error)
        return 1
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    raise SystemExit(test_clickhouse_connection())

