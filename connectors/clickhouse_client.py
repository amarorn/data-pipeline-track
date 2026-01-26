import os
import clickhouse_connect

try:
    from config.settings import clickhouse_config
except ImportError:
    from pathlib import Path
    from dotenv import load_dotenv
    
    project_root = Path(__file__).parent.parent
    env_path = os.path.join(project_root, '.env')
    load_dotenv(env_path, override=True)
    
    class ClickHouseConfig:
        def __init__(self):
            self.host = os.getenv('CLICKHOUSE_HOST', '')
            self.port = int(os.getenv('CLICKHOUSE_PORT', '8443'))
            self.user = os.getenv('CLICKHOUSE_USER', 'default')
            self.password = os.getenv('CLICKHOUSE_PASSWORD', '')
            self.database = os.getenv('CLICKHOUSE_DATABASE', 'default')
            self.secure = os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true'
            self.verify = os.getenv('CLICKHOUSE_VERIFY', 'true').lower() == 'true'
    
    clickhouse_config = ClickHouseConfig()


class ClickHouseClient:
    def __init__(self, host=None, port=None, username=None, password=None, database=None, secure=None, verify=None):
        client_params = {
            "host": host or clickhouse_config.host,
            "port": port or clickhouse_config.port,
            "username": username or clickhouse_config.user,
            "password": password or clickhouse_config.password,
            "database": database or clickhouse_config.database,
            "secure": secure if secure is not None else clickhouse_config.secure
        }
        if client_params["secure"]:
            client_params["verify"] = verify if verify is not None else clickhouse_config.verify
        self.client = clickhouse_connect.get_client(**client_params)

    def execute_query(self, query: str):
        return self.client.command(query)

    def execute_query_with_result(self, query: str):
        return self.client.query(query)

    def insert_dataframe(self, table: str, dataframe):
        self.client.insert_df(table, dataframe)

    def close(self):
        self.client.close()
