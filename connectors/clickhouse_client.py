import clickhouse_connect
from config.settings import clickhouse_config


class ClickHouseClient:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            username=clickhouse_config.user,
            password=clickhouse_config.password,
            database=clickhouse_config.database,
            secure=clickhouse_config.secure
        )

    def execute_query(self, query: str):
        return self.client.command(query)

    def execute_query_with_result(self, query: str):
        return self.client.query(query)

    def insert_dataframe(self, table: str, dataframe):
        self.client.insert_df(table, dataframe)

    def close(self):
        self.client.close()
