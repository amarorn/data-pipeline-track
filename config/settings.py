from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict


class OracleSettings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="ignore")
    
    host: str = Field(alias="ORACLE_HOST")
    port: int = Field(alias="ORACLE_PORT")
    service: str = Field(alias="ORACLE_SERVICE")
    user: str = Field(alias="ORACLE_USER")
    password: str = Field(alias="ORACLE_PASSWORD")


class ClickHouseSettings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="ignore")
    
    host: str = Field(alias="CLICKHOUSE_HOST")
    port: int = Field(default=8443, alias="CLICKHOUSE_PORT")
    user: str = Field(alias="CLICKHOUSE_USER")
    password: str = Field(alias="CLICKHOUSE_PASSWORD")
    database: str = Field(default="default", alias="CLICKHOUSE_DATABASE")
    secure: bool = Field(default=True, alias="CLICKHOUSE_SECURE")
    verify: bool = Field(default=True, alias="CLICKHOUSE_VERIFY")


class SparkSettings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="ignore")
    
    driver_memory: str = Field(alias="SPARK_DRIVER_MEMORY")
    executor_memory: str = Field(alias="SPARK_EXECUTOR_MEMORY")
    executor_cores: int = Field(alias="SPARK_EXECUTOR_CORES")
    sql_shuffle_partitions: int = Field(alias="SPARK_SQL_SHUFFLE_PARTITIONS")
    sql_adaptive_enabled: bool = Field(alias="SPARK_SQL_ADAPTIVE_ENABLED")


class AppSettings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="ignore")
    
    ref_date_format: str = Field(alias="REF_DATE_FORMAT")
    batch_size: int = Field(alias="BATCH_SIZE")


oracle_config = OracleSettings()
clickhouse_config = ClickHouseSettings()
spark_config = SparkSettings()
app_config = AppSettings()
