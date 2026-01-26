import sys
import os
sys.path.insert(0, '/app')

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, sha2, struct, to_json, current_timestamp
from connectors.clickhouse_client import ClickHouseClient
from config.settings import oracle_config, clickhouse_config, spark_config

TABLES_TO_EXTRACT = [
    {"schema": "ginf", "table": "depara_cliente"},
    {"schema": "ginf", "table": "BASE_CEP_COMPLETA"},
    {"schema": "ginf", "table": "TST_CONTRATOS_BI"},
    {"schema": "siga", "table": "SC5030"},
    {"schema": "siga", "table": "SC6030"},
    {"schema": "ginf", "table": "TST_HISTORICO_SOLICITACOES"},
    {"schema": "ginf", "table": "TST_SOLICIT_CADASTRADAS"},
    {"schema": "siga", "table": "SD2030"},
    {"schema": "siga", "table": "SF2030"},
    {"schema": "siga", "table": "ZTX030"},
    {"schema": "ginf", "table": "TST_CONTRATOS"}
]

def get_primary_keys_from_oracle(spark, schema, table, oracle_jdbc_url):
    try:
        query = f"""
        SELECT column_name
        FROM all_cons_columns
        WHERE constraint_name = (
            SELECT constraint_name
            FROM all_constraints
            WHERE table_name = '{table}'
            AND owner = '{schema.upper()}'
            AND constraint_type = 'P'
            AND rownum = 1
        )
        ORDER BY position
        """
        
        pk_df = (
            spark.read.format("jdbc")
            .option("url", oracle_jdbc_url)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("query", query)
            .option("user", oracle_config.user)
            .option("password", oracle_config.password)
            .load()
        )
        
        primary_keys = [row.column_name for row in pk_df.collect()]
        return primary_keys if primary_keys else []
    except Exception as e:
        print(f"    ⚠ Não foi possível obter chaves primárias: {str(e)}")
        return []

def extract_table_to_bronze(spark, schema, table, ref_date, oracle_jdbc_url):
    extraction_id = f"{schema}_{table}_{ref_date}_{datetime.now().strftime('%H%M%S')}"
    extraction_start = datetime.now()
    
    print(f"\n{'='*80}")
    print(f"Extraindo: {schema}.{table}")
    print(f"Extraction ID: {extraction_id}")
    print(f"{'='*80}")
    
    try:
        primary_keys = get_primary_keys_from_oracle(spark, schema, table, oracle_jdbc_url)
        
        if not primary_keys:
            print(f"    ⚠ Nenhuma chave primária encontrada. Usando todas as colunas como chave.")
            source_df = (
                spark.read.format("jdbc")
                .option("url", oracle_jdbc_url)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("dbtable", f"{schema}.{table}")
                .option("user", oracle_config.user)
                .option("password", oracle_config.password)
                .option("fetchsize", "10000")
                .option("numPartitions", "10")
                .load()
            )
            all_columns = [c.name for c in source_df.schema]
            primary_keys = all_columns[:min(5, len(all_columns))]
        else:
            source_df = (
                spark.read.format("jdbc")
                .option("url", oracle_jdbc_url)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("dbtable", f"{schema}.{table}")
                .option("user", oracle_config.user)
                .option("password", oracle_config.password)
                .option("fetchsize", "10000")
                .option("numPartitions", "10")
                .load()
            )
        
        print(f"    ✓ Schema obtido: {len(source_df.schema.fields)} colunas")
        print(f"    ✓ Chaves primárias: {primary_keys}")
        
        row_count = source_df.count()
        print(f"    ✓ {row_count} registros lidos do Oracle")
        
        if row_count == 0:
            print(f"    ⚠ Tabela vazia, pulando...")
            return
        
        primary_key_expr = concat_ws('|||', *[col(pk) for pk in primary_keys])
        row_hash_expr = sha2(concat_ws('|||', *[col(c.name) for c in source_df.schema]), 256)
        json_expr = to_json(struct(*[col(c.name) for c in source_df.schema]))
        
        bronze_df = (
            source_df
            .select(
                lit(ref_date).cast('date').alias('ref_date'),
                lit(f"{schema}.{table}").alias('table_name'),
                primary_key_expr.alias('primary_key'),
                row_hash_expr.alias('row_hash'),
                json_expr.alias('data'),
                current_timestamp().alias('ingestion_timestamp')
            )
        )
        
        clickhouse_jdbc_url = f"jdbc:clickhouse://{clickhouse_config.host}:{clickhouse_config.port}/{clickhouse_config.database}"
        
        (
            bronze_df.write
            .format('jdbc')
            .option('url', clickhouse_jdbc_url)
            .option('dbtable', 'bronze.snapshot_raw')
            .option('user', clickhouse_config.user)
            .option('password', clickhouse_config.password)
            .option('driver', 'com.clickhouse.jdbc.ClickHouseDriver')
            .option('batchsize', '500000')
            .mode('append')
            .save()
        )
        
        print(f"    ✓ Dados inseridos no Bronze: {row_count} registros")
        
        extraction_end = datetime.now()
        duration = (extraction_end - extraction_start).total_seconds()
        print(f"    ✓ Extração concluída em {duration:.2f} segundos")
        
    except Exception as e:
        error_msg = str(e)[:500]
        print(f"    ✗ Erro ao processar {schema}.{table}: {error_msg}")
        raise

def initialize_bronze():
    client = ClickHouseClient()
    
    ddl = """
    CREATE DATABASE IF NOT EXISTS bronze
    """
    client.execute_query(ddl)
    print("✓ Database bronze verificado")
    
    ddl = """
    CREATE TABLE IF NOT EXISTS bronze.snapshot_raw
    (
        ref_date Date,
        table_name String,
        primary_key String,
        row_hash String,
        data String,
        ingestion_timestamp DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ref_date)
    ORDER BY (ref_date, table_name, primary_key)
    SETTINGS index_granularity = 8192
    """
    client.execute_query(ddl)
    print("✓ Tabela bronze.snapshot_raw verificada")
    
    client.close()

def main():
    ref_date = datetime.now().strftime("%Y-%m-%d")
    
    print("=" * 80)
    print("PIPELINE DE EXTRAÇÃO ORACLE → CLICKHOUSE BRONZE")
    print("=" * 80)
    print(f"Data de referência: {ref_date}")
    print(f"Total de tabelas: {len(TABLES_TO_EXTRACT)}")
    print()
    
    initialize_bronze()
    
    spark_home = os.environ.get('SPARK_HOME', '/usr/local/spark')
    ojdbc_jar = f"{spark_home}/jars/ojdbc8-21.9.0.0.jar"
    clickhouse_jar = f"{spark_home}/jars/clickhouse-jdbc-0.4.6-all.jar"
    
    spark = (
        SparkSession.builder
        .appName(f"OracleToBronze_{ref_date}")
        .config('spark.jars', f"{ojdbc_jar},{clickhouse_jar}")
        .config('spark.driver.memory', spark_config.driver_memory)
        .config('spark.executor.memory', spark_config.executor_memory)
        .config('spark.executor.cores', spark_config.executor_cores)
        .config('spark.sql.shuffle.partitions', spark_config.sql_shuffle_partitions)
        .config('spark.sql.adaptive.enabled', str(spark_config.sql_adaptive_enabled).lower())
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .getOrCreate()
    )
    
    oracle_jdbc_url = f"jdbc:oracle:thin:@//{oracle_config.host}:{oracle_config.port}/{oracle_config.service}"
    
    print("=" * 80)
    print("INICIANDO EXTRAÇÃO DAS TABELAS")
    print("=" * 80)
    
    for table_config in TABLES_TO_EXTRACT:
        try:
            extract_table_to_bronze(
                spark=spark,
                schema=table_config["schema"],
                table=table_config["table"],
                ref_date=ref_date,
                oracle_jdbc_url=oracle_jdbc_url
            )
        except Exception as e:
            print(f"\n✗ Falha na extração de {table_config['schema']}.{table_config['table']}: {str(e)}")
            continue
    
    spark.stop()
    
    print()
    print("=" * 80)
    print("EXTRAÇÃO CONCLUÍDA")
    print("=" * 80)

if __name__ == "__main__":
    main()
