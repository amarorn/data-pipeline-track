#!/usr/bin/env python3
"""
Silver Layer Pipeline com Spark REAL - VERSÃO CORRIGIDA
Resolve problema de ORDER BY ao criar tabelas ClickHouse manualmente
"""

import os
from datetime import datetime
from typing import Dict, Any
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import clickhouse_connect
import pandas as pd

# ========================================
# CONFIGURAÇÕES
# ========================================
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'e1a1lieug8.us-central1.gcp.clickhouse.cloud')
CH_PORT = int(os.getenv('CLICKHOUSE_PORT', 8443))
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '_uv765EvWphL_')

CH_DATABASE_BRONZE = 'default'
CH_DATABASE_SILVER = 'track_silver'

JDBC_URL = f"jdbc:clickhouse:https://{CH_HOST}:{CH_PORT}/{CH_DATABASE_BRONZE}?ssl=true"

TABLES_TO_PROCESS = [
    "depara_cliente",  # Começar com tabela pequena para testar
    "sc5030",
    "sc6030",
    "sd2030",
    "sf2030",
    "tst_contratos"   # Maior tabela por último
]

NUM_PARTITIONS = 20
SAMPLE_MODE = True  # Testar com amostra primeiro
SAMPLE_SIZE = 1000

# ========================================
# FUNÇÕES AUXILIARES
# ========================================

def create_clickhouse_table(client, table_name: str, spark_schema, database: str):
    """
    Cria tabela ClickHouse com DDL correto (ORDER BY obrigatório)
    """
    # Mapear tipos Spark -> ClickHouse
    type_mapping = {
        'StringType': 'Nullable(String)',
        'IntegerType': 'Nullable(Int32)',
        'LongType': 'Nullable(Int64)',
        'DoubleType': 'Nullable(Float64)',
        'FloatType': 'Nullable(Float32)',
        'BooleanType': 'Nullable(UInt8)',
        'DateType': 'Nullable(Date)',
        'TimestampType': 'Nullable(DateTime)',
        'ShortType': 'Nullable(Int16)',
        'ByteType': 'Nullable(Int8)'
    }

    # Gerar colunas DDL e identificar ORDER BY cols
    columns_ddl = []
    order_by_cols = []
    order_by_col_names = []

    for field in spark_schema.fields:
        col_name = field.name
        spark_type = type(field.dataType).__name__
        ch_type = type_mapping.get(spark_type, 'Nullable(String)')

        # Para colunas ORDER BY: usar tipo NOT NULL (sem Nullable)
        # Usar primeiras 3 colunas não-metadata
        if not col_name.startswith('_') and len(order_by_col_names) < 3:
            order_by_col_names.append(col_name)
            # Tipo NOT NULL para ORDER BY
            ch_type_not_null = ch_type.replace('Nullable(', '').replace(')', '')
            columns_ddl.append(f"    `{col_name}` {ch_type_not_null}")
            order_by_cols.append(f"`{col_name}`")
        else:
            # Outras colunas podem ser Nullable
            columns_ddl.append(f"    `{col_name}` {ch_type}")

    # Se não tiver colunas, usar primeira coluna
    if not order_by_cols:
        first_col = spark_schema.fields[0].name
        order_by_cols = [f"`{first_col}`"]

    # DDL completo
    columns_str = ',\n'.join(columns_ddl)
    order_by_str = ', '.join(order_by_cols)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table_name} (
{columns_str}
    ) ENGINE = MergeTree()
    ORDER BY ({order_by_str})
    """

    print(f"   📝 ORDER BY: ({', '.join(order_by_cols)})")
    client.command(ddl)

    return order_by_cols


def process_table_spark(
    spark: SparkSession,
    client: clickhouse_connect.driver.Client,
    table_name: str,
    num_partitions: int = 20,
    sample_mode: bool = False,
    sample_size: int = 1000
) -> Dict[str, Any]:
    """
    Processa tabela Bronze → Silver com Spark
    """
    execution_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()

    print(f"\n{'='*80}")
    print(f"🔄 {table_name}")
    print(f"{'='*80}")
    print(f"ID: {execution_id} | Modo: {'AMOSTRA' if sample_mode else 'COMPLETO'}")

    try:
        # ========================================
        # 1. LEITURA SPARK JDBC
        # ========================================
        print(f"\n📥 [1/4] Lendo Bronze...")

        jdbc_options = {
            "url": JDBC_URL,
            "dbtable": f"{CH_DATABASE_BRONZE}.{table_name}",
            "user": CH_USER,
            "password": CH_PASSWORD,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "fetchsize": "10000",
            "numPartitions": str(num_partitions),
        }

        df_spark = spark.read.format("jdbc").options(**jdbc_options).load()

        if sample_mode:
            df_spark = df_spark.limit(sample_size)

        df_spark.cache()
        rows_input = df_spark.count()
        num_parts = df_spark.rdd.getNumPartitions()

        print(f"   ✅ {rows_input:,} linhas | {num_parts} partições")

        # ========================================
        # 2. TRANSFORMAÇÕES SPARK
        # ========================================
        print(f"\n🔧 [2/4] Transformando...")

        # Remover duplicatas
        df_clean = df_spark.dropDuplicates()
        rows_after_dedup = df_clean.count()
        rows_dups = rows_input - rows_after_dedup
        print(f"   🧹 Duplicatas: {rows_dups:,}")

        # Padronizar strings
        string_cols = [f.name for f in df_clean.schema.fields
                      if isinstance(f.dataType, StringType)][:10]
        for col in string_cols:
            df_clean = df_clean.withColumn(col, F.trim(F.upper(F.col(col))))
        print(f"   ✨ {len(string_cols)} colunas padronizadas")

        # Metadados
        df_clean = df_clean \
            .withColumn("_execution_id", F.lit(execution_id)) \
            .withColumn("_silver_ingestion_timestamp", F.current_timestamp()) \
            .withColumn("_silver_processing_date", F.current_date()) \
            .withColumn("_data_quality_flag", F.lit("VALIDATED")) \
            .withColumn("_bronze_schema", F.lit(CH_DATABASE_BRONZE)) \
            .withColumn("_silver_schema", F.lit(CH_DATABASE_SILVER))

        rows_output = df_clean.count()
        print(f"   ✅ {rows_output:,} linhas prontas")

        # ========================================
        # 3. CRIAR TABELA CLICKHOUSE (FIX!)
        # ========================================
        print(f"\n💾 [3/4] Criando tabela Silver...")

        silver_table_full = f"{CH_DATABASE_SILVER}.{table_name}"

        # Dropar tabela antiga
        try:
            client.command(f"DROP TABLE IF EXISTS {silver_table_full}")
        except:
            pass

        # Criar tabela com ORDER BY correto
        order_by_cols = create_clickhouse_table(
            client,
            table_name,
            df_clean.schema,
            CH_DATABASE_SILVER
        )
        print(f"   ✅ Tabela criada")

        # ========================================
        # 4. INSERIR DADOS COM SPARK JDBC
        # ========================================
        print(f"   💾 Inserindo dados...")

        # Escrever dados (tabela já existe, só inserir)
        df_clean.write \
            .format("jdbc") \
            .option("url", JDBC_URL.replace(CH_DATABASE_BRONZE, CH_DATABASE_SILVER)) \
            .option("dbtable", table_name) \
            .option("user", CH_USER) \
            .option("password", CH_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .option("isolationLevel", "NONE") \
            .option("createTableOptions", "") \
            .mode("append") \
            .save()

        print(f"   ✅ {rows_output:,} linhas inseridas")

        # Liberar cache
        df_spark.unpersist()

        # ========================================
        # 5. MÉTRICAS
        # ========================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        throughput = rows_output / duration if duration > 0 else 0

        print(f"\n✅ SUCESSO!")
        print(f"   Duração: {duration:.2f}s | Throughput: {throughput:,.0f} rows/s")

        return {
            'execution_id': execution_id,
            'table_name': table_name,
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration,
            'rows_input': rows_input,
            'rows_output': rows_output,
            'rows_duplicates': rows_dups,
            'num_partitions': num_parts,
            'throughput_rows_per_sec': throughput,
            'status': 'success',
            'error_message': ''
        }

    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            'execution_id': execution_id,
            'table_name': table_name,
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration,
            'rows_input': 0,
            'rows_output': 0,
            'rows_duplicates': 0,
            'num_partitions': 0,
            'throughput_rows_per_sec': 0,
            'status': 'failed',
            'error_message': str(e)[:500]
        }


# ========================================
# MAIN
# ========================================

def main():
    print("="*80)
    print("🚀 SPARK SILVER PIPELINE - VERSÃO CORRIGIDA")
    print("="*80)
    print(f"Modo: {'AMOSTRA' if SAMPLE_MODE else 'COMPLETO'}")
    print(f"Sample: {SAMPLE_SIZE if SAMPLE_MODE else 'N/A'}")
    print(f"Partições: {NUM_PARTITIONS}")
    print(f"Tabelas: {len(TABLES_TO_PROCESS)}")
    print("="*80)

    # Inicializar Spark
    print("\n⚡ Inicializando Spark...")
    spark = SparkSession.builder \
        .appName("SilverLayer-Fixed") \
        .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "100") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark {spark.version} inicializado")

    # Conectar ClickHouse
    print(f"\n🔌 Conectando ClickHouse...")
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        secure=True
    )

    version = client.query("SELECT version()").result_rows[0][0]
    print(f"✅ ClickHouse {version}")

    # Criar database
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE_SILVER}")

    # Processar tabelas
    all_metrics = []

    for idx, table in enumerate(TABLES_TO_PROCESS, 1):
        print(f"\n[{idx}/{len(TABLES_TO_PROCESS)}] {table}")

        metrics = process_table_spark(
            spark=spark,
            client=client,
            table_name=table,
            num_partitions=NUM_PARTITIONS,
            sample_mode=SAMPLE_MODE,
            sample_size=SAMPLE_SIZE
        )

        all_metrics.append(metrics)

    # Resumo
    df_metrics = pd.DataFrame(all_metrics)

    print(f"\n{'='*80}")
    print("📊 RESUMO FINAL")
    print(f"{'='*80}")

    success = len([m for m in all_metrics if m['status'] == 'success'])
    failed = len([m for m in all_metrics if m['status'] == 'failed'])

    print(f"Tabelas processadas: {len(all_metrics)}")
    print(f"✅ Sucesso: {success}")
    print(f"❌ Falhas: {failed}")

    if success > 0:
        total_rows = df_metrics[df_metrics['status'] == 'success']['rows_output'].sum()
        total_duration = df_metrics[df_metrics['status'] == 'success']['duration_seconds'].sum()
        avg_throughput = df_metrics[df_metrics['status'] == 'success']['throughput_rows_per_sec'].mean()

        print(f"\nTotal linhas processadas: {total_rows:,}")
        print(f"Duração total: {total_duration:.2f}s ({total_duration/60:.2f} min)")
        print(f"Throughput médio: {avg_throughput:,.0f} rows/s")

        print("\n📋 Detalhes:")
        print(df_metrics[['table_name', 'rows_output', 'duration_seconds', 'throughput_rows_per_sec', 'status']].to_string(index=False))

    print("="*80)

    # Cleanup
    spark.stop()
    print("\n✅ Pipeline concluído!")


if __name__ == "__main__":
    main()
