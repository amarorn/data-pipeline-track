"""
Pipeline Snapshot + Hash + Delta: Oracle -> Bronze -> Silver -> Gold.
Executa ingestao, normalizacao + hash e computacao de deltas no ClickHouse.
Config: apps/orchestrator/configs/snapshot_pipeline.yaml
"""
import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

import yaml

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    UTC = None

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.chdir(_PROJECT_ROOT)

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window

CONFIG_PATH = Path(__file__).parent / "configs" / "snapshot_pipeline.yaml"


def _load_config(path: Path) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _ref_date_from_config(config: Dict) -> date:
    ref = config.get("ref_date_source")
    if ref == "env":
        env_key = config.get("ref_date_env", "REF_DATE")
        val = _env(env_key)
        if val:
            return datetime.strptime(val, "%Y-%m-%d").date()
    return date.today()


def _get_spark(oracle_jar: Optional[str] = None, clickhouse_jar: Optional[str] = None) -> SparkSession:
    jars = []
    if oracle_jar and os.path.isfile(oracle_jar):
        jars.append(oracle_jar)
    if clickhouse_jar and os.path.isfile(clickhouse_jar):
        jars.append(clickhouse_jar)
    if not jars:
        packages = "com.clickhouse:clickhouse-jdbc:0.4.6"
        builder = SparkSession.builder.config("spark.jars.packages", packages)
    else:
        builder = SparkSession.builder.config("spark.jars", ",".join(jars))

    spark = (
        builder.appName("SnapshotHashDelta-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", _env("SPARK_SQL_SHUFFLE_PARTITIONS", "100"))
        .config("spark.driver.memory", _env("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.executor.memory", _env("SPARK_EXECUTOR_MEMORY", "4g"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _ch_client():
    return clickhouse_connect.get_client(
        host=_env("CLICKHOUSE_HOST"),
        port=int(_env("CLICKHOUSE_PORT", "8443")),
        username=_env("CLICKHOUSE_USER", "default"),
        password=_env("CLICKHOUSE_PASSWORD"),
        secure=_env("CLICKHOUSE_SECURE", "true").lower() == "true",
    )


def _ensure_databases(client, config: Dict) -> None:
    for key in ("db_bronze", "db_silver", "db_gold"):
        db = config.get("clickhouse", {}).get(key, key)
        client.command(f"CREATE DATABASE IF NOT EXISTS {db}")


def _spark_to_ch_type(spark_type) -> str:
    if isinstance(spark_type, StringType):
        return "Nullable(String)"
    if isinstance(spark_type, IntegerType):
        return "Nullable(Int32)"
    if isinstance(spark_type, LongType):
        return "Nullable(Int64)"
    if isinstance(spark_type, DoubleType):
        return "Nullable(Float64)"
    if isinstance(spark_type, DateType):
        return "Nullable(Date)"
    if isinstance(spark_type, TimestampType):
        return "Nullable(DateTime64(3))"
    return "Nullable(String)"


def run_bronze(
    spark: SparkSession,
    client: Any,
    config: Dict,
    ref_date: date,
    table_config: Dict,
    jdbc_props: Dict,
) -> Dict[str, Any]:
    pipeline_name = table_config["pipeline_name"]
    oracle_schema = table_config["oracle"]["schema"]
    oracle_table = table_config["oracle"]["table"]
    dbtable = f"{oracle_schema}.{oracle_table}"
    bronze_table = f"{pipeline_name.lower()}_bronze_snapshot"
    ch_db = config["clickhouse"].get("db_bronze", "track_bronze")

    df = spark.read.format("jdbc").options(**jdbc_props).option("dbtable", dbtable).load()
    row_count = df.count()
    if row_count == 0:
        return {"table_name": bronze_table, "ref_date": ref_date, "row_count": 0, "status": "skipped_empty"}

    df_bronze = df.withColumn("_ref_date", F.lit(ref_date)).withColumn(
        "_bronze_ingestion_timestamp", F.current_timestamp()
    )

    try:
        client.command(f"ALTER TABLE {ch_db}.{bronze_table} DELETE WHERE _ref_date = '{ref_date}'")
    except Exception:
        pass

    try:
        client.command(f"SELECT 1 FROM {ch_db}.{bronze_table} LIMIT 1")
    except Exception:
        col_defs = []
        for field in df_bronze.schema.fields:
            ch_type = _spark_to_ch_type(field.dataType)
            if field.name == "_ref_date":
                ch_type = "Date"
            elif field.name == "_bronze_ingestion_timestamp":
                ch_type = "DateTime64(3)"
            col_defs.append(f"`{field.name}` {ch_type}")
        client.command(
            f"CREATE TABLE {ch_db}.{bronze_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() PARTITION BY _ref_date ORDER BY (_ref_date, _bronze_ingestion_timestamp)"
        )

    jdbc_url_ch = f"jdbc:clickhouse://{_env('CLICKHOUSE_HOST')}:{_env('CLICKHOUSE_PORT', '8443')}/{ch_db}?ssl={_env('CLICKHOUSE_SECURE', 'true')}"
    df_bronze.write.jdbc(
        url=jdbc_url_ch,
        table=bronze_table,
        mode="append",
        properties={
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "user": _env("CLICKHOUSE_USER", "default"),
            "password": _env("CLICKHOUSE_PASSWORD"),
            "batchsize": "100000",
        },
    )
    return {"table_name": bronze_table, "ref_date": ref_date, "row_count": row_count, "status": "success"}


def run_silver(
    spark: SparkSession,
    client: Any,
    config: Dict,
    ref_date: date,
    table_config: Dict,
) -> Dict[str, Any]:
    pipeline_name = table_config["pipeline_name"]
    primary_keys = list(table_config.get("primary_keys") or [])
    bronze_table = f"{pipeline_name.lower()}_bronze_snapshot"
    silver_table = f"{pipeline_name.lower()}_silver_snapshot"
    ch_db_bronze = config["clickhouse"].get("db_bronze", "track_bronze")
    ch_db_silver = config["clickhouse"].get("db_silver", "track_silver")

    jdbc_bronze = f"jdbc:clickhouse://{_env('CLICKHOUSE_HOST')}:{_env('CLICKHOUSE_PORT', '8443')}/{ch_db_bronze}?ssl={_env('CLICKHOUSE_SECURE', 'true')}"
    jdbc_silver = f"jdbc:clickhouse://{_env('CLICKHOUSE_HOST')}:{_env('CLICKHOUSE_PORT', '8443')}/{ch_db_silver}?ssl={_env('CLICKHOUSE_SECURE', 'true')}"
    jdbc_props = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": _env("CLICKHOUSE_USER", "default"),
        "password": _env("CLICKHOUSE_PASSWORD"),
        "ssl": "true",
    }

    df_bronze = (
        spark.read.jdbc(url=jdbc_bronze, table=ch_db_bronze + "." + bronze_table, properties=jdbc_props)
        .filter(F.col("_ref_date") == F.lit(ref_date))
    )
    row_count_bronze = df_bronze.count()
    if row_count_bronze == 0:
        return {"table_name": silver_table, "ref_date": ref_date, "row_count": 0, "status": "skipped_empty"}

    technical_cols = ["_ref_date", "_bronze_ingestion_timestamp"]
    all_cols = [c for c in df_bronze.columns if c not in technical_cols]
    business_columns = all_cols
    if not primary_keys and all_cols:
        primary_keys = [all_cols[0]]

    df_normalized = df_bronze
    for col_name in business_columns:
        if dict(df_bronze.dtypes).get(col_name) == "string":
            df_normalized = df_normalized.withColumn(
                col_name, F.trim(F.upper(F.col(col_name)))
            )

    hash_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted(business_columns)])
    df_silver = (
        df_normalized.withColumn("row_hash", F.sha2(hash_expr, 256))
        .withColumn("ref_date", F.col("_ref_date"))
        .withColumn("_silver_processing_timestamp", F.current_timestamp())
    )
    final_columns = primary_keys + business_columns + ["ref_date", "row_hash", "_silver_processing_timestamp"]
    final_columns = list(dict.fromkeys(final_columns))
    df_silver = df_silver.select(*final_columns)

    window_spec = Window.partitionBy(*primary_keys).orderBy(F.desc("_silver_processing_timestamp"))
    df_silver = df_silver.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")
    row_count_silver = df_silver.count()

    try:
        client.command(f"ALTER TABLE {ch_db_silver}.{silver_table} DELETE WHERE ref_date = '{ref_date}'")
    except Exception:
        pass

    try:
        client.command(f"SELECT 1 FROM {ch_db_silver}.{silver_table} LIMIT 1")
    except Exception:
        col_defs = []
        for field in df_silver.schema.fields:
            col_defs.append(f"`{field.name}` {_spark_to_ch_type(field.dataType)}")
        pk_order = ", ".join(primary_keys)
        client.command(
            f"CREATE TABLE {ch_db_silver}.{silver_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() PARTITION BY ref_date ORDER BY (ref_date, {pk_order})"
        )

    df_silver.write.jdbc(url=jdbc_silver, table=silver_table, mode="append", properties={**jdbc_props, "batchsize": "100000"})
    return {"table_name": silver_table, "ref_date": ref_date, "row_count": row_count_silver, "status": "success"}


def run_gold(
    spark: SparkSession,
    client: Any,
    config: Dict,
    ref_date_current: date,
    ref_date_previous: date,
    table_config: Dict,
) -> Dict[str, Any]:
    pipeline_name = table_config["pipeline_name"]
    primary_keys = list(table_config.get("primary_keys") or [])
    silver_table = f"{pipeline_name.lower()}_silver_snapshot"
    delta_table = f"{pipeline_name.lower()}_gold_deltas"
    ch_db_silver = config["clickhouse"].get("db_silver", "track_silver")
    ch_db_gold = config["clickhouse"].get("db_gold", "track_gold")

    jdbc_silver = f"jdbc:clickhouse://{_env('CLICKHOUSE_HOST')}:{_env('CLICKHOUSE_PORT', '8443')}/{ch_db_silver}?ssl={_env('CLICKHOUSE_SECURE', 'true')}"
    jdbc_gold = f"jdbc:clickhouse://{_env('CLICKHOUSE_HOST')}:{_env('CLICKHOUSE_PORT', '8443')}/{ch_db_gold}?ssl={_env('CLICKHOUSE_SECURE', 'true')}"
    jdbc_props = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": _env("CLICKHOUSE_USER", "default"),
        "password": _env("CLICKHOUSE_PASSWORD"),
        "ssl": "true",
    }

    df_current = (
        spark.read.jdbc(url=jdbc_silver, table=ch_db_silver + "." + silver_table, properties=jdbc_props)
        .filter(F.col("ref_date") == F.lit(ref_date_current))
    )
    df_previous = (
        spark.read.jdbc(url=jdbc_silver, table=ch_db_silver + "." + silver_table, properties=jdbc_props)
        .filter(F.col("ref_date") == F.lit(ref_date_previous))
    )
    count_prev = df_previous.count()
    if count_prev == 0:
        return {
            "table_name": delta_table,
            "ref_date_current": ref_date_current,
            "ref_date_previous": ref_date_previous,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "total_deltas": 0,
            "status": "no_previous_snapshot",
        }

    if not primary_keys:
        cols = [c for c in df_current.columns if c not in ("row_hash", "ref_date", "_silver_processing_timestamp")]
        primary_keys = [cols[0]] if cols else []

    pk_cols = primary_keys
    df_cur = df_current.select(*pk_cols, "row_hash").withColumnRenamed("row_hash", "hash_current")
    df_prev = df_previous.select(*pk_cols, "row_hash").withColumnRenamed("row_hash", "hash_previous")
    df_comp = df_cur.join(df_prev, on=pk_cols, how="full_outer")
    df_deltas = (
        df_comp.withColumn(
            "operation",
            F.when(
                (F.col("hash_current").isNotNull()) & (F.col("hash_previous").isNull()),
                F.lit("I"),
            )
            .when(
                (F.col("hash_current").isNull()) & (F.col("hash_previous").isNotNull()),
                F.lit("D"),
            )
            .when(
                (F.col("hash_current").isNotNull())
                & (F.col("hash_previous").isNotNull())
                & (F.col("hash_current") != F.col("hash_previous")),
                F.lit("U"),
            )
            .otherwise(F.lit("N"))
        )
        .filter(F.col("operation") != "N")
    )
    df_deltas_full = (
        df_deltas.join(df_current, on=pk_cols, how="left")
        .withColumn("delta_date", F.lit(ref_date_current))
        .withColumn("delta_timestamp", F.current_timestamp())
    )
    total_deltas = df_deltas_full.count()
    if total_deltas == 0:
        return {
            "table_name": delta_table,
            "ref_date_current": ref_date_current,
            "ref_date_previous": ref_date_previous,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "total_deltas": 0,
            "status": "no_changes",
        }

    try:
        client.command(f"ALTER TABLE IF EXISTS {ch_db_gold}.{delta_table} DELETE WHERE delta_date = '{ref_date_current}'")
    except Exception:
        pass

    cols_to_write = [
        c for c in df_deltas_full.columns
        if c not in ("hash_current", "hash_previous", "ref_date", "_silver_processing_timestamp")
    ]
    try:
        client.command(f"SELECT 1 FROM {ch_db_gold}.{delta_table} LIMIT 1")
    except Exception:
        col_defs = []
        for field in df_deltas_full.schema.fields:
            if field.name in ("hash_current", "hash_previous", "ref_date", "_silver_processing_timestamp"):
                continue
            col_defs.append(f"`{field.name}` {_spark_to_ch_type(field.dataType)}")
        pk_order = ", ".join(pk_cols)
        client.command(
            f"CREATE TABLE {ch_db_gold}.{delta_table} ({', '.join(col_defs)}) "
            f"ENGINE = MergeTree() PARTITION BY delta_date ORDER BY (delta_date, operation, {pk_order})"
        )

    df_deltas_full.select(*cols_to_write).write.jdbc(
        url=jdbc_gold, table=delta_table, mode="append", properties={**jdbc_props, "batchsize": "100000"}
    )
    stats = df_deltas_full.groupBy("operation").count().collect()
    delta_stats = {r["operation"]: r["count"] for r in stats}
    return {
        "table_name": delta_table,
        "ref_date_current": ref_date_current,
        "ref_date_previous": ref_date_previous,
        "inserts": delta_stats.get("I", 0),
        "updates": delta_stats.get("U", 0),
        "deletes": delta_stats.get("D", 0),
        "total_deltas": total_deltas,
        "status": "success",
    }


def run_pipeline(
    layer: str = "all",
    config_path: Optional[Path] = None,
    ref_date_str: Optional[str] = None,
    table_filter: Optional[str] = None,
) -> None:
    config = _load_config(config_path or CONFIG_PATH)
    if ref_date_str:
        ref_date = datetime.strptime(ref_date_str, "%Y-%m-%d").date()
    else:
        ref_date = _ref_date_from_config(config)
    ref_date_previous = ref_date - timedelta(days=1)

    tables = config.get("tables", [])
    if table_filter:
        tables = [t for t in tables if t["pipeline_name"] == table_filter]
    if not tables:
        print("Nenhuma tabela para processar")
        return

    client = _ch_client()
    _ensure_databases(client, config)

    oracle_jdbc = {
        "url": f"jdbc:oracle:thin:@//{_env('ORACLE_HOST')}:{_env('ORACLE_PORT', '1521')}/{_env('ORACLE_SERVICE')}",
        "user": _env("ORACLE_USER"),
        "password": _env("ORACLE_PASSWORD"),
        "driver": "oracle.jdbc.OracleDriver",
    }

    spark = _get_spark()
    layers = ["bronze", "silver", "gold"] if layer == "all" else [layer]

    for table_cfg in tables:
        name = table_cfg["pipeline_name"]
        print(f"\n{'='*80}\nTabela: {name}\n{'='*80}")
        try:
            if "bronze" in layers:
                run_bronze(spark, client, config, ref_date, table_cfg, oracle_jdbc)
            if "silver" in layers:
                run_silver(spark, client, config, ref_date, table_cfg)
            if "gold" in layers and table_cfg.get("primary_keys"):
                run_gold(spark, client, config, ref_date, ref_date_previous, table_cfg)
        except Exception as e:
            print(f"Erro em {name}: {e}")
            raise

    spark.stop()
    print("\nPipeline concluido.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline Snapshot + Hash + Delta")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH, help="Path to snapshot_pipeline.yaml")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    parser.add_argument("--ref-date", type=str, default=None, help="YYYY-MM-DD (default: today or REF_DATE env)")
    parser.add_argument("--table", type=str, default=None, help="Run only this pipeline_name")
    args = parser.parse_args()
    run_pipeline(
        layer=args.layer,
        config_path=args.config,
        ref_date_str=args.ref_date,
        table_filter=args.table,
    )


if __name__ == "__main__":
    main()
