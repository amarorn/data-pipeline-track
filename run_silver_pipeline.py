#!/usr/bin/env python3
"""
Pipeline Silver Layer - Bronze to Silver transformation
Executa transformação de dados usando pandas (sem Spark)
"""

import clickhouse_connect
import pandas as pd
import numpy as np
from datetime import datetime
import uuid
from dataclasses import dataclass
import sys

# Configurações ClickHouse
CH_HOST = 'e1a1lieug8.us-central1.gcp.clickhouse.cloud'
CH_PORT = 8443
CH_USER = 'default'
CH_PASSWORD = '_uv765EvWphL_'
CH_DATABASE_BRONZE = 'default'
CH_DATABASE_SILVER = 'track_silver'

# Configurações do pipeline
TABLES_TO_PROCESS = [
    "tst_contratos",
    "depara_cliente",
    "sc5030",
    "sc6030",
    "sd2030",
    "sf2030",
]

SAMPLE_MODE = True
SAMPLE_SIZE = 10000

@dataclass
class PipelineMetrics:
    table_name: str
    start_time: datetime
    rows_input: int = 0
    rows_output: int = 0
    rows_duplicates: int = 0
    duration_seconds: float = 0.0
    throughput_rows_per_sec: float = 0.0
    quality_score: float = 0.0
    status: str = "running"
    error_message: str = None

    def finalize(self):
        end_time = datetime.now()
        self.duration_seconds = (end_time - self.start_time).total_seconds()
        if self.duration_seconds > 0:
            self.throughput_rows_per_sec = self.rows_output / self.duration_seconds


def process_table(client, table_name, sample_size=None):
    """Processa uma tabela Bronze → Silver"""

    execution_id = str(uuid.uuid4())[:8]
    metrics = PipelineMetrics(table_name=table_name, start_time=datetime.now())

    print(f"\n{'='*80}")
    print(f"🔄 {table_name}")
    print(f"{'='*80}")

    try:
        # 1. Ler Bronze
        query = f"SELECT * FROM {CH_DATABASE_BRONZE}.{table_name}"
        if sample_size:
            query += f" ORDER BY rand() LIMIT {sample_size}"

        print(f"📥 Lendo Bronze...")
        df = client.query_df(query)
        metrics.rows_input = len(df)
        print(f"   ✅ {metrics.rows_input:,} linhas")

        # 2. Transformar
        print(f"🔧 Transformando...")

        # Duplicatas
        initial = len(df)
        df = df.drop_duplicates()
        metrics.rows_duplicates = initial - len(df)
        print(f"   🧹 {metrics.rows_duplicates:,} duplicatas")

        # Limpar NAs
        df = df.replace({pd.NA: None, np.nan: None})

        # Padronizar strings (primeiras 5 colunas)
        str_cols = df.select_dtypes(include=['object']).columns[:5]
        for col in str_cols:
            try:
                df[col] = df[col].str.strip().str.upper()
            except:
                pass

        metrics.rows_output = len(df)

        # Qualidade
        completeness = (df.notna().sum() / len(df)).mean() * 100
        metrics.quality_score = completeness

        # 3. Criar Silver
        print(f"💾 Criando Silver...")

        # DDL com Nullable
        type_map = {
            'int64': 'Nullable(Int64)',
            'float64': 'Nullable(Float64)',
            'object': 'Nullable(String)',
            'datetime64[ns]': 'Nullable(DateTime64)',
            'bool': 'Nullable(UInt8)'
        }

        col_defs = [f"`{col}` {type_map.get(str(df[col].dtype), 'Nullable(String)')}"
                    for col in df.columns]

        silver_table = f"{CH_DATABASE_SILVER}.{table_name}"

        # Dropar e criar
        client.command(f"DROP TABLE IF EXISTS {silver_table}")
        client.command(f"CREATE TABLE {silver_table} ({', '.join(col_defs)}) ENGINE = MergeTree() ORDER BY tuple()")

        # Inserir
        client.insert_df(silver_table, df)
        print(f"   ✅ {metrics.rows_output:,} linhas em Silver")

        # 4. Métricas
        print(f"📊 Métricas...")

        metrics.status = "success"
        metrics.finalize()

        # Quality metrics
        quality_data = pd.DataFrame([{
            'table_name': table_name,
            'execution_id': execution_id,
            'execution_timestamp': datetime.now(),
            'total_rows_input': metrics.rows_input,
            'total_rows_output': metrics.rows_output,
            'rows_duplicates': metrics.rows_duplicates,
            'rows_invalid': 0,
            'rows_nulls': int(df.isna().sum().sum()),
            'completeness_score': metrics.quality_score,
            'quality_score': metrics.quality_score,
            'checks_passed': 1,
            'checks_failed': 0,
            'issues': '[]'
        }])
        client.insert_df(f"{CH_DATABASE_SILVER}.quality_metrics", quality_data)

        # Performance metrics
        perf_data = pd.DataFrame([{
            'table_name': table_name,
            'execution_id': execution_id,
            'execution_timestamp': datetime.now(),
            'start_time': metrics.start_time,
            'end_time': datetime.now(),
            'duration_seconds': metrics.duration_seconds,
            'rows_processed': metrics.rows_output,
            'throughput_rows_per_sec': metrics.throughput_rows_per_sec,
            'spark_partitions': 0,
            'memory_used_mb': 0.0,
            'status': metrics.status
        }])
        client.insert_df(f"{CH_DATABASE_SILVER}.performance_metrics", perf_data)

        print(f"✅ SUCESSO!")
        print(f"   Input: {metrics.rows_input:,} | Output: {metrics.rows_output:,}")
        print(f"   Duplicatas: {metrics.rows_duplicates:,} | {metrics.duration_seconds:.2f}s")
        print(f"   Throughput: {metrics.throughput_rows_per_sec:.0f} rows/s | Quality: {metrics.quality_score:.1f}%")

        return metrics

    except Exception as e:
        print(f"❌ ERRO: {e}")
        import traceback
        traceback.print_exc()

        metrics.status = "failed"
        metrics.error_message = str(e)
        metrics.finalize()

        return metrics


def main():
    """Executa pipeline completo"""

    print(f"\n{'='*80}")
    print(f"🚀 PIPELINE SILVER LAYER - Bronze → Silver")
    print(f"{'='*80}")
    print(f"Bronze: {CH_DATABASE_BRONZE}")
    print(f"Silver: {CH_DATABASE_SILVER}")
    print(f"Tabelas: {len(TABLES_TO_PROCESS)}")
    if SAMPLE_MODE:
        print(f"Modo: AMOSTRA ({SAMPLE_SIZE:,} linhas por tabela)")
    else:
        print(f"Modo: PRODUÇÃO (dados completos)")
    print(f"{'='*80}\n")

    # Conectar
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD
    )

    # Processar tabelas
    all_metrics = []
    start_time = datetime.now()

    for idx, table in enumerate(TABLES_TO_PROCESS, 1):
        print(f"\n[{idx}/{len(TABLES_TO_PROCESS)}] Processando {table}...")

        sample = SAMPLE_SIZE if SAMPLE_MODE else None
        metrics = process_table(client, table, sample)
        all_metrics.append(metrics)

    # Resumo
    total_duration = (datetime.now() - start_time).total_seconds()
    success = [m for m in all_metrics if m.status == 'success']
    failed = [m for m in all_metrics if m.status == 'failed']

    print(f"\n{'='*80}")
    print(f"📊 RESUMO FINAL")
    print(f"{'='*80}")
    print(f"Total processado: {len(all_metrics)} tabelas")
    print(f"✅ Sucesso: {len(success)}")
    print(f"❌ Falhas: {len(failed)}")
    print(f"⏱️  Tempo total: {total_duration:.2f}s")

    if success:
        total_input = sum(m.rows_input for m in success)
        total_output = sum(m.rows_output for m in success)
        total_dups = sum(m.rows_duplicates for m in success)
        avg_quality = np.mean([m.quality_score for m in success])

        print(f"\nLinhas entrada: {total_input:,}")
        print(f"Linhas saída: {total_output:,}")
        print(f"Duplicatas removidas: {total_dups:,}")
        print(f"Quality score médio: {avg_quality:.1f}%")

    print(f"{'='*80}\n")

    if failed:
        print(f"⚠️  Tabelas com falha:")
        for m in failed:
            print(f"   - {m.table_name}: {m.error_message[:100]}")

    client.close()

    return 0 if len(failed) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
