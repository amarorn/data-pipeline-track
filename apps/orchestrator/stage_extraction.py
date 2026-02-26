import sys
import os
import socket
sys.path.insert(0, '/app')

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from connectors.clickhouse_client import ClickHouseClient
from config.settings import oracle_config, clickhouse_config, spark_config

schemas_to_extract = [
    "ADMBI_PRD",
    "ALLSYSTEM_PROD",
    "BISTAGE",
    "CCTRACKERAVV",
    "CLICKHOUSE",
    "COMANDO",
    "CONSULTA",
    "DB_TESTE",
    "DIP",
    "DOUGLAS_FERREIRA",
    "DVF",
    "FERNANDO_RIBEIRO",
    "GINF",
    "HORUS",
    "HORUS_STAT",
    "IMPORT_USER",
    "LIVIADCORTE",
    "MDDATA",
    "MILLENA_PIVATO",
    "MPORTAL",
    "MULTIPORTAL",
    "NEXTAGE_PROD",
    "PDBUSER",
    "PERFSTAT",
    "RONALDO_ARIMURA",
    "SCOT",
    "SCOTGI",
    "SCOTSEQ",
    "SCOTT",
    "SCOT_BKP",
    "SCTHORUS",
    "SGTI",
    "SGTIGI",
    "SIGA",
    "SIGAGI",
    "SIGA_RO",
    "SUPORTEBD",
    "THIAGO_FERNANDES",
    "N8N_SERASA"
]

def check_oracle_connectivity():
    print("=" * 80)
    print("DIAGNÓSTICO DE CONECTIVIDADE ORACLE")
    print("=" * 80)
    print(f"Host: {oracle_config.host}")
    print(f"Port: {oracle_config.port}")
    print(f"Service: {oracle_config.service}")
    print(f"User: {oracle_config.user}")
    print()
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((oracle_config.host, int(oracle_config.port)))
        sock.close()
        
        if result == 0:
            print("✓ Conexão TCP estabelecida com sucesso")
            return True
        else:
            print(f"✗ Falha na conexão TCP: Connection refused (código {result})")
            print()
            print("POSSÍVEIS CAUSAS:")
            print("1. Servidor Oracle não está acessível a partir do container")
            print("2. Firewall bloqueando a porta 1521")
            print("3. Servidor Oracle não está rodando")
            print("4. Problemas de rede entre container e servidor Oracle")
            print()
            print("SOLUÇÕES:")
            print("1. Verifique se o servidor Oracle está rodando")
            print("2. Teste conectividade do host: telnet {} {}".format(oracle_config.host, oracle_config.port))
            print("3. Verifique regras de firewall")
            print("4. Se estiver em Docker, verifique network_mode e portas expostas")
            return False
    except socket.gaierror as e:
        print(f"✗ Erro de DNS: Não foi possível resolver o hostname '{oracle_config.host}'")
        print(f"  Erro: {str(e)}")
        return False
    except Exception as e:
        print(f"✗ Erro ao verificar conectividade: {str(e)}")
        return False
    finally:
        print("=" * 80)
        print()

def initialize_stage():
    client = ClickHouseClient()
    
    ddl_stage_database = "CREATE DATABASE stage"
    ddl_stage_metadata = """
    CREATE TABLE IF NOT EXISTS stage.extraction_metadata
    (
        extraction_id String,
        schema_name String,
        table_name String,
        ref_date Date,
        row_count UInt64,
        extraction_start DateTime,
        extraction_end DateTime,
        status String,
        error_message String,
        created_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ref_date)
    ORDER BY (ref_date, schema_name, table_name)
    """
    
    try:
        client.execute_query(ddl_stage_database)
        print("✓ Database 'stage' criado")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "exists" in error_msg.lower():
            print("✓ Database 'stage' já existe")
        else:
            print(f"⚠ Erro ao criar database (pode já existir): {error_msg[:200]}")
    
    try:
        client.execute_query(ddl_stage_metadata)
        print("✓ Tabela 'extraction_metadata' criada/verificada")
    except Exception as e:
        print(f"✗ Erro ao criar tabela extraction_metadata: {str(e)}")
    finally:
        client.close()

def get_tables_from_schema(spark, schema_name, oracle_jdbc_url):
    query = f"""
    (SELECT table_name 
     FROM all_tables 
     WHERE owner = '{schema_name}' 
     AND table_name NOT LIKE 'BIN$%'
     ORDER BY table_name)
    """
    
    try:
        df_tables = (
            spark.read.format("jdbc")
            .option("url", oracle_jdbc_url)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("dbtable", query)
            .option("user", oracle_config.user)
            .option("password", oracle_config.password)
            .option("fetchsize", "1000")
            .load()
        )
        return [row.TABLE_NAME for row in df_tables.collect()]
    except Exception as e:
        error_str = str(e)
        if "Connection refused" in error_str or "Network Adapter" in error_str:
            print(f"✗ Erro de conectividade ao listar tabelas do schema {schema_name}")
            print(f"  Host: {oracle_config.host}:{oracle_config.port}")
            print(f"  Verifique se o servidor Oracle está acessível")
        else:
            print(f"✗ Erro ao listar tabelas do schema {schema_name}: {error_str[:200]}")
        return []

def create_stage_table(schema_name, table_name, df_sample):
    stage_table_name = f"stage.{schema_name.lower()}_{table_name.lower()}"
    
    columns = df_sample.columns
    dtypes = df_sample.dtypes
    
    type_mapping = {
        'string': 'String',
        'int': 'Int64',
        'bigint': 'Int64',
        'double': 'Float64',
        'float': 'Float64',
        'decimal': 'Decimal64(2)',
        'date': 'Date',
        'timestamp': 'DateTime',
        'boolean': 'UInt8'
    }
    
    clickhouse_columns = []
    for col_name, dtype in zip(columns, dtypes):
        base_type = dtype.split('(')[0].lower()
        ch_type = type_mapping.get(base_type, 'String')
        clickhouse_columns.append(f"`{col_name}` {ch_type}")
    
    clickhouse_columns.append("`ref_date` Date")
    clickhouse_columns.append("`ingestion_timestamp` DateTime DEFAULT now()")
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {stage_table_name}
    (
        {', '.join(clickhouse_columns)}
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ref_date)
    ORDER BY (ref_date, ingestion_timestamp)
    """
    
    return ddl, stage_table_name

def extract_schema_to_stage(spark, schema_name, ref_date, oracle_jdbc_url, clickhouse_jdbc_url):
    extraction_id = f"{schema_name}_{ref_date}_{datetime.now().strftime('%H%M%S')}"
    extraction_start = datetime.now()
    
    print(f"\n{'='*80}")
    print(f"Extraindo schema: {schema_name}")
    print(f"Extraction ID: {extraction_id}")
    print(f"{'='*80}")
    
    try:
        tables = get_tables_from_schema(spark, schema_name, oracle_jdbc_url)
        print(f"✓ {len(tables)} tabelas encontradas no schema {schema_name}")
        
        if not tables:
            return
        
        client = ClickHouseClient()
        
        for table_name in tables:
            try:
                print(f"\n  Processando tabela: {table_name}")
                
                df_oracle = (
                    spark.read.format("jdbc")
                    .option("url", oracle_jdbc_url)
                    .option("driver", "oracle.jdbc.OracleDriver")
                    .option("dbtable", f"{schema_name}.{table_name}")
                    .option("user", oracle_config.user)
                    .option("password", oracle_config.password)
                    .option("fetchsize", "10000")
                    .option("numPartitions", "10")
                    .load()
                )
                
                row_count = df_oracle.count()
                print(f"    ✓ {row_count} registros lidos do Oracle")
                
                if row_count == 0:
                    print(f"    ⚠ Tabela vazia, pulando...")
                    continue
                
                df_stage = df_oracle.withColumn("ref_date", lit(ref_date).cast("date"))
                
                ddl, stage_table_name = create_stage_table(schema_name, table_name, df_stage)
                client.execute_query(ddl)
                print(f"    ✓ Tabela stage criada: {stage_table_name}")
                
                (
                    df_stage.write
                    .format("jdbc")
                    .option("url", clickhouse_jdbc_url)
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                    .option("dbtable", stage_table_name)
                    .option("user", clickhouse_config.user)
                    .option("password", clickhouse_config.password)
                    .option("batchsize", "50000")
                    .mode("append")
                    .save()
                )
                
                print(f"    ✓ Dados inseridos na stage: {row_count} registros")
                
                extraction_end = datetime.now()
                
                metadata_insert = f"""
                INSERT INTO stage.extraction_metadata
                (extraction_id, schema_name, table_name, ref_date, row_count, 
                 extraction_start, extraction_end, status, error_message)
                VALUES
                ('{extraction_id}', '{schema_name}', '{table_name}', 
                 toDate('{ref_date}'), {row_count},
                 '{extraction_start.strftime('%Y-%m-%d %H:%M:%S')}',
                 '{extraction_end.strftime('%Y-%m-%d %H:%M:%S')}',
                 'SUCCESS', '')
                """
                client.execute_query(metadata_insert)
                
            except Exception as e:
                error_msg = str(e)[:500].replace("'", "''")
                print(f"    ✗ Erro ao processar {table_name}: {error_msg}")
                
                extraction_end = datetime.now()
                metadata_insert = f"""
                INSERT INTO stage.extraction_metadata
                (extraction_id, schema_name, table_name, ref_date, row_count,
                 extraction_start, extraction_end, status, error_message)
                VALUES
                ('{extraction_id}', '{schema_name}', '{table_name}',
                 toDate('{ref_date}'), 0,
                 '{extraction_start.strftime('%Y-%m-%d %H:%M:%S')}',
                 '{extraction_end.strftime('%Y-%m-%d %H:%M:%S')}',
                 'ERROR', '{error_msg}')
                """
                client.execute_query(metadata_insert)
        
        client.close()
        print(f"\n✓ Schema {schema_name} processado com sucesso")
        
    except Exception as e:
        print(f"\n✗ Erro ao processar schema {schema_name}: {str(e)}")

def main():
    ref_date = datetime.now().strftime("%Y-%m-%d")
    
    print("=" * 80)
    print("CONFIGURAÇÃO STAGE CLICKHOUSE")
    print("=" * 80)
    print(f"Data de referência: {ref_date}")
    print(f"Total de schemas: {len(schemas_to_extract)}")
    print()
    
    if not check_oracle_connectivity():
        print("⚠ ATENÇÃO: Conectividade Oracle falhou. A extração pode falhar.")
        print("  Continuando mesmo assim...")
        print()
    
    initialize_stage()
    
    import os
    
    possible_spark_homes = [
        os.environ.get('SPARK_HOME'),
        '/usr/local/spark',
        '/opt/spark',
        '/usr/local/share/spark'
    ]
    
    spark_home = None
    for path in possible_spark_homes:
        if path and os.path.exists(f"{path}/jars/ojdbc8-21.9.0.0.jar"):
            spark_home = path
            break
    
    if not spark_home:
        spark_home = os.environ.get('SPARK_HOME', '/usr/local/spark')
    
    ojdbc_jar = f"{spark_home}/jars/ojdbc8-21.9.0.0.jar"
    clickhouse_jar = f"{spark_home}/jars/clickhouse-jdbc-0.4.6-all.jar"
    
    if not os.path.exists(ojdbc_jar):
        print(f"⚠ Aviso: JAR Oracle não encontrado em {ojdbc_jar}")
    if not os.path.exists(clickhouse_jar):
        print(f"⚠ Aviso: JAR ClickHouse não encontrado em {clickhouse_jar}")
    
    spark = (
        SparkSession.builder
        .appName(f"StageIngestion_{ref_date}")
        .config("spark.driver.memory", spark_config.driver_memory)
        .config("spark.executor.memory", spark_config.executor_memory)
        .config("spark.executor.cores", spark_config.executor_cores)
        .config("spark.sql.shuffle.partitions", spark_config.sql_shuffle_partitions)
        .config("spark.sql.adaptive.enabled", spark_config.sql_adaptive_enabled)
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.jars", f"{ojdbc_jar},{clickhouse_jar}")
        .getOrCreate()
    )
    
    oracle_jdbc_url = f"jdbc:oracle:thin:@//{oracle_config.host}:{oracle_config.port}/{oracle_config.service}"
    protocol = "https" if clickhouse_config.secure else "http"
    clickhouse_jdbc_url = (
        f"jdbc:clickhouse://{protocol}://{clickhouse_config.host}:"
        f"{clickhouse_config.port}/stage"
    )
    
    print("=" * 80)
    print("INICIANDO EXTRAÇÃO DOS SCHEMAS")
    print("=" * 80)
    
    for schema_name in schemas_to_extract:
        extract_schema_to_stage(spark, schema_name, ref_date, oracle_jdbc_url, clickhouse_jdbc_url)
    
    spark.stop()
    
    print()
    print("=" * 80)
    print("EXTRAÇÃO CONCLUÍDA")
    print("=" * 80)

if __name__ == "__main__":
    main()

