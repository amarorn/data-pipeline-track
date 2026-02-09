# %% [markdown]
# # Migração Oracle → ClickHouse
# 
# **PROBLEMA ATUAL**: Banco Oracle não acessível (ORA-12514)
# 
# ## Como religar/verificar o Oracle (servidor 10.255.150.11)
# 
# ### 1. Conectar no servidor Oracle via SSH:
# ```bash
# ssh oracle@10.255.150.11
# # ou
# ssh root@10.255.150.11
# ```
# 
# ### 2. Verificar status do banco:
# ```bash
# # Como usuario oracle
# su - oracle
# 
# # Verificar se o banco está rodando
# ps -ef | grep pmon
# 
# # Se não aparecer nada, o banco está PARADO
# ```
# 
# ### 3. Iniciar o banco Oracle:
# ```bash
# # Conectar no SQL*Plus como SYSDBA
# sqlplus / as sysdba
# 
# # Dentro do SQL*Plus:
# SQL> STARTUP;
# SQL> EXIT;
# ```
# 
# ### 4. Verificar/Iniciar o Listener:
# ```bash
# # Verificar status
# lsnrctl status
# 
# # Se estiver parado, iniciar:
# lsnrctl start
# 
# # Ver services registrados:
# lsnrctl services
# ```
# 
# ### 5. Descobrir o SERVICE NAME correto:
# ```bash
# sqlplus / as sysdba
# SQL> SELECT value FROM v$parameter WHERE name='service_names';
# SQL> SELECT name FROM v$database;
# SQL> EXIT;
# ```
# 
# ### 6. Testar conexão do Jupyter para o Oracle:
# Depois de religar, execute a célula 4 deste notebook para testar a conexão.
# 
# ---
# 
# **ALTERNATIVA**: Se não tiver acesso SSH ao servidor Oracle, consulte o DBA responsável.
# %%
!pip install "numpy<2" pandas --force-reinstall
# %% [markdown]
# ## 1. Verificar NumPy
# %%
import numpy as np
print(f"NumPy: {np.__version__}")
# %% [markdown]
# ## 2. Criar Spark Session
# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("oracle-clickhouse-migration")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)

print("Spark Session criada (Arrow desabilitado)")
# %% [markdown]
# ## 3. Configurar Oracle
# %%
oracle_host = "10.255.150.11"
oracle_port = 1521
oracle_user = "clickhouse"
oracle_password = "qiU!EOoe"
oracle_service = "CDBQA_grupotracker.grupotracker.com.br"
jdbc_url = f"jdbc:oracle:thin:@//{oracle_host}:{oracle_port}/{oracle_service}"

print(f"Service Name: {oracle_service}")
print(f"JDBC URL: {jdbc_url}")
print("\nTestando conexao...")
# %%
df_test = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("user", oracle_user)
    .option("password", oracle_password)
    .option("driver", "oracle.jdbc.OracleDriver")
    .option("query", "SELECT 1 AS ok FROM dual")
    .load()
)

df_test.show()
print("[OK] Oracle funcionando")
# %% [markdown]
# ## 4. Definir Tabelas
# %%
tables_to_extract = [
    "ginf.depara_cliente",
    "ginf.BASE_CEP_COMPLETA",
    "ginf.TST_CONTRATOS_BI",
    "siga.SC5030",
    "siga.SC6030",
    "ginf.TST_HISTORICO_SOLICITACOES",
    "ginf.TST_SOLICIT_CADASTRADAS",
    "siga.SD2030",
    "siga.SF2030",
    "siga.ZTX030",
    "ginf.TST_CONTRATOS",
]

print(f"Total: {len(tables_to_extract)} tabelas")
for t in tables_to_extract:
    print(f"  - {t}")
# %% [markdown]
# ## 5. Executar Migracao
# %%
import clickhouse_connect
import pandas as pd
from datetime import datetime

client = clickhouse_connect.get_client(
    host="e1a1lieug8.us-central1.gcp.clickhouse.cloud",
    port=8443,
    username="default",
    password="_uv765EvWphL_",
    database="raw",
    secure=False,
    connect_timeout=60,
    send_receive_timeout=300
)
print("ClickHouse: OK\n")

ch_tables = {
    "ginf_depara_cliente": "ginf.depara_cliente",
    "ginf_base_cep_completa": "ginf.BASE_CEP_COMPLETA",
    "bistage_tst_contratos_bi": "bistage.TST_CONTRATOS_BI",
    "siga_sc5030": "siga.SC5030",
    "siga_sc6030": "siga.SC6030",
    "ginf_tst_historico_solicitacoes": "ginf.TST_HISTORICO_SOLICITACOES",
    "ginf_tst_solicit_cadastradas": "ginf.TST_SOLICIT_CADASTRADAS",
    "siga_sd2030": "siga.SD2030",
    "siga_sf2030": "siga.SF2030",
    "siga_ztx030": "siga.ZTX030",
    "ginf_tst_contratos": "ginf.TST_CONTRATOS"
}

print("Dropando tabelas antigas...")
for ch_tbl in ch_tables.keys():
    try:
        client.command(f"DROP TABLE IF EXISTS raw.{ch_tbl}")
        print(f"  [OK] {ch_tbl} dropada")
    except Exception as e:
        print(f"  [AVISO] {ch_tbl}: {str(e)[:80]}")

print("\nPronto para migracao")
print(f"Total: {len(ch_tables)} tabelas")
print("=" * 60)
# %%
jdbc_opts = {
    "url": jdbc_url,
    "user": oracle_user,
    "password": oracle_password,
    "driver": "oracle.jdbc.OracleDriver"
}

from tqdm.notebook import tqdm
import logging
import time
import gc

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

CHUNK_SIZE = 50000
MAX_RETRIES = 3
RETRY_DELAY = 10


def spark_to_ch_type(spark_type):
    tipo = str(spark_type).lower()
    if 'string' in tipo or 'varchar' in tipo:
        base = 'String'
    elif 'decimal' in tipo or 'number' in tipo:
        base = 'String'
    elif 'int' in tipo:
        base = 'Int64'
    elif 'long' in tipo or 'bigint' in tipo:
        base = 'Int64'
    elif 'double' in tipo or 'float' in tipo:
        base = 'Float64'
    elif 'date' in tipo:
        base = 'String'
    elif 'timestamp' in tipo:
        base = 'String'
    else:
        base = 'String'
    
    return f'Nullable({base})'


def create_table_if_not_exists(client, ch_tbl, df):
    try:
        client.command(f"SELECT 1 FROM {ch_tbl} LIMIT 1")
        return True
    except:
        ch_cols = []
        for field in df.schema.fields:
            ch_type = spark_to_ch_type(field.dataType)
            ch_cols.append(f"`{field.name}` {ch_type}")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {ch_tbl} (
            {', '.join(ch_cols)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        client.command(create_sql)
        return False


def process_in_chunks(rows, cols, chunk_size=CHUNK_SIZE):
    for i in range(0, len(rows), chunk_size):
        chunk_rows = rows[i:i+chunk_size]
        data = []
        for row in chunk_rows:
            row_data = []
            for col in cols:
                val = getattr(row, col)
                if val is None:
                    row_data.append(None)
                elif isinstance(val, (int, float, str)):
                    row_data.append(val)
                else:
                    row_data.append(str(val))
            data.append(row_data)
        yield pd.DataFrame(data, columns=cols)


def restart_spark_with_delay(current_spark, delay=5):
    try:
        current_spark.stop()
        time.sleep(delay)
        gc.collect()
    except:
        pass
    
    from pyspark.sql import SparkSession
    new_spark = (
        SparkSession.builder
        .appName("oracle-clickhouse-migration")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    return new_spark


def migrate_table(current_spark, ch_tbl, oracle_tbl, client, jdbc_opts, pbar):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t0 = datetime.now()

            df = current_spark.read.format("jdbc") \
                .options(**jdbc_opts) \
                .option("dbtable", oracle_tbl) \
                .load()

            create_table_if_not_exists(client, ch_tbl, df)
            row_count = df.count()
            
            if row_count == 0:
                pbar.write(f"  [{ch_tbl}] AVISO: Tabela vazia")
                return False, 0, current_spark

            rows = df.collect()
            cols = df.columns
            total_inserted = 0
            for chunk_pdf in process_in_chunks(rows, cols):
                client.insert_df(ch_tbl, chunk_pdf)
                total_inserted += len(chunk_pdf)

            del rows
            del df
            gc.collect()
            
            dur = (datetime.now() - t0).total_seconds()
            pbar.write(f"  [{ch_tbl}] OK: {total_inserted:,} linhas em {dur:.2f}s")

            if total_inserted > 1000000:
                pbar.write(f"  -> Reiniciando Spark...")
                current_spark = restart_spark_with_delay(current_spark, delay=5)
            
            return True, total_inserted, current_spark

        except Exception as e:
            msg = str(e)[:150]
            
            if attempt < MAX_RETRIES:
                pbar.write(f"  [{ch_tbl}] Tentativa {attempt}/{MAX_RETRIES} falhou: {msg}")
                pbar.write(f"  -> Aguardando {RETRY_DELAY}s antes de tentar novamente...")
                time.sleep(RETRY_DELAY)

                if "ORA-12514" in str(e) or "Connection refused" in str(e):
                    pbar.write(f"  -> Reiniciando Spark...")
                    current_spark = restart_spark_with_delay(current_spark, delay=10)
            else:
                pbar.write(f"  [{ch_tbl}] ERRO (apos {MAX_RETRIES} tentativas): {msg}")
                return False, 0, current_spark
    
    return False, 0, current_spark

sep = "=" * 60
print(sep)
print("MIGRACAO COMPLETA (SEM LIMITE)")
print(f"Chunk size: {CHUNK_SIZE:,} linhas")
print(f"Max retries: {MAX_RETRIES}")
print(sep)

start = datetime.now()
ok = 0
fail = 0
total_rows = 0
failed = []
current_spark = spark
pbar = tqdm(ch_tables.items(), total=len(ch_tables), desc="Migracao", unit="tabela")

for i, (ch_tbl, oracle_tbl) in enumerate(pbar, 1):
    pbar.set_description(f"[{i}/{len(ch_tables)}] {oracle_tbl}")
    
    success, rows_inserted, current_spark = migrate_table(
        current_spark, ch_tbl, oracle_tbl, client, jdbc_opts, pbar
    )
    
    if success:
        ok += 1
        total_rows += rows_inserted
    else:
        fail += 1
        failed.append(oracle_tbl)

pbar.close()

dur = (datetime.now() - start).total_seconds()

print()
print(sep)
print("RESUMO")
print(sep)
print(f"Sucesso: {ok}/{len(ch_tables)}")
print(f"Falhas: {fail}")
print(f"Total linhas: {total_rows:,}")
print(f"Tempo: {dur:.2f}s ({dur/60:.1f} min)")

if failed:
    print("\nTabelas com erro:")
    for t in failed:
        print(f"  - {t}")

print(sep)