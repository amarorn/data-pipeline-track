import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from config.settings import oracle_config

def get_function_source_spark(schema, function_name):
    try:
        spark = SparkSession.builder.appName("ViewOracleFunction").getOrCreate()
        
        oracle_jdbc_url = f"jdbc:oracle:thin:@//{oracle_config.host}:{oracle_config.port}/{oracle_config.service}"
        
        query = f"""
        (SELECT line, text
        FROM all_source
        WHERE owner = UPPER('{schema}')
        AND name = UPPER('{function_name}')
        AND type = 'FUNCTION'
        ORDER BY line)
        """
        
        df = (
            spark.read.format("jdbc")
            .option("url", oracle_jdbc_url)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("query", query)
            .option("user", oracle_config.user)
            .option("password", oracle_config.password)
            .load()
        )
        
        rows = df.collect()
        
        if not rows:
            print(f"Função {schema}.{function_name} não encontrada ou sem código fonte disponível.")
            spark.stop()
            return
        
        print("=" * 80)
        print(f"CÓDIGO FONTE: {schema}.{function_name}")
        print("=" * 80)
        print()
        
        for row in rows:
            line_num = row['LINE']
            text = row['TEXT']
            print(f"{line_num:4d} | {text.rstrip()}")
        
        print()
        print("=" * 80)
        
        spark.stop()
        
    except Exception as e:
        print(f"Erro ao consultar função: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    schema = "BISTAGE"
    function_name = "F_CONSULTA_INST_BI"
    
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
    if len(sys.argv) > 2:
        schema = sys.argv[2]
    
    get_function_source_spark(schema, function_name)
