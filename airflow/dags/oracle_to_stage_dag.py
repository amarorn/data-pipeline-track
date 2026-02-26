from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'oracle_to_stage_extraction',
    default_args=default_args,
    description='Extração diária de schemas Oracle para ClickHouse Stage',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['oracle', 'clickhouse', 'stage', 'extraction'],
)

extract_task = BashOperator(
    task_id='extract_oracle_schemas_to_stage',
    bash_command="""
    docker exec track-jupyter python3 /app/apps/orchestrator/stage_extraction.py
    """,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_oracle_schemas_to_stage',
    python_callable=extract_schemas_task,
    dag=dag,
)

def verify_stage_data_task(**context):
    import sys
    sys.path.insert(0, '/app')
    
    from connectors.clickhouse_client import ClickHouseClient
    from datetime import datetime
    
    client = ClickHouseClient()
    ref_date = datetime.now().strftime("%Y-%m-%d")
    
    query = f"""
    SELECT 
        schema_name,
        COUNT(DISTINCT table_name) as tables_count,
        SUM(row_count) as total_rows,
        COUNTIF(status = 'SUCCESS') as success_count,
        COUNTIF(status = 'ERROR') as error_count
    FROM stage.extraction_metadata
    WHERE ref_date = toDate('{ref_date}')
    GROUP BY schema_name
    ORDER BY schema_name
    """
    
    result = client.execute_query_with_result(query)
    print("\n=== RESUMO DA EXTRAÇÃO ===")
    print(f"Data: {ref_date}\n")
    
    if result.result_set:
        for row in result.result_set:
            schema, tables, rows, success, errors = row
            print(f"Schema: {schema}")
            print(f"  Tabelas: {tables} | Registros: {rows:,} | Sucesso: {success} | Erros: {errors}")
    else:
        print("Nenhum dado encontrado para a data de hoje")
    
    client.close()
    return "VERIFIED"

verify_task = PythonOperator(
    task_id='verify_stage_data',
    python_callable=verify_stage_data_task,
    dag=dag,
)

extract_task >> verify_task

