from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from tasks.landing import google_sheet_to_minio_etl

default_args = {
    'owner': 'Wuldson',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='main_dag',
    default_args=default_args,
    description='DAG responsavel pelo ETL do case breweries',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def main_dag():
    #table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']  # Altere conforme necessário
    google_sheets = ['Clientes_Bike', 'Vendedores_Bike', 'Produtos_Bike', 'Vendas_Bike', 'ItensVendas_Bike']  # Altere para o nome das abas da sua planilha
    bucket_name = 'landing'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '1XMrFMPptCKfy6u2tsP0FuAgPlbJ-anA0caH2e6LL5F0'

    with TaskGroup("group_task_sheets", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as group_task_sheets:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'task_sheets_{sheets_name}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheets_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    # Definindo a ordem de execução dos grupos
    # Aqui a gente vai garantir que o airflow execute primeiramente as cargas e depois as transformações.
    # group_task_parquet >>
    group_task_sheets  # >> tasks_dbt

main_dag_instance = main_dag()
