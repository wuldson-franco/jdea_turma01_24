from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

#from nova_drive.tasks.stg_nova_drive_full import postgres_to_minio_etl_parquet_full
from nova_drive.tasks.stg_nova_drive import postgres_to_minio_etl_parquet

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
    dag_id='dag_nova_drive',
    default_args=default_args,
    description='DAG responsável pelo ETL dos dados do cliente nova drive',
    schedule_interval="0 14 * * *",
    catchup=False
)
def main_dag():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']  # Altere conforme necessário
    bucket_name = 'projetos'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'

    #with TaskGroup("group_task_nova_drive_full", tooltip="Tasks processadas do BD externo para minio(carga full), salvando em .parquet") as group_task_nova_drive_full:
    #    for table_name in table_names:
    #        PythonOperator(
    #            task_id=f'carga_full_{table_name}',
    #            python_callable=postgres_to_minio_etl_parquet_full,
    #            op_args=[table_name, bucket_name, endpoint_url, access_key, secret_key]
    #        )

    with TaskGroup("group_task_nova_drive", tooltip="Tasks processadas do BD externo para minio(carga incremental), salvando em .parquet") as group_task_nova_drive:
        for table_name in table_names:
            PythonOperator(
                task_id=f'carga_inc_{table_name}',
                python_callable=postgres_to_minio_etl_parquet,
                op_args=[table_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    # Definindo a ordem de execução dos grupos
    # Aqui a gente vai garantir que o airflow execute primeiramente as cargas e depois as transformações.
    # group_task_parquet >>
    #group_task_nova_drive_full
    group_task_nova_drive  # >> tasks_dbt

main_dag_instance = main_dag()
