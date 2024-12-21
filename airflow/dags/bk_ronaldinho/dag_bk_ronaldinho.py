from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from bk_ronaldinho.tasks.landing_bk_ronaldinho import google_sheet_to_minio_etl
from bk_ronaldinho.tasks.dim_bk_ronaldinho import dim_clientes_minio
#from bk_ronaldinho.tasks.fato_bk_ronaldinho import fato_vendas_minio

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
    dag_id='dag_bk_ronaldinho',
    default_args=default_args,
    description='DAG responsavel pelo ETL do cliente BIKE_RONALDINHO. Dados vindos da API do google sheets',
    schedule_interval="0 11 * * *",
    catchup=False
)
def main_dag():
    google_sheets = ['Clientes', 'Vendedores', 'Produtos', 'Vendas', 'ItensVendas']  # Altere para o nome das abas da sua planilha
    bucket_name = 'projetos'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    sheet_id = '1XMrFMPptCKfy6u2tsP0FuAgPlbJ-anA0caH2e6LL5F0'

    with TaskGroup("landing_task_bk_ronaldinho", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as landing_task_bk_ronaldinho:
        for sheets_name in google_sheets:
            PythonOperator(
                task_id=f'landing_sheets_{sheets_name}',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheets_name, bucket_name, endpoint_url, access_key, secret_key]
            )

    with TaskGroup("dim_task_bk_ronaldinho", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as dim_task_bk_ronaldinho:
        dimensoes = {
            'dim_clientes': 'Clientes',
            # 'dim_vendedores': 'Vendedores',
            # 'dim_produtos': 'Produtos',
            # Adicione outras dimensões aqui, se necessário.
        }
        for dim_name, sheet_name in dimensoes.items():
            PythonOperator(
                task_id=f'{dim_name}',  # O nome da tarefa será único, baseado na chave do dicionário
                python_callable=dim_clientes_minio,  # Ajuste esta função se necessário para outras dimensões
                op_args=[bucket_name,
                         f"bk_ronaldinho/landing/{sheet_name}/lnd_{sheet_name}.parquet",
                         endpoint_url,
                         access_key,
                         secret_key]
            )

    #with TaskGroup("fato_task_bk_ronaldinho", tooltip="Tasks processadas do google sheets para minio, salvando em .parquet") as fato_task_bk_ronaldinho:
    #    for sheets_name in google_sheets:
    #        PythonOperator(
    #            task_id=f'landing_sheets_{sheets_name}',
    #            python_callable=fato_vendas_minio,
    #            op_args=[sheet_id, sheets_name, bucket_name, endpoint_url, access_key, secret_key]
    #        )


    # Definindo a ordem de execução dos grupos
    # Aqui a gente vai garantir que o airflow execute primeiramente as cargas e depois as transformações.
    # group_task_parquet >>
    landing_task_bk_ronaldinho >> dim_task_bk_ronaldinho #>> fato_task_bk_ronaldinho

main_dag_instance = main_dag()
