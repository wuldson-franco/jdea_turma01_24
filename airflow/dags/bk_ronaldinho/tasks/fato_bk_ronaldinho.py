import boto3
import pandas as pd
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fato_vendas_minio(bucket_name, key, endpoint_url, access_key, secret_key):
    # Configurar cliente MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        # Ler dados do MinIO
        response = minio_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))

        # Transformar dados para criar fato vendas
        fact_vendas_df = df.rename(columns={
            'id_vendas': 'cod_vendas',
            'id_veiculos': 'cod_veiculo',
            'id_concessionarias': 'cod_concessionaria',
            'id_vendedores': 'cod_vendedor',
            'valor_pago': 'valor_pago',
            'data_venda': 'data_venda',
            'data_inclusao': 'data_cadastro'
        })

        # Conectar ao MariaDB e criar tabela
        mysql_hook = MySqlHook(mysql_conn_id='mariadb_local')
        connection = mysql_hook.get_conn()
        with connection.cursor() as cursor:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS fact_vendas (
                cod_vendas INT PRIMARY KEY,
                cod_veiculo INT,
                cod_concessionaria INT,
                cod_vendedor INT,
                valor_pago FLOAT,
                data_venda DATE,
                data_cadastro DATE
            )
            """
            cursor.execute(create_table_sql)
            logging.info("Tabela fact_vendas criada/verificada com sucesso.")

            # Inserir dados na tabela fato
            for _, row in fact_vendas_df.iterrows():
                insert_sql = """
                INSERT INTO fact_vendas (cod_vendas, cod_veiculo, cod_concessionaria, cod_vendedor, valor_pago, data_venda, data_cadastro)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                cod_veiculo=VALUES(cod_veiculo),
                cod_concessionaria=VALUES(cod_concessionaria),
                cod_vendedor=VALUES(cod_vendedor),
                valor_pago=VALUES(valor_pago),
                data_venda=VALUES(data_venda),
                data_cadastro=VALUES(data_cadastro)
                """
                cursor.execute(insert_sql, tuple(row))
            connection.commit()
            logging.info("Dados inseridos/atualizados na tabela fato_vendas.")
    except Exception as e:
        logging.error(f"Erro ao criar fato vendas: {e}")
        raise
    finally:
        if connection:
            connection.close()
