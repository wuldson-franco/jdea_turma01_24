import boto3
import pandas as pd
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def dim_clientes_minio(bucket_name, key, endpoint_url, access_key, secret_key):
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

        # Transformar dados para criar dimensão clientes
        dim_clientes_df = df[['ClienteID', 'Cliente', 'Estado', 'Sexo', 'Status']].drop_duplicates()
        dim_clientes_df.rename(columns={
            'ClienteID': 'cod_cliente',
            'Cliente': 'nome_cliente',
            'Estado': 'estado_cliente',
            'Sexo': 'sexo_cliente',
            'Status': 'status_cliente'
        }, inplace=True)

        # Salvar a dimensão transformada de volta no MinIO
        output_buffer = io.BytesIO()
        dim_clientes_df.to_parquet(output_buffer, index=False)
        output_buffer.seek(0)
        dim_key = 'bk_ronaldinho/dimensao/dim_clientes.parquet'  # Ajustar a chave para o nome do arquivo que você quer.
        minio_client.put_object(
            Bucket=bucket_name,
            Key=dim_key,
            Body=output_buffer.getvalue()
        )
        logging.info(f"Dimensão 'dim_clientes' salva no MinIO com a chave: {dim_key}")

        # Conectar ao MariaDB e criar tabela
        mysql_hook = MySqlHook(mysql_conn_id='mariadb_local')
        connection = mysql_hook.get_conn()
        with connection.cursor() as cursor:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dim_clientes (
                cod_cliente INT PRIMARY KEY,
                nome_cliente VARCHAR(255),
                estado_cliente VARCHAR(255),
                sexo_cliente VARCHAR(255),
                status_cliente VARCHAR(255)
            )
            """
            cursor.execute(create_table_sql)
            logging.info("Tabela dim_clientes criada/verificada com sucesso.")

            # Inserir dados na tabela dimensão
            for _, row in dim_clientes_df.iterrows():
                insert_sql = """
                INSERT INTO dim_clientes (cod_cliente, nome_cliente, estado_cliente, sexo_cliente, status_cliente)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                nome_cliente=VALUES(nome_cliente),
                estado_cliente=VALUES(estado_cliente),
                sexo_cliente=VALUES(sexo_cliente),
                status_cliente=VALUES(status_cliente)
                """
                cursor.execute(insert_sql, tuple(row))
            connection.commit()
            logging.info("Dados inseridos/atualizados na tabela dim_clientes.")
    except Exception as e:
        logging.error(f"Erro ao criar dimensão clientes: {e}")
        raise
    finally:
        if connection:
            connection.close()
