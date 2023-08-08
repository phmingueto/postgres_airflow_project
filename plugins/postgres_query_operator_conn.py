from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


class ConnPostgresQuery:
    """
    Classe responsável por conectar e executar consultas no PostgreSQL.

    Args:
        conn_id (str): ID da conexão do PostgreSQL no Airflow.

    Method:
        execute_query(sql: str) -> list: Executa a consulta fornecida e retorna o resultado.

    """

    def __init__(self, conn_id):
        self.conn_id = conn_id

    def execute_query(self, sql):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        print(f"Conectado ao banco de dados Postgres usando Conn Id: {self.conn_id}")

        print(f"Executando a consulta: {sql}")
        cursor.execute(sql)

        result = cursor.fetchall()
        print(f"Resultado da consulta: {result}")

        cursor.close()
        conn.close()

        return result


class LoadData:
    """
    Classe responsável por tranformar os dados em DataFrame e carregar no caminho definido.

    Arg:
        path (str): caminho do diretório aonde o arquivo CSV será carregado.

    Method: load_csv(result: list, file_name: str) -> CSV: Converte e salva o resultado em um arquivo CSV.
    """

    def __init__(self, path):
        self.path = path

    def load_csv(self, result, file_name):
        df = pd.DataFrame(result)
        df.to_csv(f'{self.path}/{file_name}.csv')
        print(f"Arquivo {file_name} armazenado no diretório: {self.path}")


class PostgresQueryConnOperator(BaseOperator):
    """
    Operador personalizado para conectar e executar consultas no PostgreSQL, transformar consulta em aquivo CSV e
    armazenar arquivo no diretório definido.

    Args:
        sql (str): A consulta SQL a ser executada.
        conn_id (str): ID da conexão do PostgreSQL no Airflow.
        path (str): Caminho para o diretório onde o arquivo CSV será armazenado.
        file_name (str): Nome do arquivo CSV (sem extensão) a ser gerado.
    """

    @apply_defaults
    def __init__(self, sql, conn_id, path, file_name, *args, **kwargs):
        super(PostgresQueryConnOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.path = path
        self.file_name = file_name

    def execute(self, context):
        query_executor = ConnPostgresQuery(self.conn_id)
        query_result = query_executor.execute_query(self.sql)

        load_result = LoadData(self.path)
        load_result.load_csv(query_result, self.file_name)
