from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


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