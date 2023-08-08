from typing import Optional
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks import BaseHook
from operator.file_system.hook.file_system import LoadData


class PostgresHookPedro(object):
    """
    Classe responsável por conectar e executar consultas no PostgreSQL.

    :param conn_id: ID da conexão do PostgreSQL no Airflow.
    :type conn_id: str

    """
    #sphinx - docstring/site


    def __init__(
            self, 
            conn_id:str, 
            batch_len: Optional[int] = None, 
            file_system_hook: Optional[LoadData] = None, 
            postgres_hook: Optional[BaseHook] = None
    ):
        self.conn_id = conn_id
        self.batch_len = batch_len or 100000
        self.postgres_hook = postgres_hook or PostgresHook(postgres_conn_id=self.conn_id)
        self.file_system_hook = file_system_hook or LoadData()


    def execute_query(self, sql):
        conn = self.postgres_hook.get_conn()
        cursor = conn.cursor()
        print(f"Conectado ao banco de dados Postgres usando Conn Id: {self.conn_id}")

        print(f"Executando a consulta: {sql}")
        cursor.execute(sql)

        result = list()
        for row in cursor:
            new_row = cursor.fetchone()
            result.append(new_row)
            if len(result) > self.batch_len:
                #TODO: retornar um yield para ir pegando os dados e carregando a cada X linhas
                yield
                result = list()