import psycopg2

# Varivel para inserir nas funções de criação tabela e inserção de dados.
query_create_table = '''
    CREATE TABLE nome_cliente(
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        cpf INT
    )
'''

query_inset_data = "INSERT INTO nome_cliente (id, nome, cpf) VALUES (%s, %s, %s)"
data = ('1', 'pedro', 564545)

def conection_postgres(host: str, port: int, database: str, user: str, password: int) -> None:
    # Configuração da conexão com o banco de dados
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    print('Banco de Dados Postgres conectado com sucesso!')
    return conn


def create_table(connection: None, query: str) -> None:
    # Criação de um cursor para executar query
    cur = connection.cursor()
    cur.execute(query)
    print('Criação de tabela realizada com sucesso!')

def insert_data(query: str, data: str, connection: None) -> None:
    # Inserindo dados na tabela e executando
    cur = connection.cursor()
    cur.execute(query, data)

    # Confirmação das alterações no banco de dados
    connection.commit()

    # Fechamento do cursor e da conexão
    cur.close()
    connection.close()

    print('Iserção de dados realizado com sucesso!')




conn = conection_postgres('localhost', 5433, 'postgres_db', 'postgres', 123456)

create_table(conn, query_create_table)

insert_data(query_inset_data, data, conn)
