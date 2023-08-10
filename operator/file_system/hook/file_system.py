import pandas as pd


class LoadData(object):
    """
    Classe responsável por tranformar os dados em DataFrame e carregar no caminho definido.

    Arg:
        path (str): caminho do diretório aonde o arquivo CSV será carregado.

    Method: load_csv(result: list, file_name: str) -> CSV: Converte e salva o resultado em um arquivo CSV.
    """

    @staticmethod
    def load_csv(result, file_name, path):
        df = pd.DataFrame(result)
        df.to_csv(f'{path}/{file_name}.csv')
        print(f"Arquivo {file_name} armazenado no diretório: {path}")