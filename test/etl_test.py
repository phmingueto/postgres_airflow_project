import pandas as pd

path = '/dags/dataset_teste_Just_BI.csv'

def extract_csv(path):
    df = pd.read_csv(path)
    return df

def head_df(df):
    df_head = df.head(5)
    return df_head

def load_df(df):
    df.to_csv("/home/ph/airflow_project/dataset_teste.csv")
    print('ok3')

df = extract_csv(path)
df_head = head_df(df)
load_df(df_head)

extract_csv
head_df
load_df