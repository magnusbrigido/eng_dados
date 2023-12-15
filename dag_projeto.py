from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

file_path = '/home/magnus/Documentos/airflowengdados/dadosProjeto/'

with DAG(
    "DAG_projeto_eng_dados",
    start_date=datetime(2023, 12, 14),
    schedule_interval='@daily',
    max_active_runs=1
    ) as dag:

    cria_pasta = BashOperator(
         task_id = 'tarefa_1',
         bash_command = 'mkdir -p "/home/magnus/Documentos/airflowengdados/dadosProjeto"'
         )
    
    def extract_data(ti, csv_url):
        df = pd.read_csv(csv_url, sep=";")
        df.to_csv(file_path + "dados_originais.csv")

        df_without_null = df.dropna()
        df_without_null.to_csv(file_path + 'dados_sem_null.csv')

        ti.xcom_push(key='tarefa_2', value=df_without_null.to_json(orient='records'))
        

    def filter_df(ti, key_value, column, condition, file_name):
        xcom_df = ti.xcom_pull(key=key_value)
        df = pd.read_json(xcom_df)

        df_filtered = df[df[column] == condition]
        
        df_filtered = df_filtered.drop(columns=[column])

        df_filtered.to_csv(file_path + file_name + ".csv")

        ti.xcom_push(key=key_value[:7]+str(int(key_value[7]) + 1), value=df_filtered.to_json(orient='records'))

    extrai_dados_sem_null = PythonOperator(
        task_id = 'tarefa_2',
        python_callable = extract_data,
        op_kwargs = {
            'csv_url': 'https://dados.ufrn.br/dataset/554c2d41-cfce-4278-93c6-eb9aa49c5d16/resource/7c88d1ec-3e83-41b3-9487-c5402fb82c6e/download/discentes-2023.csv',
            # 'file_path' : '/home/magnus/Documentos/airflowengdados/dadosProjeto/',
            }
    )

    ingressantes_sisu = PythonOperator(
        task_id = 'tarefa_3',
        python_callable = filter_df,
        op_kwargs = {
            # 'file_path' : '/home/magnus/Documentos/airflowengdados/dadosProjeto/',
            'key_value': 'tarefa_2',
            'column': 'forma_ingresso',
            'condition': 'SiSU',
            'file_name': 'ingressantes_sisu'
            }
    )

    ingressantes_sisu_eng_comp = PythonOperator(
        task_id = 'tarefa_4',
        python_callable = filter_df,
        op_kwargs = {
            # 'file_path' : '/home/magnus/Documentos/airflowengdados/dadosProjeto/',
            'key_value': 'tarefa_3',
            'column': 'nome_curso',
            'condition': 'ENGENHARIA DE COMPUTAÇÃO',
            'file_name': 'ingressantes_sisu_eng_comp'
            }
    )

    cria_pasta >> extrai_dados_sem_null >> ingressantes_sisu >> ingressantes_sisu_eng_comp