import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Definindo a função de extração de dados
def extract():
    try:
        print("Extraindo dados...")
        # Caminhos dos arquivos
        caminho_oportunidades = "C:/Users/wesle/OneDrive/Documentos/Teste_Analista_BI/database/registros_oportunidades.json"
        caminho_sellout = "C:/Users/wesle/OneDrive/Documentos/Teste_Analista_BI/database/sellout.parquet"
        
        # Carregar os dados dos arquivos
        oportunidades = pd.read_json(caminho_oportunidades)
        sellout = pd.read_parquet(caminho_sellout)
        
        print("Dados carregados com sucesso.")
        
        return oportunidades, sellout
    
    except Exception as e:
        raise Exception(f"Erro na etapa de extração: {e}")

# Definindo a função de transformação de dados
def transform():
    try:
        print("Transformando dados...")
        # Aqui você colocaria a lógica de transformação
        print("Transformações concluídas.")
    
    except Exception as e:
        raise Exception(f"Erro na etapa de transformação: {e}")

# Definindo a função de carregamento de dados
def load():
    try:
        print("Carregando dados...")
        # Aqui você colocaria a lógica para carregar os dados (por exemplo, para um arquivo Excel)
        print("Dados carregados com sucesso.")
    
    except Exception as e:
        raise Exception(f"Erro na etapa de carregamento: {e}")

# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_airflow_novo2',  # Nome da DAG
    default_args=default_args,
    description='ETL com Airflow',
    schedule=None,  # Substituindo schedule_interval por schedule
    start_date=pendulum.today('UTC').add(days=-1),  # Substituindo days_ago por pendulum
    catchup=False,
)

# Definindo as tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag,
)

# Definindo a ordem de execução
extract_task >> transform_task >> load_task
