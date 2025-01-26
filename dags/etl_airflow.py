import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.models import TaskInstance

# Definindo a função de extração de dados
def extract():
    try:
        print("Extraindo dados...")
        # Caminhos dos arquivos

        # Caminhos dos arquivos no contêiner Docker
        caminho_oportunidades = "/opt/airflow/dags/database/registros_oportunidades.json"
        caminho_sellout = "/opt/airflow/dags/database/sellout.parquet"

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

# Função para verificar o estado de retry da task
def check_task_retry_status(task_instance: TaskInstance):
    # Alterado de 'up_for_retry' para 'ready_for_retry'
    if task_instance.ready_for_retry:
        print(f"Tarefa {task_instance.task_id} está esperando para ser reexecutada.")
    else:
        print(f"Tarefa {task_instance.task_id} não está mais aguardando reexecução.")

# Definindo a DAG
default_args = { 
    'owner': 'airflow',
    'retries': 3,  # Tenta até 3 vezes
    'retry_delay': timedelta(minutes=5),  # A cada 5 minutos
}

dag = DAG(
    'etl_airflow_final',  # Nome da DAG
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

# Função que será chamada após a execução da task para checar o estado de retry
def check_retry(**kwargs):
    # O Airflow já passa 'execution_date' no contexto
    execution_date = kwargs['execution_date']  # Usando o execution_date da execução atual
    task_instance = TaskInstance(task=extract_task, execution_date=execution_date)  # Passando a task da extração
    check_task_retry_status(task_instance)

# Adicionando a tarefa de verificação de retry após as outras
check_retry_task = PythonOperator(
    task_id='check_retry_status',
    python_callable=check_retry,
    provide_context=True,  # Necessário para passar o execution_date
    dag=dag,
)

# Definindo a ordem de execução
extract_task >> transform_task >> load_task >> check_retry_task

