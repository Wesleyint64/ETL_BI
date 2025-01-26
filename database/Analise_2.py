from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Funções de ETL

def extract_data():
    """
    Função para extrair os dados dos arquivos.
    """
    try:
        print("Carregando dados de oportunidades...")
        oportunidades = pd.read_json(r"C:\\Users\\wesle\\OneDrive\\Documentos\\Teste_Analista_BI\\database\\registros_oportunidades.json")
        print("registros_oportunidades.json carregado com sucesso.")
        
        print("Carregando dados de sellout...")
        sellout = pd.read_parquet(r"C:\\Users\\wesle\\OneDrive\\Documentos\\Teste_Analista_BI\\database\\sellout.parquet")
        print("sellout.parquet carregado com sucesso.")
        
        return oportunidades, sellout
    except Exception as e:
        print(f"Erro durante a extração: {e}")
        return None, None

def transform_data(oportunidades, sellout):
    """
    Função para realizar a transformação dos dados.
    """
    try:
        print("Transformando dados de oportunidades...")
        
        # Renomeando colunas conforme o modelo dimensional
        oportunidades.rename(columns={
            "Data de Registro": "data_registro",
            "Quantidade": "quantidade",
            "Status": "status",
            "Nome Fantasia": "nome_parceiro",
            "CNPJ Parceiro": "cnpj_parceiro",
            "Telefone Parceiro": "telefone_parceiro",
            "Categoria produto": "categoria_produto",
            "Nome_Produto": "nome_produto",
            "Valor_Unitario": "valor_unitario"
        }, inplace=True)

        oportunidades['data_registro'] = pd.to_datetime(oportunidades['data_registro'], unit='ms', errors='coerce')
        oportunidades['valor_total'] = oportunidades['quantidade'] * oportunidades['valor_unitario']
        oportunidades['id_oportunidade'] = range(1, len(oportunidades) + 1)

        print("Transformações no dataset de oportunidades concluídas.")

        print("Transformando dados de sellout...")
        
        sellout.rename(columns={
            "Data_Fatura": "data_fatura",
            "Quantidade": "quantidade",
            "NF": "nf",
            "Valor_Unitario": "preco_unitario",
            "Nome Fantasia": "nome_parceiro",
            "CNpj Parceiro": "cnpj_parceiro",
        }, inplace=True)

        sellout['data_fatura'] = pd.to_datetime(sellout['data_fatura'], errors='coerce')
        sellout['valor_total'] = sellout['quantidade'] * sellout['preco_unitario']
        sellout['id_sellout'] = range(1, len(sellout) + 1)

        print("Transformações no dataset de sellout concluídas.")

        return oportunidades, sellout
    except Exception as e:
        print(f"Erro durante a transformação: {e}")
        return None, None

def load_data(oportunidades, sellout):
    """
    Função para carregar os dados transformados em um arquivo Excel.
    """
    try:
        print("Carregando os dados transformados para o arquivo Excel...")
        with pd.ExcelWriter('dados_processados.xlsx') as writer:
            oportunidades.to_excel(writer, sheet_name='fato_registro_oportunidade', index=False)
            sellout.to_excel(writer, sheet_name='fato_sellout', index=False)
        print("Dados carregados com sucesso no arquivo 'dados_processados.xlsx'.")
    except Exception as e:
        print(f"Erro durante a carga: {e}")


# Definição do DAG no Airflow
# dag = DAG(
#     'etl_pipeline',
#     description='ETL Pipeline utilizando Airflow',
#     schedule_interval='@daily',  # Executa o pipeline diariamente
#     start_date=datetime(2025, 1, 23),
#     catchup=False,
# )

# Tarefas do Airflow

def extract_task():
    oportunidades, sellout = extract_data()
    return oportunidades, sellout

def transform_task(oportunidades, sellout):
    return transform_data(oportunidades, sellout)

def load_task(oportunidades, sellout):
    load_data(oportunidades, sellout)

# Tarefas no Airflow
t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    op_args=["{{ task_instance.xcom_pull(task_ids='extract_data') }}"],
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    op_args=["{{ task_instance.xcom_pull(task_ids='transform_data') }}"],
    dag=dag,
)

# Definindo a ordem de execução das tarefas
t1 >> t2 >> t3
