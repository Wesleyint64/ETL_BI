import mysql.connector
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Funções de ETL

def extract_data(**kwargs):
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
        
        kwargs['ti'].xcom_push(key='oportunidades', value=oportunidades)
        kwargs['ti'].xcom_push(key='sellout', value=sellout)

    except Exception as e:
        print(f"Erro durante a extração: {e}")

def transform_data(**kwargs):
    """
    Função para realizar a transformação dos dados.
    """
    try:
        oportunidades = kwargs['ti'].xcom_pull(key='oportunidades', task_ids='extract_data')
        sellout = kwargs['ti'].xcom_pull(key='sellout', task_ids='extract_data')

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

        kwargs['ti'].xcom_push(key='oportunidades_transformed', value=oportunidades)
        kwargs['ti'].xcom_push(key='sellout_transformed', value=sellout)

    except Exception as e:
        print(f"Erro durante a transformação: {e}")

def load_data(**kwargs):
    """
    Função para carregar os dados transformados nas tabelas do MySQL.
    """
    try:
        oportunidades = kwargs['ti'].xcom_pull(key='oportunidades_transformed', task_ids='transform_data')
        sellout = kwargs['ti'].xcom_pull(key='sellout_transformed', task_ids='transform_data')

        print("Conectando ao banco de dados MySQL...")

        # Conectando ao banco de dados MySQL
        conn = mysql.connector.connect(
            host='localhost',  # Altere para o seu host, se necessário
            user='root',       # Altere para seu usuário MySQL
            password='Sasuke110800@',  # Altere para sua senha
            database='analista_bi'  # Nome do banco de dados
        )

        cursor = conn.cursor()

        # Inserir dados na tabela de oportunidades
        print("Inserindo dados na tabela 'oportunidades'...")
        for _, row in oportunidades.iterrows():
            cursor.execute("""
                INSERT INTO oportunidades (data_registro, quantidade, status, nome_parceiro, cnpj_parceiro, telefone_parceiro, 
                                          categoria_produto, nome_produto, valor_unitario, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['data_registro'],
                row['quantidade'],
                row['status'],
                row['nome_parceiro'],
                row['cnpj_parceiro'],
                row['telefone_parceiro'],
                row['categoria_produto'],
                row['nome_produto'],
                row['valor_unitario'],
                row['valor_total']
            ))

        # Inserir dados na tabela de sellout
        print("Inserindo dados na tabela 'sellout'...")
        for _, row in sellout.iterrows():
            cursor.execute("""
                INSERT INTO sellout (data_fatura, quantidade, nf, preco_unitario, nome_parceiro, cnpj_parceiro, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['data_fatura'],
                row['quantidade'],
                row['nf'],
                row['preco_unitario'],
                row['nome_parceiro'],
                row['cnpj_parceiro'],
                row['valor_total']
            ))

        # Commit para garantir que as alterações sejam salvas
        conn.commit()

        print("Dados carregados com sucesso no MySQL.")

    except Exception as e:
        print(f"Erro durante a carga dos dados: {e}")

    finally:
        # Fechar a conexão com o banco de dados
        cursor.close()
        conn.close()

# Definição do DAG no Airflow
dag = DAG(
    'etl_pipeline_FINAL',
    description='ETL Pipeline utilizando Airflow',
    schedule='@daily',  # Usando o novo parâmetro 'schedule'
    start_date=datetime(2025, 1, 23),
    catchup=False,
)

# Tarefas no Airflow
t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,  # Ainda necessário para passar o contexto
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Remover se não necessário
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Remover se não necessário
    dag=dag,
)

# Definindo a ordem de execução das tarefas
t1 >> t2 >> t3

