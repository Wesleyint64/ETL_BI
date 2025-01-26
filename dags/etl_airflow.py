import mysql.connector
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
<<<<<<< HEAD
from datetime import timedelta
import os

# Função para conectar ao MySQL
def connect_mysql():
    return mysql.connector.connect(
        host="172.18.0.2",  # Endereço do seu banco MySQL
        user="root",     # Seu nome de usuário
        password="Sasuke110800@",   # Sua senha
        database="analista_bi",   # Nome do seu banco de dados
        port=3306               # Porta do MySQL, geralmente 3306
    )

# Função para extração de dados
def extract(ti):
    try:
        print("Extraindo dados...")

        # Caminhos dos arquivos no contêiner Docker
        caminho_oportunidades = "/opt/airflow/dags/database/registros_oportunidades.json"
        caminho_sellout = "/opt/airflow/dags/database/sellout.parquet"

        # Verificar se os arquivos existem
        if not os.path.exists(caminho_oportunidades):
            raise Exception(f"Arquivo não encontrado: {caminho_oportunidades}")
        if not os.path.exists(caminho_sellout):
            raise Exception(f"Arquivo não encontrado: {caminho_sellout}")

        # Carregar os dados dos arquivos
        oportunidades = pd.read_json(caminho_oportunidades)
        sellout = pd.read_parquet(caminho_sellout)

        print("Dados extraídos com sucesso.")
        print(f"Oportunidades - Colunas: {oportunidades.columns.tolist()}")
        print(f"Sellout - Colunas: {sellout.columns.tolist()}")

        # Salvar os arquivos localmente
        caminho_temp = "/opt/airflow/dags/temp/"
        os.makedirs(caminho_temp, exist_ok=True)

        caminho_oportunidades_temp = os.path.join(caminho_temp, "oportunidades.json")
        caminho_sellout_temp = os.path.join(caminho_temp, "sellout.parquet")

        oportunidades.to_json(caminho_oportunidades_temp, orient='records')
        sellout.to_parquet(caminho_sellout_temp, index=False)

        # Passar os caminhos para a próxima tarefa via XCom
        ti.xcom_push(key='caminho_oportunidades', value=caminho_oportunidades_temp)
        ti.xcom_push(key='caminho_sellout', value=caminho_sellout_temp)
    except Exception as e:
        raise Exception(f"Erro na etapa de extração: {e}")

# Função para transformação de dados
def transform(ti):
    try:
        print("Transformando dados...")

        # Recuperar os caminhos dos arquivos da etapa de extração
        caminho_oportunidades = ti.xcom_pull(task_ids='extract_data', key='caminho_oportunidades')
        caminho_sellout = ti.xcom_pull(task_ids='extract_data', key='caminho_sellout')

        # Carregar os dados dos arquivos
        oportunidades = pd.read_json(caminho_oportunidades)
        sellout = pd.read_parquet(caminho_sellout)

        # Verificar se a coluna 'Valor_Unitario' está presente
        if 'Valor_Unitario' not in oportunidades.columns:
            print(f"Colunas disponíveis em oportunidades: {oportunidades.columns.tolist()}")
            raise Exception("A coluna 'Valor_Unitario' não existe no DataFrame oportunidades.")

        # Exemplo de transformação: Criar uma nova coluna calculada
        oportunidades['valor_ajustado'] = oportunidades['Valor_Unitario'] * 1.1

        print("Transformações concluídas.")
        print(f"Oportunidades transformadas - Amostra: {oportunidades.head()}")

        # Salvar o resultado em um arquivo temporário
        caminho_temp = "/opt/airflow/dags/temp/"
        os.makedirs(caminho_temp, exist_ok=True)

        caminho_transformado = os.path.join(caminho_temp, "oportunidades_transformadas.json")
        oportunidades.to_json(caminho_transformado, orient='records')

        # Passar o caminho do arquivo transformado para a próxima tarefa via XCom
        ti.xcom_push(key='caminho_transformado', value=caminho_transformado)
    except Exception as e:
        raise Exception(f"Erro na etapa de transformação: {e}")

# Função para carregamento de dados
def load(ti):
    try:
        print("Carregando dados no MySQL...")

        # Recuperar o caminho do arquivo transformado
        caminho_transformado = ti.xcom_pull(task_ids='transform_data', key='caminho_transformado')

        # Carregar os dados transformados
        dados_transformados = pd.read_json(caminho_transformado)

        # Conectar ao MySQL
        connection = connect_mysql()
        cursor = connection.cursor()

        # Inserir dados na tabela 'fato_registro_oportunidade'
        for index, row in dados_transformados.iterrows():
            sql_oportunidades = """
                INSERT INTO fato_registro_oportunidade (data_registro, quantidade, status, nome_parceiro, 
                cnpj_parceiro, telefone_parceiro, categoria_produto, nome_produto, valor_unitario, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_oportunidades, (
=======
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
>>>>>>> 66d1a5d39effd0a9288254e578788d8884464d4b
                row['data_registro'],
                row['quantidade'],
                row['status'],
                row['nome_parceiro'],
                row['cnpj_parceiro'],
                row['telefone_parceiro'],
                row['categoria_produto'],
                row['nome_produto'],
<<<<<<< HEAD
                row['Valor_Unitario'],
                row['valor_ajustado']
            ))

        # Inserir dados na tabela 'fato_sellout'
        for index, row in dados_transformados.iterrows():
            sql_sellout = """
                INSERT INTO fato_sellout (id_oportunidade, cnpj_parceiro, nome_parceiro, data_venda, valor_total)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql_sellout, (
                row['id_oportunidade'],
                row['cnpj_parceiro'],
                row['nome_parceiro'],
                row['data_registro'],
                row['valor_ajustado']  # Ajuste conforme necessário para o valor da venda
            ))

        # Commit para garantir que os dados sejam gravados
        connection.commit()

        # Fechar a conexão
        cursor.close()
        connection.close()
=======
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
>>>>>>> 66d1a5d39effd0a9288254e578788d8884464d4b

        print("Dados carregados com sucesso no MySQL.")

    except Exception as e:
        print(f"Erro durante a carga dos dados: {e}")

<<<<<<< HEAD
# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
=======
    finally:
        # Fechar a conexão com o banco de dados
        cursor.close()
        conn.close()
>>>>>>> 66d1a5d39effd0a9288254e578788d8884464d4b

# Definição do DAG no Airflow
dag = DAG(
<<<<<<< HEAD
    'etl_airflow_mysql',
    default_args=default_args,
    description='ETL com Airflow e MySQL',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
=======
    'etl_pipeline_FINAL',
    description='ETL Pipeline utilizando Airflow',
    schedule='@daily',  # Usando o novo parâmetro 'schedule'
    start_date=datetime(2025, 1, 23),
>>>>>>> 66d1a5d39effd0a9288254e578788d8884464d4b
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

<<<<<<< HEAD
load_task = PythonOperator(
=======
t3 = PythonOperator(
>>>>>>> 66d1a5d39effd0a9288254e578788d8884464d4b
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,  # Remover se não necessário
    dag=dag,
)

<<<<<<< HEAD
# Definindo a ordem de execução
extract_task >> transform_task >> load_task
=======
# Definindo a ordem de execução das tarefas
t1 >> t2 >> t3

>>>>>>> 66d1a5d39effd0a9288254e578788d8884464d4b
