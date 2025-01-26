import mysql.connector
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
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
                row['data_registro'],
                row['quantidade'],
                row['status'],
                row['nome_parceiro'],
                row['cnpj_parceiro'],
                row['telefone_parceiro'],
                row['categoria_produto'],
                row['nome_produto'],
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

        print("Dados carregados com sucesso no MySQL.")

    except Exception as e:
        raise Exception(f"Erro na etapa de carregamento: {e}")

# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_airflow_mysql',
    default_args=default_args,
    description='ETL com Airflow e MySQL',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
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
