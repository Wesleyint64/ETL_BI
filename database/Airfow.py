import pandas as pd
import mysql.connector

def extract_data():
    """
    Extrai os dados dos arquivos fornecidos.
    Retorna dois DataFrames: oportunidades e sellout.
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
    Realiza a transformação nos dados extraídos.
    Aplica limpeza e formatação conforme requisitos.
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

        # Gerando colunas adicionais
        oportunidades['data_registro'] = pd.to_datetime(oportunidades['data_registro'], unit='ms', errors='coerce')
        oportunidades['valor_total'] = oportunidades['quantidade'] * oportunidades['valor_unitario']

        # Criando IDs únicos
        oportunidades['id_oportunidade'] = range(1, len(oportunidades) + 1)

        print("Transformações no dataset de oportunidades concluídas.")

        print("Transformando dados de sellout...")

        # Renomeando colunas
        sellout.rename(columns={
            "Data_Fatura": "data_fatura",
            "Quantidade": "quantidade",
            "NF": "nf",
            "Valor_Unitario": "preco_unitario",
            "Nome Fantasia": "nome_parceiro",
            "CNpj Parceiro": "cnpj_parceiro",
        }, inplace=True)

        # Gerando colunas adicionais
        sellout['data_fatura'] = pd.to_datetime(sellout['data_fatura'], errors='coerce')
        sellout['valor_total'] = sellout['quantidade'] * sellout['preco_unitario']

        # Criando IDs únicos
        sellout['id_sellout'] = range(1, len(sellout) + 1)

        print("Transformações no dataset de sellout concluídas.")

        return oportunidades, sellout
    except Exception as e:
        print(f"Erro durante a transformação: {e}")
        return None, None

def load_data(oportunidades, sellout):
    """
    Carrega os dados transformados nas tabelas MySQL.
    """
    try:
        print("Conectando ao banco de dados MySQL...")
        # Estabelecendo a conexão com o banco de dados MySQL
        connection = mysql.connector.connect(
            host='localhost',        # Pode ser 'localhost' ou o IP do servidor MySQL
            user='root',             # Usuário do MySQL
            password='Sasuke110800@',    # Senha do MySQL
            database='analista_bi'   # Nome do banco de dados
        )
        cursor = connection.cursor()

        # Inserindo dados na tabela 'fato_registro_oportunidade'
        for index, row in oportunidades.iterrows():
            print(f"Inserindo dados na tabela fato_registro_oportunidade: {row}")
            cursor.execute("""
                INSERT INTO fato_registro_oportunidade (data_registro, quantidade, status, nome_parceiro, cnpj_parceiro, telefone_parceiro, categoria_produto, nome_produto, valor_unitario, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['data_registro'].strftime('%Y-%m-%d %H:%M:%S'),  # Convertendo para string
                row['quantidade'], row['status'], row['nome_parceiro'], row['cnpj_parceiro'],
                row['telefone_parceiro'], row['categoria_produto'], row['nome_produto'], row['valor_unitario'], row['valor_total']
            ))

        # Inserindo dados na tabela 'fato_sellout'
        for index, row in sellout.iterrows():
            print(f"Inserindo dados na tabela fato_sellout: {row}")
            cursor.execute("""
                INSERT INTO fato_sellout (data_fatura, quantidade, nf, preco_unitario, nome_parceiro, cnpj_parceiro, valor_total)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['data_fatura'].strftime('%Y-%m-%d %H:%M:%S'),  # Convertendo para string
                row['quantidade'], row['nf'], row['preco_unitario'], row['nome_parceiro'],
                row['cnpj_parceiro'], row['valor_total']
            ))

        # Commit para salvar as inserções
        connection.commit()
        print("Dados carregados com sucesso no banco de dados MySQL.")
        
    except mysql.connector.Error as e:
        print(f"Erro ao conectar ou inserir dados no MySQL: {e}")
    finally:
        # Fechando a conexão com o banco
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão com o banco de dados fechada.")

def main():
    """
    Função principal para execução do pipeline ETL.
    """
    print("Iniciando o pipeline ETL...")

    # Extração
    oportunidades, sellout = extract_data()

    if oportunidades is not None and sellout is not None:
        # Transformação
        oportunidades, sellout = transform_data(oportunidades, sellout)

        if oportunidades is not None and sellout is not None:
            # Carga
            load_data(oportunidades, sellout)
        else:
            print("Erro durante a transformação dos dados.")
    else:
        print("Erro durante a extração dos dados.")

if __name__ == "__main__":
    main()
