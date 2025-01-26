import pandas as pd # type: ignore
from datetime import datetime

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
    Carrega os dados transformados em um arquivo Excel.
    """
    try:
        print("Carregando os dados transformados para o arquivo Excel...")
        with pd.ExcelWriter('dados_processados.xlsx') as writer:
            oportunidades.to_excel(writer, sheet_name='fato_registro_oportunidade', index=False)
            sellout.to_excel(writer, sheet_name='fato_sellout', index=False)
        print("Dados carregados com sucesso no arquivo 'dados_processados.xlsx'.")
    except Exception as e:
        print(f"Erro durante a carga: {e}")

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
