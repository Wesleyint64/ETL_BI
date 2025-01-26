# ETL_BI
## Wesley dos Santos de Lima

Este repositório contém um projeto de **ETL (Extract, Transform, Load)** desenvolvido para integração e análise de dados. O objetivo é facilitar a manipulação de dados utilizando o **Apache Airflow**, criando pipelines eficientes para transformar e carregar os dados em um formato útil para Business Intelligence (BI).

---

## 📂 Estrutura do Projeto

### Diretórios
- **dags/**: Contém os scripts principais do Apache Airflow para os pipelines de ETL.
  - `etl_pipeline_entrega.py`: Pipeline principal de ETL.
  - `doc.py` e `doc2.py`: Scripts auxiliares ou documentações adicionais.
- **dags/database/**: Arquivos de dados de exemplo usados nos pipelines.
  - `registros_oportunidades.json`: Dados em formato JSON.
  - `sellout.parquet`: Dados em formato Parquet.
- **dags/sql/**: Scripts SQL para manipulação ou criação de tabelas.
  - `create_tables.sql`: Scripts para criar as tabelas necessárias no banco de dados.
  - `EXECUCAO_DO_DAG_DE_ETL_ARIFLOW.docx`: Documento com instruções detalhadas para configuração e execução do DAG de ETL.
  - `Relatorio_Explicativo_do_ETL_Airflow.pdf`: Relatório descrevendo o desenvolvimento de um pipeline ETL utilizando o Apache Airflow.
- **docs/**: Documentos gerais e relatórios adicionais.
  - `Relatorio_Explicativo_ETL.pdf`: Relatório explicando as etapas do processo de ETL.
  - `Relatorio_Orquestracao_Airflow_Docker.pdf`: Relatório detalhando a configuração do Airflow com Docker e as instruções de execução.

### Arquivos
- **docker-compose.yaml**: Configuração do ambiente Docker para rodar o Apache Airflow.
- **.gitignore**: Lista de arquivos e diretórios ignorados pelo Git.
- **Extração_dados.xlsx**: Arquivo com a extração dos dados iniciais da database (primeiro desenvolvimento).

---

## 🚀 Tecnologias Utilizadas
- **Python**: Linguagem principal para os scripts de ETL.
- **Apache Airflow**: Orquestração de pipelines.
- **Docker**: Contêiner para gerenciar o ambiente de execução.
- **SQL**: Scripts para manipulação de banco de dados.

---

## 📋 Como Utilizar

### Pré-requisitos
- **Docker** e **Docker Compose** instalados na máquina.
- Python 3.8+.

### Passos
1. Clone este repositório:
   ```bash
   git clone https://github.com/Wesleyint64/ETL_BI.git
   ```
2. Navegue até o diretório do projeto:
   ```bash
   cd ETL_BI
   ```
3. Inicie o ambiente Docker:
   ```bash
   docker-compose up -d
   ```
4. Acesse o Apache Airflow via navegador:
   - URL: `http://localhost:8080`
   - Credenciais padrão: `airflow` / `airflow`

5. Ative e execute os DAGs disponíveis no painel do Airflow.

---

## ✨ Funcionalidades
- Pipeline ETL customizável.
- Integração com múltiplos formatos de dados (JSON, Parquet, etc.).
- Scripts SQL para manipulação de dados.
- Configuração completa via Docker.
- Relatórios explicativos detalhados para o acompanhamento do processo.

---

## 🤝 Contribuição
Contribuições são bem-vindas! Sinta-se à vontade para abrir uma **issue** ou enviar um **pull request** com melhorias.

---

## 📄 Licença
Este projeto está sob a licença [MIT](https://opensource.org/licenses/MIT).

---

## 📞 Contato
Se tiver dúvidas ou sugestões, entre em contato:
- **Autor**: Wesley dos Santos de Lima
- **E-mail**: wesleydslima1@gmail.com
- **GitHub**: [Wesleyint64](https://github.com/Wesleyint64)
