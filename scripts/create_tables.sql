-- Criação da tabela de Oportunidades
CREATE TABLE oportunidades (
    id_oportunidade INT PRIMARY KEY AUTO_INCREMENT,
    data_registro DATETIME,
    quantidade INT,
    status VARCHAR(100),
    nome_parceiro VARCHAR(255),
    cnpj_parceiro VARCHAR(20),
    telefone_parceiro VARCHAR(20),
    categoria_produto VARCHAR(100),
    nome_produto VARCHAR(255),
    valor_unitario DECIMAL(10, 2),
    valor_total DECIMAL(10, 2)
);

-- Inserção de dados de exemplo na tabela Oportunidades
INSERT INTO oportunidades (data_registro, quantidade, status, nome_parceiro, cnpj_parceiro, telefone_parceiro, categoria_produto, nome_produto, valor_unitario, valor_total)
VALUES 
('2025-01-01 10:00:00', 100, 'Em andamento', 'Parceiro A', '12.345.678/0001-99', '(11) 98765-4321', 'Eletrônicos', 'Smartphone', 1500.00, 150000.00),
('2025-01-02 11:30:00', 200, 'Finalizado', 'Parceiro B', '23.456.789/0001-88', '(21) 91234-5678', 'Eletrônicos', 'Laptop', 3000.00, 600000.00);

-- Criação da tabela de Sellout
CREATE TABLE sellout (
    id_sellout INT PRIMARY KEY AUTO_INCREMENT,
    data_fatura DATETIME,
    quantidade INT,
    nf VARCHAR(50),
    preco_unitario DECIMAL(10, 2),
    nome_parceiro VARCHAR(255),
    cnpj_parceiro VARCHAR(20),
    valor_total DECIMAL(10, 2)
);

-- Inserção de dados de exemplo na tabela Sellout
INSERT INTO sellout (data_fatura, quantidade, nf, preco_unitario, nome_parceiro, cnpj_parceiro, valor_total)
VALUES
('2025-01-01 14:00:00', 50, 'NF12345', 1500.00, 'Parceiro A', '12.345.678/0001-99', 75000.00),
('2025-01-02 15:00:00', 100, 'NF12346', 3000.00, 'Parceiro B', '23.456.789/0001-88', 300000.00);
