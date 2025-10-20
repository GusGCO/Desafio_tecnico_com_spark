# ETL Pipeline com PySpark

Este repositório contém um pipeline ETL (Extração, Transformação e Carga) utilizando PySpark para processar arquivos de dados e gerar resumos e balanços a partir de arquivos de clientes e vendas.

## Visão Geral

O pipeline realiza as seguintes etapas:

1. **Extração (Extract)**:
   - **Clientes**: Lê um arquivo CSV contendo informações de clientes (ex.: `clientes.csv`).
   - **Vendas**: Lê um arquivo de texto (formato `.txt`) contendo registros de vendas (ex.: `vendas.txt`).

2. **Transformação (Transform)**:
   - **Clientes**: Converte os dados dos clientes para tipos adequados (ex.: ID dos clientes como `Integer`, data de nascimento como `Date`).
   - **Vendas**: Realiza o parsing do arquivo de texto de vendas, convertendo as colunas para os tipos adequados, incluindo o cálculo do valor total e do ticket médio.

3. **Carga (Load)**:
   - Gera dois arquivos CSV de saída:
     - **Resumo dos Clientes** (`resumo_clientes.csv`): Contém o total de vendas, a quantidade de vendas e o ticket médio por cliente.
     - **Balanço dos Produtos** (`balanco_produtos.csv`): Contém o total de vendas, a quantidade de vendas e o ticket médio por produto.

## Arquivos Necessários

### Arquivos de Entrada:
1. **clientes.csv** - Arquivo CSV com informações dos clientes. O arquivo deve ter as seguintes colunas:
   - `cliente_id`: ID único do cliente.
   - `nome`: Nome do cliente.
   - `data_nascimento`: Data de nascimento do cliente no formato `dd/MM/yyyy`.

2. **vendas.txt** - Arquivo de texto com informações das vendas. O formato do arquivo deve ser o seguinte:
   - `venda_id`: ID da venda (5 caracteres).
   - `cliente_id`: ID do cliente (5 caracteres).
   - `produto_id`: ID do produto (5 caracteres).
   - `valor`: Valor da venda (8 caracteres).
   - `data_venda`: Data da venda (8 caracteres no formato `yyyyMMdd`).

### Arquivos de Saída:
1. **resumo_clientes.csv**: Contém o resumo dos clientes, com as colunas:
   - `cliente_id`: ID do cliente.
   - `nome`: Nome do cliente.
   - `total_vendas`: Total de vendas realizadas pelo cliente.
   - `quantidade_vendas`: Quantidade de vendas realizadas pelo cliente.
   - `ticket_medio`: Ticket médio das vendas do cliente.

2. **balanco_produtos.csv**: Contém o balanço dos produtos, com as colunas:
   - `produto_id`: ID do produto.
   - `total_vendas_produto`: Total de vendas realizadas do produto.
   - `quantidade_vendas_produto`: Quantidade de vezes que o produto foi vendido.
   - `ticket_medio_produto`: Ticket médio das vendas do produto.

## Tecnologias Utilizadas

- **PySpark**: Framework de processamento de dados distribuídos.
- **Pandas**: Biblioteca para manipulação de dados em Python, utilizada para salvar os resultados como CSV.
- **SparkSession**: Contexto de execução para PySpark.

## Instruções para Execução

### 1. Instalar Dependências

Certifique-se de ter o PySpark instalado. Caso não tenha, instale-o via `pip`:

```bash
pip install pyspark
