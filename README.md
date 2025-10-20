# ETL Pipeline com PySpark

Este repositório contém um pipeline ETL (Extração, Transformação e Carga) utilizando PySpark para processar arquivos de dados e gerar resumos e balanços a partir de arquivos de clientes e vendas.

# Documentação do Projeto - Análise de Dados com PySpark

Este repositório contém um código de análise de dados utilizando PySpark. O objetivo é realizar operações de transformação e agregação de dados de maneira eficiente e escalável.

## Pré-requisitos

Antes de executar o código, é necessário configurar o ambiente com o Apache Spark e as bibliotecas necessárias. Siga as etapas abaixo para realizar a instalação.

### Instalação do Apache Spark

1. **Instalação do Java**: O Apache Spark exige o Java 8 ou posterior. Se ainda não tiver o Java instalado, baixe e instale o JDK (Java Development Kit) em [Oracle JDK](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) ou use OpenJDK.
   
2. **Instalação do Apache Spark**:
   - Baixe a versão mais recente do Apache Spark em [Apache Spark Downloads](https://spark.apache.org/downloads.html).
   - Extraia o arquivo baixado em um diretório de sua escolha.
   - Defina as variáveis de ambiente no seu sistema:
     - `SPARK_HOME` como o diretório onde o Spark foi extraído.
     - Adicione o diretório `bin` do Spark ao seu `PATH`.

3. **Instalar o Hadoop** (opcional, caso necessário para a sua configuração). O Spark pode funcionar sem o Hadoop, mas se precisar, baixe a versão compatível com sua instalação do Spark em [Apache Hadoop Downloads](https://hadoop.apache.org/releases.html).

4. **Instalar o PySpark**:
   Execute o seguinte comando para instalar a biblioteca PySpark via `pip`:
   
   ```bash
   pip install pyspark
   
## Visão Geral

O pipeline realiza as seguintes etapas:

1. **Extração (Extract)**:
   - **Clientes**: Lê um arquivo CSV contendo informações de clientes (`clientes.csv`).
   - **Vendas**: Lê um arquivo de texto (formato `.txt`) contendo registros de vendas (`vendas.txt`).

2. **Transformação (Transform)**:
   - **Clientes**: Converte os dados dos clientes para tipos adequados (ID dos clientes como `Integer`, data de nascimento como `Date`).
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

### 2. Ajustar Caminhos de Arquivos

Antes de rodar o código, altere os caminhos dos arquivos de entrada e saída de acordo com o seu ambiente local. Os diretórios precisam ser ajustados nos seguintes trechos do código:

```python
caminho_clientes = r"C:/Users/gusta/Downloads/clientes.csv"
caminho_vendas = r"C:/Users/gusta/Downloads/vendas.txt"
output_dir = r"C:/Users/gusta/Downloads/output"
caminho_resumo_clientes = os.path.join(output_dir, "resumo_clientes.csv")
caminho_balanco_produtos = os.path.join(output_dir, "balanco_produtos.csv")
```

- **Caminho dos Arquivos de Entrada**: Os arquivos `clientes.csv` e `vendas.txt` precisam estar localizados no diretório especificado. Alterar os caminhos conforme necessário, dependendo de onde os arquivos de dados estão armazenados no seu ambiente local.

- **Caminho de Saída**: O diretório `output` será o local onde os arquivos de saída (`resumo_clientes.csv` e `balanco_produtos.csv`) serão salvos. Se o diretório não existir, ele será criado automaticamente.

### 3. Executar o Pipeline

Com os caminhos ajustados e as dependências instaladas, execute o pipeline. O código processará os arquivos de entrada, realizará as transformações e gerará os arquivos de saída no diretório especificado.

```python
# Inicialização do Spark
spark = SparkSession.builder.appName("Desafio ETL PySpark").getOrCreate()
```

### 4. Verificar os Resultados

Após executar o pipeline, os resultados estarão disponíveis nos arquivos de saída:

1. **resumo_clientes.csv**: Contém o resumo dos clientes, com as colunas: `cliente_id`, `nome`, `total_vendas`, `quantidade_vendas`, `ticket_medio`.
2. **balanco_produtos.csv**: Contém o balanço dos produtos, com as colunas: `produto_id`, `total_vendas_produto`, `quantidade_vendas_produto`, `ticket_medio_produto`.

Verifique esses arquivos no diretório de saída para analisar os resultados da transformação.

### Conclusão

Este pipeline ETL com PySpark é eficiente para processar grandes volumes de dados, realizar transformações e gerar resumos detalhados. Ele pode ser facilmente adaptado para diferentes conjuntos de dados, sendo escalável e modular. Certifique-se de ajustar os caminhos de arquivos conforme necessário e garantir que os arquivos de entrada estejam disponíveis antes de executar o pipeline.

Com a utilização do PySpark, é possível trabalhar com grandes volumes de dados de forma distribuída, o que torna o processo mais rápido e eficiente. Este projeto é uma excelente base para quem deseja trabalhar com processamento de dados em grande escala.
