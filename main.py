import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date
from pyspark.sql.types import IntegerType, FloatType, StringType

# Caminhos dos arquivos de entrada e saída
caminho_clientes = r"C:/Users/gusta/Downloads/clientes.csv"  # Caminho do arquivo clientes.csv
caminho_vendas = r"C:/Users/gusta/Downloads/vendas.txt"  # Caminho do arquivo vendas.txt
caminho_resumo_clientes = r"C:/Users/gusta/Downloads/output/resumo_clientes.csv"  # Caminho de saída para resumo_clientes.csv
caminho_balanco_produtos = r"C:/Users/gusta/Downloads/output/balanco_produtos.csv"  # Caminho de saída para balanco_produtos.csv

# Verificar se o diretório de saída existe; caso não, cria o diretório
output_dir = r"C:/Users/gusta/Downloads/output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Criação de uma sessão Spark
spark = SparkSession.builder \
    .appName("Desafio ETL PySpark") \
    .getOrCreate()

# Função para ler o arquivo CSV de clientes com PySpark
def ler_clientes(caminho_clientes):
    # Lê o arquivo CSV com Spark, garantindo a codificação UTF-8
    clientes = spark.read.option("delimiter", ";").option("header", "true").option("charset", "UTF-8").csv(caminho_clientes)
    # Exibe as primeiras linhas para verificação
    print("Dados dos clientes:", clientes.show(5))

    # Converte a coluna 'data_nascimento' para o tipo Data usando o formato correto
    clientes = clientes.withColumn("cliente_id", col("cliente_id").cast(IntegerType()))
    clientes = clientes.withColumn("data_nascimento", to_date(col("data_nascimento"), "dd/MM/yyyy"))

    return clientes

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, FloatType

# Função para ler o arquivo TXT de vendas com PySpark
def ler_vendas(caminho_vendas):
    # Lê o arquivo de vendas como um arquivo de texto
    vendas = spark.read.text(caminho_vendas)

    # Exibe as primeiras linhas para verificar como está estruturado
    vendas.show(5)

    # Extrai as colunas com base nas posições fixas
    vendas = vendas.select(
        col("value").substr(1, 5).alias("venda_id"),        # venda_id
        col("value").substr(6, 5).alias("cliente_id"),      # cliente_id
        col("value").substr(11, 5).alias("produto_id"),     # produto_id
        col("value").substr(16, 8).alias("valor"),          # valor
        col("value").substr(24, 8).alias("data_venda")     # data_venda
    )

    # Converte as colunas para os tipos corretos
    vendas = vendas.withColumn("venda_id", col("venda_id").cast(IntegerType()))
    vendas = vendas.withColumn("cliente_id", col("cliente_id").cast(IntegerType()))
    vendas = vendas.withColumn("produto_id", col("produto_id").cast(IntegerType()))
    vendas = vendas.withColumn("valor", col("valor").cast(FloatType()))

    # Substitui valores nulos ou não numéricos por 0
    vendas = vendas.fillna({"valor": 0.0})

    # Converte a coluna 'data_venda' para o tipo DataType
    vendas = vendas.withColumn("data_venda", to_date(col("data_venda"), "yyyyMMdd"))

    # Exibe os dados para verificação
    vendas.show(5)  # Mostrar as primeiras 5 linhas da tabela 'vendas' com a conversão aplicada

    return vendas

# Função para calcular os resumos por cliente
def resumo_clientes(clientes, vendas):
    # Realiza o join entre as tabelas de clientes e vendas usando a coluna cliente_id
    clientes_vendas = clientes.join(vendas, "cliente_id", "inner")

    # Exibe os dados após o join para verificação
    print("Resultado do Join (clientes e vendas):")
    clientes_vendas.show(5)  # Exibe as primeiras 5 linhas do join

    # Calcula o valor total de vendas, quantidade de vendas e ticket médio por cliente
    resumo = clientes_vendas.groupBy("cliente_id", "nome").agg(
        sum("valor").alias("total_vendas"),
        count("venda_id").alias("quantidade_vendas"),
        (sum("valor") / count("venda_id")).alias("ticket_medio")
    )

    # Converte o DataFrame do Spark para Pandas para salvar como CSV
    resumo_pd = resumo.toPandas()

    # Exibe o resumo para verificação
    print("Resumo de Clientes:")
    print(resumo_pd.head())  # Exibe as primeiras linhas do resumo

    return resumo_pd

# Função para calcular o balanço por produto
def balanco_produtos(vendas):
    # Calcula o total de vendas, quantidade de vendas e ticket médio por produto
    balanco = vendas.groupBy("produto_id").agg(
        sum("valor").alias("total_vendas_produto"),
        count("venda_id").alias("quantidade_vendas_produto"),
        (sum("valor") / count("venda_id")).alias("ticket_medio_produto")
    )

    # Converte o DataFrame do Spark para Pandas
    balanco_pd = balanco.toPandas()

    # Exibe o balanço por produto
    print("Balanço por Produto:", balanco_pd.head())

    return balanco_pd

# Função para salvar os resultados em arquivos CSV com Pandas
def salvar_resultados(resumo_clientes, balanco_produtos, caminho_resumo_clientes, caminho_balanco_produtos):
    # Salva o resumo de clientes em um arquivo CSV com ponto e vírgula como delimitador e codificação UTF-8
    resumo_clientes.to_csv(caminho_resumo_clientes, index=False, sep=";", encoding="utf-8")
    # Salva o balanço por produto em um arquivo CSV com ponto e vírgula como delimitador e codificação UTF-8
    balanco_produtos.to_csv(caminho_balanco_produtos, index=False, sep=";", encoding="utf-8")

# Função principal para rodar o pipeline ETL
def run_etl(caminho_clientes, caminho_vendas, caminho_resumo_clientes, caminho_balanco_produtos):
    # Leitura dos dados de clientes e vendas
    clientes = ler_clientes(caminho_clientes)
    vendas = ler_vendas(caminho_vendas)

    # Transformações: calcular resumo de clientes e balanço por produto
    resumo_clientes_df = resumo_clientes(clientes, vendas)
    balanco_produtos_df = balanco_produtos(vendas)

    # Salvando os resultados em arquivos
    salvar_resultados(resumo_clientes_df, balanco_produtos_df, caminho_resumo_clientes, caminho_balanco_produtos)

# Rodar o pipeline ETL
if __name__ == "__main__":
    # Rodar o pipeline ETL com os caminhos de entrada e saída
    run_etl(caminho_clientes, caminho_vendas, caminho_resumo_clientes, caminho_balanco_produtos)
    print("Pipeline ETL concluído com sucesso.")
