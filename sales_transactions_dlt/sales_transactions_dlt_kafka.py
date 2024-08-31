# Databricks notebook source
# MAGIC %md ## Overview
# MAGIC
# MAGIC ## DLT para pedidos 
# MAGIC
# MAGIC Essa primeira abordagem consiste em usarmos o Auto Loader do Databricks para ler os dados de pedidos em streaming de uma fila Kafka e construir um pipeline em Delta Live Table seguindo a arquitetura medalhão e com processamento em real time.
# MAGIC
# MAGIC PONTO DE ATENÇÃO: O Spark Kafka não faz inferência de schema, precisamos definí-lo previamente.
# MAGIC
# MAGIC
# MAGIC | Engineer Name           | Version | O.B.S.   | Date (dd/mm/yyyy) |
# MAGIC |-------------------------|---------|----------|------|
# MAGIC | Matheus Vasconcelos     | 1.0     | Creation | 30/08/2024 |

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.table(
  comment="Ingestão de dados de um tópico Kafka",
  table_properties={
    "pipelines.reset.allowed": reset
  }
)

topic = "<application-topic>" # Tópico relacionado a essa fila Kafka.

# Uma boa prática é colocar todas as credenciais no secrets, para evitar a exposição de chaves de API em nossos notebooks.

kafka_broker = dbutils.secrets.get(scope="kafka", key="server")
confluent_api_key = dbutils.secrets.get(scope="confluent_api", key="key")
confluent_api_secret = dbutils.secrets.get(scope="confluent_api", key="secret")


kafka_sales_bronze = (
    spark.readStream.format("kafka") 
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers", kafka_broker)
      .option("kafka.security.protocol", "SASL_SSL") # Colocar de acordo com o método de autenticação do ConfluentCloud.
      .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username ='{}' password='{}';"
              .format(confluent_api_key, confluent_api_secret))
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "earliest")
      .load()
  )

def kafka_sales():
  return kafka_sales_bronze

# COMMAND ----------

# MAGIC %md ## Definindo schema

# COMMAND ----------

schema = StructType([
    StructField("transaction_no", StringType()),
    StructField("date", StringType()),
    StructField("product_no", StringType()),
    StructField("product_name", StringType()),
    StructField("price", DoubleType(15,2)),
    StructField("quantity", IntegerType()),
    StructField("customer_no", StringType()),
    StructField("country", StringType())
])

# COMMAND ----------

# MAGIC %md ## Utilizando o schema acima

# COMMAND ----------

@dlt.table(
  comment="Tabela de pedidos para a camada bronze sales transactions de um tópico Kafka, dado as-is.",
  table_properties={
    "pipelines.reset.allowed": reset
  }
)

def sales_transactions_bronze():
    return (
        dlt.read_stream("kafka_sales") # Streams Kafka possuem os campos: timestamp, value. Onde 'value' contem nossa carga de dados. 
           .select(col("timstamp"),
                from_json(col("value").cast("string"), schema).alias("sales_transactions"))
           .select("timestamp", "sales_transactions.*")
    )

# COMMAND ----------

@dlt.table(
  comment="Tabela silver de pedidos, aqui faremos alguns tratamentos e transformações.",
  table_properties={
    "pipelines.reset.allowed": reset
  }
)

@dlt.expect_or_drop("valid quantity and price", "quantity > 0 and price > 0") # Processo de data quality, inserção de dados inválidos serão descartados.

def sales_transactions_silver():
  df = dlt.read_stream("sales_transactions_bronze")
  df = df.select("transaction_no", "date", "product_no", "product_name", "price",
    "quantity", "customer_no", "country")
  return df
