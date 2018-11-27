# Imports
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import *


# Cria context
sc = SparkContext("local", "Teste")
sql_context = SQLContext(sc)

# Schema df reviews
campos_integer = ["id","points"]
campos_string = ["country","description","designation","province","region_1","region_2","taster_name","taster_twitter_handle","title","variety","winery"]
campos_double = ["price"]

campos  = [StructField(campo, IntegerType(), True) for campo in campos_integer]
campos += [StructField(campo, StringType(), True) for campo in campos_string]
campos += [StructField(campo, DoubleType(), True) for campo in campos_double]

schema_reviews = StructType(campos)

# Lê csv Df reviews
df_reviews = sql_context.read.csv(r"C:\Users\maximilian.erhard\Downloads\bigdata_test\reviews.csv", header=True, schema=schema_reviews, encoding="UTF-8")

# Exibe Schema
df_reviews.printSchema()

# Quantos registros com vinhos franceses existem no dataset?
df_reviews[df_reviews["country"] == "France"].count()
registros_vinhos_franceses = df_reviews.filter(df_reviews["country"] == "France").count()
registros_vinhos_franceses.write.parquet(r"C:\Users\maximilian.erhard\Downloads\bigdata_test\registros_vinhos_franceses.parquet")

# Qual a província Italiana com o maior número de reviews no dataset?
prov = df_reviews.filter("country" == 'Italy'")["province"].groupBy("province").count().withColumnRenamed("count", "contagem_reviews").orderBy($"contagem_reviews".desc).show(1)
# Como não consigo executar para testar entao chamarei de "prov"

# Dentro dessa província, quem fez mais reviews?
sommelier = df_reviews.filter("province" == 'prov'").groupBy("taster_name").count().withColumnRenamed("count", "qtd_reviews").orderBy($"qtd_reviews".desc).show(1)
# Como não consigo executar para testar entao chamarei de "sommelier"

# Salve todas reviews desta pessoa no formato JSON com as seguintes colunas: designation, points, price, title, variety e winery.
df_provador = df_reviews[["designation","points","price","title","variety","winery"]].filter("taster_name == 'sommelier'").collect()
df_provador.write.json(r"C:\Users\maximilian.erhard\Downloads\bigdata_test\sommelier.json")

# Qual o vinho com mais pontos da variedade mais comentada?
variedade_mais_comentada = df_reviews["variety"].groupBy("variety").count().withColumnRenamed("count", "qtd_variedade").orderBy($"qtd_variedade".desc).take(1)
Vinho maior pontuado = df_reviews.filter("variety == 'variedade_mais_comentada'").groupby("title").sum("points").withColumnRenamed("sum", "soma_pontos").orderBy($"soma_pontos".desc).show(1)
