# -*- coding: utf-8 -*-
"""pyspark.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1uYI25k21oyF77R6PPFxqULCehJUJg1Vx

# PREPARAÇÃO AMBIENTE - SPARK
"""

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

!wget -q https://downloads.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz

!tar xf spark-3.2.4-bin-hadoop3.2.tgz

!pip install -q findspark

!pip install -q pyspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"
os.environ["SPARK_HOME"] = "/content/spark-3.2.4-bin-hadoop3.2/"

"""Começando a trabalhar com o Spark


"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

dataset = spark.read.csv('/content/sample_data/california_housing_test.csv',inferSchema=True, header =True)

dataset.printSchema()

dataset.head()

dataset.createOrReplaceTempView("tabela_temporaria")
print(spark.catalog.listTables())

query = "FROM tabela_temporaria SELECT longitude, latitude LIMIT 3"
saida = spark.sql(query)
saida.show()

query1 = "SELECT MAX(total_rooms) as maximo_quartos FROM tabela_temporaria"
q_maximo_quartos = spark.sql(query1)
pd_maximo_quartos = q_maximo_quartos.toPandas()
print('A quantidade máxima de quartos é: {}'.format(pd_maximo_quartos['maximo_quartos']))
qtd_maximo_quartos = int(pd_maximo_quartos.loc[0,'maximo_quartos'])

"""Convertendo Pandas DataFrame para Spark DataFrame"""

import pandas as pd
import numpy as np
media = 0
desvio_padrao=0.1
pd_temporario = pd.DataFrame(np.random.normal(media,desvio_padrao,100))
spark_temporario = spark.createDataFrame(pd_temporario)
print(spark.catalog.listTables())
spark_temporario.createOrReplaceTempView("nova_tabela_temporaria")
print(spark.catalog.listTables())

spark.stop()

"""# Práticas no PySpark

TRANSFORMAÇÃO

Exemplo 1
"""

from pyspark import SparkContext
spark_contexto = SparkContext()

import numpy as np

vetor = np.array([10, 20, 30, 40, 50])

paralelo = spark_contexto.parallelize(vetor)

print(paralelo)

mapa = paralelo.map(lambda x : x**2+x)

mapa.collect()

"""Exemplo 2"""

paralelo = spark_contexto.parallelize(["distribuida", "distribuida", "spark", "rdd", "spark", "spark"])

funcao_lambda = lambda x:(x,1)

from operator import add
mapa = paralelo.map(funcao_lambda).reduceByKey(add).collect()
for (w, c) in mapa:

  print("{}: {}".format(w, c))

spark_contexto.stop()

"""AÇÃO"""

from pyspark import SparkContext

spark_contexto = SparkContext()
lista = [1, 2, 3, 4, 5, 3]
lista_rdd = spark_contexto.parallelize(lista)
lista_rdd.count()

par_ordenado = lambda numero: (numero, numero*10)

lista_rdd.flatMap(par_ordenado).collect()

lista_rdd.map(par_ordenado).collect()

spark_contexto.stop()