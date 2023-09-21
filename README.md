# PySpark Jupyter Notebook

Este Jupyter Notebook foi criado automaticamente no Colaboratory e demonstra como configurar o ambiente Spark, carregar dados, executar consultas SQL, realizar transformações e ações básicas usando o Apache Spark em Python (PySpark).

## Preparação do Ambiente Spark

Para executar este notebook, primeiro é necessário configurar o ambiente do Spark. As etapas incluem a instalação do Java 8 e o download do Apache Spark. Além disso, as bibliotecas `findspark` e `pyspark` são instaladas. As variáveis de ambiente são configuradas para o Spark, e o Spark é inicializado.

```python
# Instalação do Java 8
!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Download e extração do Apache Spark
!wget -q https://downloads.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
!tar xf spark-3.2.4-bin-hadoop3.2.tgz

# Instalação das bibliotecas findspark e pyspark
!pip install -q findspark
!pip install -q pyspark

# Configuração das variáveis de ambiente para o Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"
os.environ["SPARK_HOME"] = "/content/spark-3.2.4-bin-hadoop3.2/"

# Inicialização do Spark
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
