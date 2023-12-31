from pyspark.sql import SparkSession


def get_spark_object(appName):
    spark = (
        SparkSession.builder.appName(appName)
        .master("local[*]")
        .config('spark.jars',"C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .config('spark.jars',"C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\mysql-connector-java-8.0.12.jar")
        .config("spark.executor.extraClassPath","C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .config("spark.executor.extraLibrary","C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .config("spark.driver.extraClassPath","C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\spark-sql-kafka-0-10_2.12-3.1.2.jar,C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\kafka-clients-3.1.2.jar")
        .getOrCreate()
    )
    return spark
