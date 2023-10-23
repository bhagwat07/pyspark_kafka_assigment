from create_session import get_spark_object
import get_all_variable as gav
from udf import process_message_udf
from mysql_load import mysql_connection
from pyspark.sql.functions import row_number, col, hour, when, udf, desc
from pyspark.sql.window import Window
from preprocessdata import process_data

def main():
    #creating a spark object.
    spark = get_spark_object(gav.appName)

    #adding "C:\Bigdata\spark-3.1.2-bin-hadoop3.2\jars\mysql-connector-java-8.0.12.jar" jar into spark-jars folder.
    #advertiserDf loading from mysql.
    advertiser_df = mysql_connection(spark,gav.advertiser_tab)

    #loads the adslot_df table from mysql into dataframe
    adslot_df = mysql_connection(spark,gav.adslot)

    #reading kafka_streaming
    kafka_params = {
        "kafka.bootstrap.servers": gav.bootstrap_server,
        "subscribe": gav.topic,
        "startingOffsets": "earliest"}
    kafka_messages = (spark.readStream.format("kafka")
                      .options(**kafka_params)
                      .load()
                      )
    #register the udf
    process_df = kafka_messages.select(process_message_udf(kafka_messages).alias("processed_data"))

    # Join the message data with advertiser and adslot data
    joined_df = process_df.join(advertiser_df, ["advertiserId"], "left").join(adslot_df, ["adslotId"], "left")

    # filter the record using window function and rank the data (latest data start from 1)
    win = Window.partitionBy("uniqId", hour("date_time")).orderBy(desc("date_time"))
    joined_df = joined_df.withColumn("row_num", row_number().over(win))

    joined_df = joined_df.filter(col("row_num") == 1).dropDuplicates(["uniqId", "hour"])

    # 2.Apply the specified conditions
    process_df = process_data(joined_df)

    #writing output to the console
    query = (process_df.writeStream
              .outputMode("append")
              .format("console")
              .start()
              )

    # # Await the termination of the streaming query
    query.awaitTermination()

    #writing output to the HDFS. #Here I am writing to local
    # query1 = (process_df.writeStream
    #          .outputMode("append")
    #          .format("json")
    #          .option("path", "file:///D://python record")
    #          .option("checkpointLocation", "D://python record//chk")
    #          .start()
    #          )
    #

    # query1.awaitTermination()       #

    # # Start the streaming query and wait for it to terminate    #

if __name__ == '__main__':
    main()
