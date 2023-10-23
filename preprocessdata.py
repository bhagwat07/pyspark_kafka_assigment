from pyspark.sql.functions import when,col
from udf import calculate_amt_udf
def process_data(df):
    calculated_stream = (df.withColumn("amount",calculate_amt_udf(df["event.show"],
                                                                            df["event.amount"],
                                                                            df["event.amount_charge"])))

    # Drop unnecessary columns
    combined_df = calculated_stream.select("advertiser_id","advertiser_name", "type_of_advertiser", "amount_charge", "output_location","output_data_format")

    return combined_df
