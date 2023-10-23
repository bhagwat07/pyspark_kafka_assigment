from pyspark.sql.functions import udf,col,from_json
from pyspark.sql.types import StringType,StructType,StructField,DoubleType


#defining a schema.
sch = StructType([
    StructField("date_time", StringType(), True),
    StructField("advertiserId", StringType(), True),
    StructField("adslotId", StringType(), True),
    StructField("uniqId", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("show", StringType(), True),
    StructField("amount", DoubleType(), True)
])
def calculate_amt(show, amount, amount_charge):
    if show == "yes":
        return amount
    elif amount_charge is not None:
        return amount_charge
    else:
        return 0.0

calculate_amt_udf = udf(calculate_amt)

def process_message(messages):
    structure_df =  (messages.select(from_json(col("value").cast(StringType()),sch).alias('value'))
                    .selectExpr("value.date_time","value.advertiserId","value.adslotId","value.uniqId","value.product","value.name","value.show","value.amount")
                     )

    return structure_df

#register the function.
process_message_udf = udf(process_message)


