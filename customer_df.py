from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
)

spark = SparkSession.builder.appName("CusmoterOrders").master("local[*]").getOrCreate()

schema = StructType(
    [
        StructField("UserID", IntegerType(), True),
        StructField("OrderID", IntegerType(), True),
        StructField("Price", FloatType(), True),
    ]
)

df = spark.read.schema(schema).csv("./datasets/customer-orders.csv")
df.printSchema()

grouped_by_customerID = df.groupBy("UserID").agg(
    func.round(func.sum("Price"), 2).alias("total")
)

sorted_total = grouped_by_customerID.sort(grouped_by_customerID["total"].desc())
sorted_total.show(sorted_total.count())

spark.stop()
