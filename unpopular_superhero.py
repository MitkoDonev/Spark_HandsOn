from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("UnpopularSuperhero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = spark.read.option("sep", " ").schema(schema).csv("./datasets/Marvel+Names.txt")
lines = spark.read.text("./datasets/Marvel+Graph.txt")

print("Names Schema")
names.printSchema()

# Add first ID
df_add_id = lines.withColumn("id", func.split(lines["value"], " ")[0])

# Add connections (remove - 1 as first entry is character id)
df_add_connections = df_add_id.withColumn(
    "connections", func.size(func.split(lines["value"], " ")) - 1
)

# Group by id
df_grouped = df_add_connections.groupBy("id").agg(
    func.sum(df_add_connections["connections"]).alias("connections")
)

# Min value
min_value = df_grouped.agg(
    func.min(df_grouped["connections"]).alias("min_value")
).first()[0]

# Sort most unpopular character
filter_unpopular = df_grouped.filter(df_grouped["connections"] == min_value)

# Connect id to name
minConnectionsWithNames = filter_unpopular.join(names, "id")

# Display
print(f"Following characters have only {min_value} connections!")
minConnectionsWithNames.select("name").show(10)

spark.stop()
