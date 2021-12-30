from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("PopularSuperhero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = spark.read.option("sep", " ").schema(schema).csv("./datasets/Marvel+Names.txt")
lines = spark.read.text("./datasets/Marvel+Graph.txt")

print("Names Schema:")
names.printSchema()

# Add first ID
df_add_id = lines.withColumn("id", func.split(func.col("value"), " ")[0])

# Add connections (remove - 1 as first entry is character id)
df_add_connections = df_add_id.withColumn(
    "connections", func.size(func.split(func.col("value"), " ")) - 1
)

# Group by id
df_grouped = df_add_connections.groupBy("id").agg(
    func.sum(df_add_connections["connections"]).alias("connections")
)

# Sort most popular character
most_popular = df_grouped.sort(func.col("connections").desc()).first()

# Connect id to name
most_popular_name = names.filter(func.col("id") == most_popular[0])

# Select name
most_popular_character = most_popular_name.select("name").first()

# Display
print(
    f"{most_popular_character.name} is the most popular superhero with {most_popular.connections} connections!"
)
