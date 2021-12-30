from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType(
    [
        StructField("stationID", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

# // Read the file as dataframe
df = spark.read.schema(schema).csv("./datasets/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation = minTempsByStation.withColumnRenamed("min(temperature)", "min_temp")
minTempsByStation.show()

# Convert temperature to celcius and sort the dataset
minTempsByStationF = (
    minTempsByStation.withColumn(
        "temperature",
        func.round(func.col("min_temp") * 0.1, 2),
    )
    .select("stationID", "temperature")
    .sort("temperature")
)

minTempsByStationF_sorted = minTempsByStationF.sort(
    minTempsByStationF["temperature"].desc()
)

# Collect, format, and print the results
results = minTempsByStationF_sorted.collect()

for result in results:
    print(f"{result[0]} {result[1]:.2f}'C")

spark.stop()
