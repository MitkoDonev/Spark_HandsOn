from pyspark.sql import SparkSession, Row
from pyspark import SparkContext

nums = range(0, 10)


def seperator():
    print("#" * 100)


transactions = [
    {"name": "Bob", "amount": 100, "country": "United Kingdom"},
    {"name": "James", "amount": 15, "country": "United Kingdom"},
    {"name": "Marek", "amount": 51, "country": "Poland"},
    {"name": "Johannes", "amount": 200, "country": "Germany"},
    {"name": "Paul", "amount": 75, "country": "Poland"},
]

with SparkSession.builder.master("local[2]").config(
    "spark.sql.shuffle.partitions", 50
).getOrCreate() as spark:

    rdd = spark.sparkContext.parallelize(transactions).map(lambda x: Row(**x))

    df = spark.createDataFrame(rdd)

    print(f"Number of partitions: {df.rdd.getNumPartitions()}")
    print(f"Partitioner: {df.rdd.partitioner}")
    print(f"Partitions structure: {df.rdd.glom().collect()}")

    df2 = df.repartition("country")

    print("\nAfter 'repartion()'")
    print(f"Number of partitions: {df2.rdd.getNumPartitions()}")
    print(f"Partitioner: {df2.rdd.partitioner}")
    print(f"Partitions structure: {df2.rdd.glom().collect()}")

seperator()

with SparkSession.builder.master("local[2]").getOrCreate() as spark_2:
    nums_rdd = spark_2.sparkContext.parallelize(nums).map(lambda x: Row(2))

    nums_df = spark_2.createDataFrame(nums_rdd, ["num"])

    print(f"Number of partitions: {nums_df.rdd.getNumPartitions()}")
    print(f"Partitions structure: {nums_df.rdd.glom().collect()}")

    nums_df = nums_df.repartition(4)

    print(f"Number of partitions: {nums_df.rdd.getNumPartitions()}")
    print(f"Partitions structure: {nums_df.rdd.glom().collect()}")

seperator()

with SparkContext("local[2]") as sc:
    rdd = sc.parallelize(nums).map(lambda el: (el, el)).partitionBy(2).persist()

    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Partitions structure: {rdd.glom().collect()}")

    rdd2 = rdd.map(lambda el: (el[0], el[0] * 2))

    print(f"Number of partitions: {rdd2.getNumPartitions()}")
    print(f"Partitioner: {rdd2.partitioner}")
    print(f"Partitions structure: {rdd2.glom().collect()}")

    rdd3 = rdd.mapValues(lambda x: x * 2)

    print(f"Number of partitions: {rdd3.getNumPartitions()}")
    print(f"Partitioner: {rdd3.partitioner}")
    print(f"Partitions structure: {rdd3.glom().collect()}")
