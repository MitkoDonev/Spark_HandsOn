from pyspark import SparkContext


nums = range(0, 10)
print(nums)


def seperator():
    print("#" * 100)


with SparkContext("local") as sc:
    rdd = sc.parallelize(nums)

    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Partitions structure: {rdd.glom().collect()}")

seperator()

with SparkContext("local[2]") as sc_2:
    rdd = sc_2.parallelize(nums)

    print(f"Default parallelism: {sc_2.defaultParallelism}")
    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Partitions structure: {rdd.glom().collect()}")

seperator()

with SparkContext("local") as sc_3:
    rdd = sc_3.parallelize(nums, 15)

    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Partitions structure: {rdd.glom().collect()}")

seperator()

with SparkContext("local[2]") as sc_4:
    rdd = sc_4.parallelize(nums).map(lambda el: (el, el)).partitionBy(2).persist()

    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Partitions structure: {rdd.glom().collect()}")

seperator()

transactions = [
    {"name": "Bob", "amount": 100, "country": "United Kingdom"},
    {"name": "James", "amount": 15, "country": "United Kingdom"},
    {"name": "Marek", "amount": 51, "country": "Poland"},
    {"name": "Johannes", "amount": 200, "country": "Germany"},
    {"name": "Paul", "amount": 75, "country": "Poland"},
]


def country_partitioner(country):
    return hash(country)


num_partitions = 5

print(country_partitioner("Poland") % num_partitions)
print(country_partitioner("Germany") % num_partitions)
print(country_partitioner("United Kingdom") % num_partitions)

with SparkContext("local[2]") as sc_5:
    rdd = (
        sc_5.parallelize(transactions)
        .map(lambda el: (el["country"], el))
        .partitionBy(4, country_partitioner)
    )

    print(f"Number of partitions: {rdd.getNumPartitions()}")
    print(f"Partitioner: {rdd.partitioner}")
    print(f"Partitions structure: {rdd.glom().collect()}")

seperator()


def sum_sales(iterator):
    yield sum(transaction[1]["amount"] for transaction in iterator)


with SparkContext("local[2]") as sc_6:
    by_country = (
        sc_6.parallelize(transactions)
        .map(lambda el: (el["country"], el))
        .partitionBy(3, country_partitioner)
    )

    print(f"Partitions structure: {by_country.glom().collect()}")

    sum_amounts = by_country.mapPartitions(sum_sales).collect()

    print(f"Total sales for each partiion: {sum_amounts}")
