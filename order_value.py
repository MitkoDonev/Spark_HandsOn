from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrderCount")
spark = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    userID = int(fields[0])
    price = float(fields[2])
    return (userID, price)


lines = spark.textFile("./datasets/customer-orders.csv")
parsed_lines = lines.map(parseLine)
total_customer = parsed_lines.reduceByKey(lambda x, y: x + y)
# flipped_data = total_customer.map(lambda x: (x[1], x[0]))
# result = flipped_data.sortByKey()

results = total_customer.collect()

results = sorted(results, key=lambda x: x[1], reverse=True)

for result in results:
    if result[1] >= 6000:
        print(f"{result[0]}: {result[1]:.2f}")
