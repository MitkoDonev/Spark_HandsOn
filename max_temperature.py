from pyspark import SparkConf, SparkContext, files

conf = SparkConf().setMaster("local").setAppName("MaxTemperature")
spark = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    stationType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, stationType, temperature)


lines = spark.textFile("./datasets/1800.csv")
parsed_lines = lines.map(parseLine)
max_temps = parsed_lines.filter(lambda x: "TMAX" in x[1])
station_temps = max_temps.map(lambda x: (x[0], x[2]))
max_temps = station_temps.reduceByKey(lambda x, y: max(x, y))

results = max_temps.collect()

for result in results:
    print(f"{result[0]}: {result[1]:.2f}'C")
