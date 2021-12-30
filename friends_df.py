from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./datasets/fakefriends-header.csv")
)

print("Shema:")
lines.printSchema()

# Select age and number of friends
friends_by_age = lines.select("age", "friends")

# Group by age and calculate average friends
print("Group by age:")
friends_by_age.groupBy("age").avg("friends").show()

# Group by age, calculate average friends and sort by age
print("Group by and sort by age:")
friends_by_age.groupBy("age").avg("friends").sort("age").show()

# Group by, sort by age, calculate and format average friends
print("Format average friends:")
friends_by_age.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# Assign alias to new column
print("New column alias:")
friends_by_age.groupBy("age").agg(
    func.round(func.avg("friends"), 2).alias("friends_average")
).sort("age").show()
