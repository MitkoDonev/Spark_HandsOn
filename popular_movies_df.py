from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open(
        "./datasets/ml-100k/u.ITEM", "r", encoding="ISO-8859-1", errors="ignore"
    ) as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def lookupName(movieID):
    return nameDict.value[movieID]


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

# Create schema when reading u.data
schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# Load up movie data as dataframe
df = spark.read.option("sep", "\t").schema(schema).csv("./datasets/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
movieCounts = df.groupBy("movieID").count()

lookupNameUDF = func.udf(lookupName)

# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn(
    "movieTitle", lookupNameUDF(func.col("movieID"))
)

# Sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

# Grab the top 10
sortedMoviesWithNames.show(10, False)

# Stop the session
spark.stop()
