from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("WordCounter").getOrCreate()

input = spark.read.text("./datasets/Book.txt")

words = input.select(func.explode(func.split(input.value, "\\W+")).alias("word"))
words.filter(words["word"] != "")

# Normalize to lowercase
lowercase_words = words.select(func.lower(words["word"]).alias("word"))

# Count words
word_count = lowercase_words.groupBy("word").count()

# Filter
filter_words = word_count.filter(word_count["count"] > 500)

# Sort by count and display
word_count_sort = filter_words.sort(filter_words["count"].desc()).show()
