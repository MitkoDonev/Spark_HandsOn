from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Create a SparkSession
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up data and convert it to the format MLLib expects.
    inputLines = spark.sparkContext.textFile("./datasets/height_weight.txt")
    data = inputLines.map(lambda x: x.split(",")).map(
        lambda x: (float(x[0]), Vectors.dense(float(x[1])))
    )

    # Convert RDD to a DataFrame
    colNames = ["label", "features"]
    df = data.toDF(colNames)

    # Split data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Create linear regression model
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Train the model training data
    model = lir.fit(trainingDF)

    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted values
    for prediction in predictionAndLabel:
        print(prediction)

    # Stop the session
    spark.stop()
