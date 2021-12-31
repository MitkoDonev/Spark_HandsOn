from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    # Create a SparkSession
    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

    # Load up data as dataframe
    data = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("./datasets/realestate.csv")
    )

    assembler = (
        VectorAssembler()
        .setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"])
        .setOutputCol("features")
    )

    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    # Split data into training and testing
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Create decision tree
    dtr = (
        DecisionTreeRegressor()
        .setFeaturesCol("features")
        .setLabelCol("PriceOfUnitArea")
    )

    # Train the model using training data
    model = dtr.fit(trainingDF)

    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted values
    for prediction in predictionAndLabel:
        print(prediction)

    # Stop the session
    spark.stop()
