from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("HelloWorld").getOrCreate()

    # kolumny = ["Spark Version", "Scala Version", "Date"]
    # dane = [("3.1.2", "2.12", "May, 2021"), ("3.1.1", "2.12", "Mar, 2021"), ("3.1.0", "2.12", "Jan, 2021")]
    # dfFromData = spark.createDataFrame(dane).toDF(*kolumny)
    # dfFromData.show()

    df = spark.read.option("header", True).format("csv").load("data.csv")

    def addRandom(num):
        numNoised = float(num) + random.random() - 0.5
        return numNoised

    addRandomUDF = udf(lambda x: addRandom(x), DoubleType())

    df = df.select(col("playerOne"), col("playerTwo"), col("gamesWon"), addRandomUDF(col("gamesWon")).alias("gamesWonWithNoise"))
    df.show()

    df.coalesce(1).write.option("header", True).csv("dataWithNoise")
