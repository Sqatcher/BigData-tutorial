from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("HelloWorld").getOrCreate()

    # kolumny = ["Spark Version", "Scala Version", "Date"]
    # dane = [("3.1.2", "2.12", "May, 2021"), ("3.1.1", "2.12", "Mar, 2021"), ("3.1.0", "2.12", "Jan, 2021")]
    # dfFromData = spark.createDataFrame(dane).toDF(*kolumny)
    # dfFromData.show()

    df = spark.read.format("csv").load("data.csv")

    def addRandom(num):
        return num + random.random() - 0.5


    df.select(col("gamesWon"), addRandom(col("gamesWon"))).show()


