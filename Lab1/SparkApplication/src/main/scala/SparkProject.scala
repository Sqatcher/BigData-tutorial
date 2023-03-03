import org.apache.spark.sql.SparkSession
object SparkProject extends App {
  val spark = SparkSession.builder
    .master("local[4]")
    .appName("Moja-aplikacja")
    .getOrCreate()

  val kolumny = Seq("Spark Version","Scala Version", "Date")
  val dane = Seq(("3.1.2", "2.12","May, 2021"), ("3.1.1", "2.12","Mar, 2021"))
  var dfFromData2 = spark.createDataFrame(dane).toDF(kolumny:_*)

  dfFromData2.show()
}