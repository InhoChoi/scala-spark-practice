object SparkWordCount {
  def main(args: Array[String]): Unit = {
    SparkWordCountApplication.read
  }
}

object SparkWordCountApplication {
  val README_FILE_PATH = "/Users/inhochoi/Downloads/spark-2.2.0-bin-hadoop2.7/README.md"
  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

  def read: Unit = {
    val textFile = spark.read.textFile(README_FILE_PATH).cached()
    val numA = textFile.filter(_.contains("a")).count()
    val numB = textFile.filter(_.contains("b")).count()
    println("A is $numA, B is $numB")
    spark.stop()
  }
}