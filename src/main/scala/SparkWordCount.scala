import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    SparkWordCountApplication.read
  }
}

object SparkWordCountApplication {
  val README_FILE_PATH = "/Users/inhochoi/Downloads/spark-2.2.0-bin-hadoop2.7/README.md"
  val conf = new SparkConf().setAppName("Spark Word Count Class").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def read: Unit = {
    val lines = sc.textFile(README_FILE_PATH)
      .flatMap(line => line.split(" "))
    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a+b)

    counts.foreach(println)

    val max = counts.sortByKey(true).first()

    println(s"Max is $max")
  }
}