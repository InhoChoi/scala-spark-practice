import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    SparkWordCountApplication read
  }
}

object SparkWordCountApplication {
  val README_FILE_PATH: String = "/Users/inho/Downloads/spark-2.2.0-bin-hadoop2.7/README.md"
  val conf: SparkConf = new SparkConf().setAppName("Spark Word Count Class").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  def read: Unit = {
    val lines: RDD[String] = sc.textFile(README_FILE_PATH)
      .flatMap(line => line.split(" "))
    val pairs: RDD[(String, Int)] = lines.map(s => (s, 1))
    val groupbyCount: RDD[(String, Int)] = pairs.reduceByKey((a, b) => a + b)
    val letterCount: Double = lines.map(_.length)
      .sum()

    groupbyCount.foreach(println)
    groupbyCount.foreachPartition(foreachMethod)
    val objectRDD: RDD[KeyValueModel] = groupbyCount.map(convertToObject)

    objectRDD.foreach(println)
    val max: (String, Int) = groupbyCount.sortByKey(true).first()

    println(s"Max is $max")
    println(s"Total length is $letterCount")
  }

  def foreachMethod(input: Iterator[(String, Int)]) = {
    for (a <- input) {
    }
  }

  def convertToObject(input: (String, Int)) = KeyValueModel(input._1, input._2)

  case class KeyValueModel(key: String, value: Int)

}