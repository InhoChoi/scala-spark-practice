import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CSVConverter {
  val SPLITER = ","
  val LANDING_TYPE_INDEX = 5
  val TAG = "NEW KEYWORD RESULT :"
  val LOG_PATH = "/Users/inho/Downloads/consoleFull.html"
  val conf: SparkConf = new SparkConf().setAppName("Spark Word Count Class").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val lines = sc.textFile(LOG_PATH)
    val filtered: RDD[String] = lines.filter(_.contains(TAG)).map(_.split(TAG)).map(_.last)
    val types = filtered.map(f => (f.split(SPLITER).apply(LANDING_TYPE_INDEX), 1))
    val groupBy = types.reduceByKey((a, b) => a + b)
    groupBy.foreach(println)
  }
}