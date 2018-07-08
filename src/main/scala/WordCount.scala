import org.apache.spark.{SparkContext, SparkConf}

object WordCount {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wordCountTest")

    var sc = new SparkContext(conf)

    val input = sc.textFile("/root/resource/sparkfile/test.txt")
    val lines = input.flatMap(line => line.split(";"))
    val count = lines.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    val output = count.saveAsTextFile("/root/resource/sparkfile/re-test")

  }

}
