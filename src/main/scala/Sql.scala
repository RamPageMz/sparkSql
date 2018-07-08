import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Sql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcOperation")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "7654321")
    val url = "jdbc:mysql://47.74.144.221:3306/spark?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    val unitShow = sqlContext.read.jdbc(url, "unit", properties)
    unitShow.show()

  }

}
