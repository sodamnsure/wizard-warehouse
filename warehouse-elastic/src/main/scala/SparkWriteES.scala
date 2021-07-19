import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.sparkDatasetFunctions

/**
 * @Author: sodamnsure
 * @Date: 2021/7/15 6:07 下午
 *
 */
object SparkWriteES {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkWriteES").master("local[4]").getOrCreate()
    // es.net.ssl参数设置为true，允许https连接
    val options = Map(
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "elastic-9194b4.es.us-east-1.aws.found.io",
      "es.port" -> "9243",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "********************",
      "es.net.ssl" -> "true",
      "es.mapping.id" -> "f_user_id"
    )

    // 读取csv文件
    val df =  spark.read.format("csv").option("header","true").load("/Users/sodamnsure/Desktop/Result_1.csv")

    df.printSchema()

    df.saveToEs("user/_doc", options)


    spark.stop()


  }

}
