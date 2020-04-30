package spark.s3

import com.audienceproject.spark.dynamodb.implicits._
import org.apache.spark.sql.DataFrame
import spark.implicits.spark


object ViewsAggregation {

  def main(args: Array[String]): Unit = {
    setHadoopConf()

    val filteredDF: DataFrame = spark
      .read
      .json(args(0))
      .groupBy("user_ip", "timestamp")
      .count
      .where("count > 5")
      .select("user_ip")
    filteredDF.write.dynamodb("fraud-ips-esoboliev")
  }

  private def setHadoopConf(): Unit = {
    // Set these env.vars. globally in your system or set while running a programm
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    if (accessKeyId != null && secretAccessKey != null) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKeyId)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    }
    spark.sparkContext.setLogLevel("ERROR")
  }
}
