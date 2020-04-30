import org.apache.spark.sql.SparkSession

package object spark {

  object implicits {
    implicit val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Aws-App")
      .getOrCreate()
  }

}
