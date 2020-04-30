package spark.glue

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import org.apache.spark.sql.DataFrame
import spark.implicits.spark

import scala.collection.JavaConverters._

object GlueFilterFraudIPs {

  def main(sysArgs: Array[String]) {

    val glueContext: GlueContext = new GlueContext(spark.sparkContext)

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val s3Views = glueContext
      .getCatalogSource(
        database = "filtered-data-from-fraud-ips",
        tableName = "aws_capstone_esoboliev",
        redshiftTmpDir = "",
        transformationContext = "s3Views")
      .getDynamicFrame()

    val dynamoDbFraudIps = glueContext
      .getCatalogSource(
        database = "filtered-data-from-fraud-ips",
        tableName = "fraud_ips_esoboliev",
        redshiftTmpDir = "",
        transformationContext = "dynamoDbFraudIps")
      .getDynamicFrame()

    val views: DataFrame = s3Views.toDF()
    val fraudIPs: DataFrame = dynamoDbFraudIps.toDF()
    val filteredDF: DataFrame = fraudIPs.join(views, views("user_ip") =!= fraudIPs("user_ip"))

    val applyMapping = DynamicFrame(filteredDF, glueContext)
      .applyMapping(mappings = Seq(
        ("item_id", "string", "item_id", "string"),
        ("timestamp", "int", "timestamp", "int"),
        ("device_type", "string", "device_type", "string"),
        ("device_id", "string", "device_id", "string"),
        ("user_ip", "string", "user_ip", "string"),
        ("year", "int", "year", "int"),
        ("month", "int", "month", "int"),
        ("day", "int", "day", "int"),
        ("hour", "int", "hour", "int")),
        caseSensitive = false,
        transformationContext = "applyMapping")

    glueContext
      .getSinkWithFormat(
        connectionType = "s3",
        options = JsonOptions("""{"path": "s3://aws-capstone-esoboliev"}"""),
        transformationContext = "datasink2",
        format = "json")
      .writeDynamicFrame(applyMapping)
    Job.commit()
  }
}

