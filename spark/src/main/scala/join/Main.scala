package join
import java.util.logging.Logger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

object Main extends App {
  Settings.read(args) match {
    case Left(error) =>
      System.err.println(s"Error: $error")
      System.exit(-1)

    case Right(settings) =>
      val log = Logger.getLogger("Main")

      val spark = SparkSession
        .builder()
        .master(s"local[${settings.threads}]")
        .appName("join-spark")
        .getOrCreate()

      log.info("Started")

      val left = spark.read
        .option("head", "true")
        .option("delimiter", "\t")
        .schema(
          StructType(
            Array(StructField("id", LongType), StructField("left", StringType))
          )
        )
        .csv(settings.left.getCanonicalPath)

      val right = spark.read
        .option("head", "true")
        .option("delimiter", "\t")
        .schema(
          StructType(
            Array(StructField("id", LongType), StructField("right", StringType))
          )
        )
        .csv(settings.right.getCanonicalPath)

      val result =
        left.join(right, Seq("id"), joinType = settings.jointType.toString)

      result
        .coalesce(1)
        .write
        .option("head", "false")
        .option("delimiter", "\t")
        .option("nullValue", null)
        .option("treatEmptyValuesAsNulls", "false")
        .csv("/tmp/spark-result")

      log.info("Completed")

      spark.close()
      System.exit(0)
  }
}
