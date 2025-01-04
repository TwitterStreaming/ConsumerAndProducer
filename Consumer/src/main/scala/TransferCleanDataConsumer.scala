import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.Paths

object TransferCleanDataConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Transfer Clean Data Consumer")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    val socketStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "raw-tweets")
      .load()

    val tweetStream = socketStream.selectExpr("CAST(value AS STRING) AS value")

    val schema = new StructType()
      .add("text", StringType)
      .add("created_at", StringType)
      .add("geo", new StructType().add("coordinates", ArrayType(DoubleType)))

    val tweetsDF = tweetStream.select(from_json(col("value"), schema).alias("data"))
      .select("data.*")

    val transformedDF = tweetsDF.select(
      col("text"),
      to_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss Z yyyy").alias("created_at"),
      col("geo.coordinates").alias("geoCoordinates")
    )

    val geoTransformedDF = transformedDF.withColumn("geo_coordinates", struct(
        expr("geoCoordinates[0]").alias("lat"),
        expr("geoCoordinates[1]").alias("lon")
      ))
      .drop("geoCoordinates")

    val hashtagsDF = geoTransformedDF.withColumn(
      "hashtags",
      array_distinct(expr("filter(split(text, ' '), x -> x like '#%')"))
    )

    val textCleaner = new CleanText(spark);
    val cleanedDF = textCleaner.clean(hashtagsDF, "text", "text")

    val osName = System.getProperty("os.name").toLowerCase

    val checkpointLocation = if (osName.contains("win")) {
      "C:/kafka/bigdata/kafka"
    } else {
      Paths.get(System.getProperty("java.io.tmpdir"), "bigdata", "kafka").toString
    }

    val finalDF = cleanedDF
    val query = finalDF
      .selectExpr(
        "to_json(struct(text, created_at, geo_coordinates, hashtags)) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "transform-tweets")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}