package org.bigdata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TransferCleanDataConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Tweet Processor")
      .master("local[*]")
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
      col("created_at"),
      col("geo.coordinates").alias("geo_coordinates")
    )

    val geoTransformedDF = transformedDF.withColumn("Longitude", expr("geo_coordinates[0]"))
      .withColumn("Latitude", expr("geo_coordinates[1]"))
      .drop("geo_coordinates")

    val hashtagsDF = geoTransformedDF.withColumn(
      "hashtags",
      array_distinct(expr("filter(split(text, ' '), x -> x like '#%')"))
    )

    val textCleaner = new CleanText(spark);
    val cleanedDF = textCleaner.clean(hashtagsDF, "text", "text")

    val finalDF = cleanedDF
    val query = finalDF
      .selectExpr(
        "to_json(struct(text, created_at, Longitude, Latitude, hashtags)) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "transform-tweets")
      .option("checkpointLocation", "C:/tmp/bigdata/kafka")
      .start()

    query.awaitTermination()
  }
}