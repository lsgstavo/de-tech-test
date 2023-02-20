package io.lgos.challenge

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputPath.txt outputPath.txt"
  *  (+ select OddOccurrenceNumberLocalApp when prompted)
  */
object OddOccurrenceNumberLocalApp extends App {
  private val (inputPath, outputPath, profile) = (args(0), args(1), args(2))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Odd occurrence")

  Runner.run(conf, inputPath, outputPath, profile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object OddOccurrenceNumberApp extends App {
  val (inputPath, outputPath, profile) = (args(0), args(1), args(2))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputPath, outputPath, profile)
}

object Runner {
  def run(conf: SparkConf, inputPath: String, outputPath: String, profile: String): Unit = {
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val provider = new ProfileCredentialsProvider(profile)

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", provider.getCredentials.getAWSAccessKeyId)

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", provider.getCredentials.getAWSSecretKey)

    val csv = spark.read
      .option("header", value = true)
      .option("sep", ",")
      .csv(s"$inputPath/*.csv")

    val tsv = spark.read
      .option("header", value = true)
      .option("sep", "\t")
      .csv(s"$inputPath/*.tsv")

    val keyValuePairs = tsv
      .withColumnRenamed(tsv.columns(0), "key")
      .withColumnRenamed(tsv.columns(1), "value")
      .union(
        csv.withColumnRenamed(csv.columns(0), "key")
          .withColumnRenamed(csv.columns(1), "value")
      )

//    val rdd = keyValuePairs.select(col("key").as[Int], col("value").as[Int]).rdd

    val result = OddOccurrenceNumber.findOddOccurrencesDF(keyValuePairs, spark)
    result.write
      .option("sep", "\t")
      .csv(s"$outputPath/result.tsv")
  }
}
