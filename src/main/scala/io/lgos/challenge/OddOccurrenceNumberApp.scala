package io.lgos.challenge

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

/**
  * Use this to test the app locally, from sbt:
  * sbt "run input_path output_path aws_profile"
  *  (+ select OddOccurrenceNumberLocalApp when prompted)
  */
object OddOccurrenceNumberLocalApp extends App {
  private val (inputPath, outputPath, profile, alg) = (args(0), args(1), args(2), args.lift(3))

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Odd occurrence")

  Runner.run(conf, inputPath, outputPath, profile, alg)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  */
object OddOccurrenceNumberApp extends App {
  val (inputPath, outputPath, profile, alg) = (args(0), args(1), args(2), args.lift(3))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputPath, outputPath, profile, alg)
}

object Runner {

  def run(
    conf: SparkConf,
    inputPath: String,
    outputPath: String,
    profile: String,
    algorithm: Option[String]
  ): Unit = {
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    val provider = new ProfileCredentialsProvider(profile)

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", provider.getCredentials.getAWSAccessKeyId)

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", provider.getCredentials.getAWSSecretKey)

    val csv = spark.read
      .option("header", value = true)
      .option("sep", ",")
      .option("emptyValue", "0")
      .csv(s"$inputPath/*.csv")

    val tsv = spark.read
      .option("header", value = true)
      .option("sep", "\t")
      .option("emptyValue", "0")
      .csv(s"$inputPath/*.tsv")

    val keyValuePairs = tsv
      .withColumnRenamed(tsv.columns(0), "key")
      .withColumnRenamed(tsv.columns(1), "value")
      .union(
        csv
          .withColumnRenamed(csv.columns(0), "key")
          .withColumnRenamed(csv.columns(1), "value")
      )
      .withColumn("key", col("key").cast(IntegerType))
      .withColumn("value", col("value").cast(IntegerType))

    val rdd = keyValuePairs.select(col("key").as[Int], col("value").as[Int]).rdd

    val result = algorithm match {
      case Some("V1-DF") =>
        OddOccurrenceNumber.findOddOccurrencesDF(keyValuePairs, spark)
      case Some("V2") =>
        OddOccurrenceNumber.findOddOccurrencesV2(rdd).toDF("key", "value")
      case _ =>
        OddOccurrenceNumber.findOddOccurrences(rdd).toDF("key", "value")
    }

    result.write
      .mode(SaveMode.Overwrite)
      .option("sep", "\t")
      .csv(s"$outputPath/result.tsv")
  }

}
