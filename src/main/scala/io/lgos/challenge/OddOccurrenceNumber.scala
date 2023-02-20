package io.lgos.challenge

import org.apache.spark.rdd._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object OddOccurrenceNumber {

  /**
   * Solution that counts all occurrence for each key value pair and filters those that are odd
   * @param rdd input rdd containing key and values
   */
  def findOddOccurrences(rdd: RDD[(Int,Int)]): RDD[(Int,Int)] = {
    rdd.keyBy(identity)
      .map { case (key, _) => (key, 1) }
      .reduceByKey((a, b) => a + b)
      .filter(x => x._2 % 2 != 0)
      .map(x => x._1)
  }

  /**
   *
   * @param rdd
   * @return
   */
  def findOddOccurrencesV2(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    rdd.keyBy(identity)
      .map { case (key, value) => (key, Set(value._2)) }
      .reduceByKey((a, b) => {
        a.union(b) -- a.intersect(b)
      })
      .collect { case (k, v) if (v.nonEmpty) =>
        k._1 -> v.head
      }
  }

  /**
   *
   * @param dataFrame
   * @param spark
   * @return
   */
  def findOddOccurrencesDF(dataFrame: DataFrame, spark: SparkSession): DataFrame = {
    dataFrame
      .withColumn("key", col("key").cast(IntegerType))
      .withColumn("value", col("value").cast(IntegerType))
      .groupBy("key", "value")
      .count()
      .where((col("count") % 2) =!= 0)
      .select(col("key"), col("value"))
  }
}
