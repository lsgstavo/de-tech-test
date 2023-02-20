package io.lgos.challenge

import org.apache.spark.rdd._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object OddOccurrenceNumber {

  /**
   * Solution that counts all occurrences for each key value pair and filters those that are odd
   * Time complexity of this solution is dominated by the reduceByKey() transformation, which has a time complexity of
   * O(n), where n is the number of elements in the input RDD. map() operation is O(n) and collect() in the worst case
   * is O(m) where m is the number of different keys.
   * Thus, time complexity is O(n) in big O notation.
   * Space complexity is O(n) since it does not create any additional data structures that grow with the size of the RDD.
   * @param rdd input rdd containing key and values
   */
  def findOddOccurrences(rdd: RDD[(Int,Int)]): RDD[(Int,Int)] = {
    rdd.keyBy(identity)
      .map { case (key, _) => (key, 1) }
      .reduceByKey((a, b) => a + b)
      .collect {
        case x if x._2 % 2 != 0 => x._1
      }
  }

  /**
   * This solution creates a Set of values for each key, when merging two sets if the value is already there it will
   * be removed (it would become a pair) and if its not its added to the set. On the end, key value pairs that remain
   * are collected.
   * Time complexity of this solution is dominated by the reduceByKey() transformation, which has a time complexity of
   * O(n), where n is the number of elements in the input RDD. map() operation is O(n) and collect() in the worst case
   * is O(m) where m is the number of different keys.
   * The time complexity of the set operations union() and intersect() depends on the size of the sets being operated
   * on, but in the worst case, the time complexity of each set operation is O(n), so the time complexity of the entire
   * reduceByKey() transformation is O(n^2).
   * Since Set uses a hashtable for efficient lookups and due to the constraints in this problem (only one odd
   * occurrence per key) the actual time complexity of this operation is closer to O(n) than O(n^2).
   * The space complexity of this function is O(n), since it creates a Set for each Key containing at most the number
   * of elements for that key.
   * @param rdd input rdd
   * @return odd occurrences
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
   * Solution that counts all occurrence for each key value pair and filters those that are odd
   * Similar to findOddOccurrences but using dataframe api.
   * After query plan is generated and optimized by spark, the underlying transformations will have space time
   * complexity similar to findOddOccurrences
   * @param dataFrame input dataframe
   * @param spark spark session
   * @return odd occurrences
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
