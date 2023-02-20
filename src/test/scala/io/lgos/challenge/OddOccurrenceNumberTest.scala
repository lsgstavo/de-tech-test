package io.lgos.challenge

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class OddOccurrenceNumberTest extends AnyFunSuite with SharedSparkContext with DataFrameSuiteBase with RDDComparisons {

  def createRDDs: (RDD[(Int, Int)], RDD[(Int, Int)]) = {
    val linesRDD: RDD[(Int, Int)] =
      sc.parallelize(
        Seq(
          (1, 2),
          (1, 3),
          (1, 3),
          (2, 4),
          (2, 4),
          (2, 4),
        )
      )

    val expected: RDD[(Int, Int)] =
      sc.parallelize(
        Seq(
          (1, 2),
          (2, 4),
        )
      )
    (linesRDD, expected)
  }

  test("findOddOccurrences") {
    val (linesRDD, expected) = createRDDs
    val oddOccurrences = OddOccurrenceNumber.findOddOccurrences(linesRDD)
    assertRDDEquals(oddOccurrences, expected)
  }

  test("findOddOccurrencesV2") {
    val (linesRDD, expected) = createRDDs
    val oddOccurrences = OddOccurrenceNumber.findOddOccurrencesV2(linesRDD)
    assertRDDEquals(oddOccurrences, expected)
  }

  test("findOddOccurrencesDF") {
    import spark.implicits._
    val (linesRDD, expected) = createRDDs
    val dataframe = linesRDD.toDF("key", "value")
    val oddOccurrences = OddOccurrenceNumber.findOddOccurrencesDF(dataframe, spark)
    assertDataFrameEquals(oddOccurrences, expected.toDF("key", "value"))
  }
}
