package org.spark_heuristics
package analyzer.heuristics

import java.time.Duration

class MultipleActionsWithoutCacheTest extends HeuristicTest {

  behavior of "MultipleActionsWithoutCache"

  it should "report if multiple actions are executed on the same dataframe without caching" in {
    // Simulate the case where a dataframe is processed with some expensive query and then has multiple actions executed on it.
    val eventLog = runSparkQueriesAndGetEvenLog { spark =>
      import spark.implicits._

      val accumulator = spark.sparkContext.longAccumulator
      val numPartitions = 5
      var df = spark.range(start = 0, end = numPartitions, step = 1, numPartitions = numPartitions)
      df = df.map { value =>
        Thread.sleep(Duration.ofSeconds(2).toMillis)
        accumulator.add(1)
        value
      }
      df.collect()
      df.count()
      assert(accumulator.value == numPartitions * 2)
    }

    val reports = MultipleActionsWithoutCache.check(eventLog, Map("MinQueryDurationSeconds" -> "1")).toSeq

    reports should have size 1
  }

  it should "not produce any reports if multiple actions are executed on the same dataframe after cache" in {
    // Simulate the case where a dataframe is processed with some expensive query and then has multiple actions executed on it.
    val eventLog = runSparkQueriesAndGetEvenLog { spark =>
      import spark.implicits._

      val accumulator = spark.sparkContext.longAccumulator
      val numPartitions = 5
      var df = spark.range(start = 0, end = numPartitions, step = 1, numPartitions = numPartitions)
      df = df.map { value =>
        Thread.sleep(Duration.ofSeconds(2).toMillis)
        accumulator.add(1)
        value
      }.cache
      df.collect()
      df.count()
      assert(accumulator.value == numPartitions)
    }

    val reports = MultipleActionsWithoutCache.check(eventLog, Map("MinQueryDurationSeconds" -> "1")).toSeq

    reports should have size 0

  }

}
