package org.spark_heuristics
package analyzer.heuristics

import java.time.Duration

class SingleQueryDuplicatedBranchesTest extends HeuristicTest {

  behavior of "check"

  it should "report if a single query has duplicated branches that can be cached" in {
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
      df.as("l").joinWith(df.as("r"), $"l.value" === $"r.value").collect()
      assert(accumulator.value == numPartitions * 2)
    }

    val reports = SingleQueryDuplicatedBranches.check(eventLog, Map("MinQueryDurationSeconds" -> "1")).toSeq

    reports should have size 1
  }

  it should "not report anything if a single query has duplicated branches that are cached" in {
    val eventLog = runSparkQueriesAndGetEvenLog { spark =>
      import spark.implicits._

      val accumulator = spark.sparkContext.longAccumulator
      val numPartitions = 5
      var df = spark.range(start = 0, end = numPartitions, step = 1, numPartitions = numPartitions)
      df = df
        .map { value =>
          Thread.sleep(Duration.ofSeconds(2).toMillis)
          accumulator.add(1)
          value
        }
        .cache()
      df.as("l").joinWith(df.as("r"), $"l.value" === $"r.value").collect()
      assert(accumulator.value == numPartitions)
    }

    val reports = SingleQueryDuplicatedBranches.check(eventLog, Map("MinQueryDurationSeconds" -> "1")).toSeq

    reports should have size 0
  }
}
