package org.spark_heuristics
package analyzer.heuristics

import event_log.EventLogReader

import org.apache.commons.io.FileUtils

import java.time.Duration

class SkewedTaskTimesTest extends HeuristicTest {

  behavior of "SkewedTaskTimes"

  it should "produce a report when skewed task times are detected" in {
    // Simulate a stage with skewed tasks.
    val eventLog = runSparkQueriesAndGetEvenLog { spark =>
      import spark.implicits._

      var df = spark.range(start = 1, end = 11, step = 1, numPartitions = 10)
      df = df.map { value =>
        Thread.sleep(Duration.ofSeconds(value).toMillis)
        value
      }
      df.collect()
    }

    val reports = SkewedTaskTimes
      .check(eventLog, Map("MinStageDurationSeconds" -> "1"))
      .toSeq

    reports should have size 2
  }

}
