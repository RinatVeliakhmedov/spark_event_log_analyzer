package org.spark_heuristics
package analyzer.heuristics

import analyzer.{Category, Heuristic, Report}
import event_log.EventLog

object SpillsPresent extends Heuristic {

  override def check(eventLog: EventLog, parameters: Map[String, String]): TraversableOnce[Report] = {
    eventLog.stages.values.toParArray
      .filter(_.successful)
      .map { stage =>
        val totalStageSpillBytes = stage.tasks.foldLeft(0L) { (totalSpill: Long, taskId) =>
          totalSpill + eventLog.tasks(taskId).taskMetrics.totalSpill
        }
        (stage.jobId, stage.stageId, totalStageSpillBytes)
      }
      .filter(_._3 > 0)
      .map { case (jobId, stageId, totalStageSpillBytes) =>
        Report(
          Category.Performance,
          "Stage contains spills",
          f"Job $jobId, stage $stageId: ${totalStageSpillBytes / 1e+9}%.2f GB spilled."
        )
      }
      .toArray
      .toSeq
  }

}
