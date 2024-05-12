package org.spark_heuristics
package event_log

class Task(val stageId: StageId, val taskId: TaskId, val startTimestamp: TimestampMs) extends EventWithDuration(startTimestamp) {
  var taskMetrics: TaskMetrics = _

  override def toString: String = s"Task( " +
    s"Stage id: $stageId, " +
    s"Task id: $taskId, " +
    s"start: $startTimestamp, " +
    s"finish: $finishTime, " +
    s"time: ${elapsedTimeMs.getOrElse(0L) / 1000.0} seconds, " +
    s"${if (successful) "Succeeded" else "Failed"} )"
}
