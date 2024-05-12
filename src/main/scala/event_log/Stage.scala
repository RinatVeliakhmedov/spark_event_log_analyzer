package org.spark_heuristics
package event_log

import scala.collection.mutable

class Stage(
    val jobId: JobId,
    val stageId: StageId,
    override val startTime: TimestampMs,
    val tasks: mutable.ArrayBuffer[TaskId] = mutable.ArrayBuffer.empty
) extends EventWithDuration(startTime) {

  override def toString: String = s"Stage( " +
    s"Stage id: $stageId, " +
    s"start: $startTime, " +
    s"finish: $finishTime, " +
    s"time: ${elapsedTimeMs.getOrElse(0L) / 1000.0} seconds, " +
    s"${if (successful) "Succeeded" else "Failed"} )"
}
