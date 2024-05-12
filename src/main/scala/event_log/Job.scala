package org.spark_heuristics
package event_log

class Job(
    val jobId: JobId,
    val sqlExecutionId: SqlExecutionId,
    override val startTime: TimestampMs,
    val stages: Traversable[StageId]
) extends EventWithDuration(startTime) {

  override def toString: String = s"Job( " +
    s"Job id: $jobId, " +
    s"Sql execution id: $sqlExecutionId" +
    s"start: $startTime, " +
    s"finish: $finishTime, " +
    s"time: ${elapsedTimeMs.getOrElse(0L) / 1000.0} seconds, " +
    s"${if (successful) "Succeeded" else "Failed"} )"

}
