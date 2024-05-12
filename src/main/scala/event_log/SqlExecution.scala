package org.spark_heuristics
package event_log

class SqlExecution(
    val sqlExecutionId: SqlExecutionId,
    val rootSqlExecutionId: SqlExecutionId,
    override val startTime: TimestampMs,
    var sparkPlans: Array[event_log.SparkPlan]
) extends EventWithDuration(startTime) {

  val isRoot: Boolean = sqlExecutionId == rootSqlExecutionId

  def finalPlan: SparkPlan = {
    val lastPlan = sparkPlans.last
    assert(lastPlan.isFinal)
    lastPlan
  }

  override def toString: String = s"SqlExecution( " +
    s"SQL execution id: $sqlExecutionId, " +
    s"Root SQL execution id: $rootSqlExecutionId" +
    s"start: $startTime, " +
    s"finish: $finishTime, " +
    s"time: ${elapsedTimeMs.getOrElse(0L) / 1000.0} seconds, " +
    s"#plans: ${sparkPlans.size}, " +
    s"${if (successful) "Succeeded" else "Failed"} )"
}
