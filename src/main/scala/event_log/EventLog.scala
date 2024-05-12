package org.spark_heuristics
package event_log

trait EventLog {

  def sqlExecutions: Map[SqlExecutionId, SqlExecution]

  def jobs: Map[JobId, Job]

  def stages: Map[StageId, Stage]

  def tasks: Map[TaskId, Task]
}
