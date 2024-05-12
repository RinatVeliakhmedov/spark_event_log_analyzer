package org.spark_heuristics
package analyzer.heuristics

import analyzer.{Category, Heuristic, Report}
import event_log.EventLog

import java.util.concurrent.TimeUnit

object SkewedTaskTimes extends Heuristic {

  private[analyzer] val DefaultMinStageDurationSeconds = 30
  private[analyzer] val DefaultMinSkewFactor = 2.0

  override def check(eventLog: EventLog, parameters: Map[String, String]): TraversableOnce[Report] = {
    val MinStageDurationSeconds: Int =
      parameters.getOrElse("MinStageDurationSeconds", DefaultMinStageDurationSeconds.toString).toInt
    val MinSkewFactor: Float = parameters.getOrElse("MinSkewFactor", DefaultMinSkewFactor.toString).toFloat

    eventLog.stages.values.par
      .filter { stage =>
        TimeUnit.MILLISECONDS.toSeconds(stage.elapsedTimeMs.getOrElse(0L)) > MinStageDurationSeconds
      }
      .map(_.tasks.map(taskId => eventLog.tasks(taskId)))
      .filter(_.forall(_.elapsedTimeMs.isDefined))
      .flatMap { tasks =>
        var reports: Array[Report] = Array.empty
        val taskDurations = tasks.map(task => TimeUnit.MILLISECONDS.toSeconds(task.elapsedTimeMs.get)).toArray.sorted
        val medianTaskTime = median(taskDurations)
        if (taskDurations.exists(d => !durationWithinMedian(d, medianTaskTime, MinSkewFactor))) {
          reports = Array(
            Report(
              Category.Info,
              "Skewed tasks",
              s"In the job with ID ${eventLog.stages(tasks.head.stageId).jobId}, stage ${tasks.head.stageId}: found tasks that are more than $MinSkewFactor times longer or shorter than the median time of $medianTaskTime seconds."
            )
          )
        }
        val longestToShortestTaskRatio = taskDurations.last.toFloat / taskDurations.head.toFloat
        if (longestToShortestTaskRatio > MinSkewFactor)
          reports = reports :+ Report(
            Category.Info,
            "Skewed tasks",
            s"In the job with ID ${eventLog.stages(tasks.head.stageId).jobId}, stage ${tasks.head.stageId}: the longest task (${taskDurations.last} seconds) is more than $MinSkewFactor longer that the shortest task (${taskDurations.head} seconds)."
          )
        reports
      }
      .seq
  }

  private def median(s: Seq[Long]): Float = {
    val (lower, upper) = s.splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0f else upper.head.toFloat
  }

  private def durationWithinMedian(durationSec: Long, medianTaskDuration: Float, minSkewFactor: Float): Boolean = {
    (durationSec > medianTaskDuration * minSkewFactor) || (durationSec < medianTaskDuration * (1.0f / minSkewFactor))
  }
}
