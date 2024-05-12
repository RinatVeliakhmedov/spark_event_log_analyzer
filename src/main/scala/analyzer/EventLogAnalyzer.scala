package org.spark_heuristics
package analyzer

import analyzer.heuristics._
import event_log.EventLog

object EventLogAnalyzer {

  val DefaultConfig: Config = Config()
  val DefaultHeuristics: Seq[Heuristic] =
    Seq(SkewedTaskTimes, MultipleActionsWithoutCache, SingleQueryDuplicatedBranches, SpillsPresent)

  def analyze(
      eventLog: EventLog,
      heuristics: Iterable[Heuristic],
      config: Config
  ): TraversableOnce[Report] = {
    if (!eventLog.jobs.forall(_._2.successful))
      println("Even log contains failed jobs, this is currently not supported and may not work correctly.")

    heuristics.par.flatMap(_.check(eventLog, config.parameters)).seq
  }

}
