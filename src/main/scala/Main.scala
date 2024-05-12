package org.spark_heuristics

import analyzer.EventLogAnalyzer
import event_log.EventLogReader

import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    val eventLogFile = new File(args.head).getAbsoluteFile
    val eventLog = EventLogReader(eventLogFile).read()

    println(s"Processed ${eventLog.jobs.size} jobs, ${eventLog.stages.size} stages, ${eventLog.tasks.size} tasks.")

    val reports = EventLogAnalyzer.analyze(eventLog, EventLogAnalyzer.DefaultHeuristics, EventLogAnalyzer.DefaultConfig)

    println(s"Reports:\n${reports.mkString("\n")}")
  }

}

/*
TODO:
- Heuristic: High GC time.
- Heuristic: Cached data accessed only once.
- Heuristic: Idle executor cores.
- Heuristic: Jobs could be executed in parallel.
- Heuristic: Default number of partitions.
- Command line interface.
- Configurability.
- Report generation.
- Process events concurrently.
 */
