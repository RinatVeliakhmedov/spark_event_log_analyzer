package org.spark_heuristics
package analyzer.heuristics

import analyzer.{Category, Heuristic, Report}
import event_log.{EventLog, SparkPlanNode, SqlExecution}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

object SingleQueryDuplicatedBranches extends Heuristic {

  private[analyzer] val DefaultMinQueryDurationSeconds = 30

  override def check(eventLog: EventLog, parameters: Map[String, String]): TraversableOnce[Report] = {
    val MinQueryDurationSeconds: Int =
      parameters.getOrElse("MinQueryDurationSeconds", DefaultMinQueryDurationSeconds.toString).toInt

    eventLog.sqlExecutions.values.par
      .filter(_.elapsedTimeMs.isDefined)
      .filter { query =>
        TimeUnit.MILLISECONDS.toSeconds(query.elapsedTimeMs.getOrElse(0L)) > MinQueryDurationSeconds
      }
      .flatMap(checkDuplicatedBranches)
      .seq
  }

  def checkDuplicatedBranches(query: SqlExecution): Iterable[Report] =
    getStarterNodes(query.finalPlan.root)
      .combinations(2)
      .toArray
      .par
      .map(arr => (arr(0), arr(1)))
      .filter(t => t._1.parent.isDefined && t._2.parent.isDefined)
      .filter(t => Utils.stripColumnIds(t._1.detail) == Utils.stripColumnIds(t._2.detail))
      .flatMap(t => findDuplicates(t._1, t._2))
      .map(t => createReport(query, t._1, t._2))
      .seq

  protected def getStarterNodes(node: SparkPlanNode): Array[SparkPlanNode] =
    if (node.children.isEmpty) Array(node) else node.children.flatMap(getStarterNodes)

  protected def findDuplicates(n1: SparkPlanNode, n2: SparkPlanNode): Option[(SparkPlanNode, SparkPlanNode)] = {
    val matchingSequence = mutable.ArrayBuffer[SparkPlanNode](n1)

    var l = n1
    var r = n2

    var continue = true
    while (continue) {
      l = l.parent.get
      r = r.parent.get
      continue = l.parent.isDefined && r.parent.isDefined

      if (l != r && Utils.stripColumnIds(l.detail) == Utils.stripColumnIds(r.detail))
        matchingSequence.append(l)
      else
        continue = false

      if (Seq("InMemoryTableScan", "TableCacheQueryStage").contains(l.nodeName))
        matchingSequence.clear()
    }

    if (matchingSequence.length >= 2)
      Some((matchingSequence.head, matchingSequence.last))
    else
      None
  }

  protected def createReport(query: SqlExecution, startNode: SparkPlanNode, endNode: SparkPlanNode): Report =
    Report(
      Category.Performance,
      "Duplicated sequence of transformations in a single query",
      s"""SQL query with id ${query.sqlExecutionId}${if (!query.isRoot) s" (root id ${query.rootSqlExecutionId})"
      else ""} shares contains a sequence of duplicated transformations that could potentially be cached.
         |First shared Spark plan node: '${startNode.nodeName}': ${startNode.detail
        .substring(0, math.min(startNode.detail.length, 200))}.
         |Last shared Spark plan node: '${endNode.nodeName}': ${endNode.detail
        .substring(0, math.min(endNode.detail.length, 200))}.
         |Consider caching after the last shared node to improve performance.
         |Note: there is no guarantee that caching will speed up execution or not result in memory and/or disk issues. Use caution and measure both options.
         |""".stripMargin
    )
}
