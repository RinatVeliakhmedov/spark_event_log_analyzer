package org.spark_heuristics
package analyzer.heuristics

import analyzer.{Category, Heuristic, Report}
import event_log.{EventLog, SparkPlanNode, SqlExecution}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

object MultipleActionsWithoutCache extends Heuristic {

  private[analyzer] val DefaultMinQueryDurationSeconds = 30

  override def check(eventLog: EventLog, parameters: Map[String, String]): TraversableOnce[Report] = {
    val MinQueryDurationSeconds: Int =
      parameters.getOrElse("MinQueryDurationSeconds", DefaultMinQueryDurationSeconds.toString).toInt

    eventLog.sqlExecutions.values
      .filter(_.elapsedTimeMs.isDefined)
      .filter { query => TimeUnit.MILLISECONDS.toSeconds(query.elapsedTimeMs.getOrElse(0L)) > MinQueryDurationSeconds }
      .toArray
      .combinations(2)
      .toArray
      .par
      .map(arr => (arr(0), arr(1)))
      .filter { case (q1, q2) => q1.rootSqlExecutionId != q2.rootSqlExecutionId && q1.sqlExecutionId != q2.sqlExecutionId }
      .flatMap { case (q1, q2) => checkDuplicatedExecutions(q1, q2) }
      .seq
  }

  protected def checkDuplicatedExecutions(q1: SqlExecution, q2: SqlExecution): TraversableOnce[Report] = {
    val p1StartNodes = getStarterNodes(q1.finalPlan.root)
    val p2StartNodes = getStarterNodes(q2.finalPlan.root)

    val reports = for {
      x <- p1StartNodes
      y <- p2StartNodes
      if x.parent.isDefined && y.parent.isDefined
      if Utils.stripColumnIds(x.detail) == Utils.stripColumnIds(y.detail)
      (startNode, endNode) <- findDuplicates(x, y)
    } yield Report(
      Category.Performance,
      "Multiple actions without cache",
      s"""SQL query with id ${q1.sqlExecutionId}${if (!q1.isRoot) s" (root id ${q1.rootSqlExecutionId})"
      else ""} shares a sequence of operation with SQL query with id ${q2.sqlExecutionId}${if (!q2.isRoot)
        s" (root id ${q2.rootSqlExecutionId})"
      else ""}.
         |First shared Spark plan node: '${startNode.nodeName}': ${startNode.detail
        .substring(0, math.min(startNode.detail.length, 200))}.
         |Last shared Spark plan node: '${endNode.nodeName}': ${endNode.detail
        .substring(0, math.min(endNode.detail.length, 200))}.
         |Consider caching after the last shared node to improve performance.
         |Note: there is no guarantee that caching will speed up execution or not result in memory and/or disk issues. Use caution and measure both options.
         |""".stripMargin
    )

    reports
  }

  protected def getStarterNodes(node: SparkPlanNode): Seq[SparkPlanNode] =
    if (node.children.isEmpty) Seq(node) else node.children.flatMap(getStarterNodes)

  protected def findDuplicates(n1: SparkPlanNode, n2: SparkPlanNode): Option[(SparkPlanNode, SparkPlanNode)] = {
    val matchingSequence = mutable.ArrayBuffer[SparkPlanNode](n1)

    var l = n1
    var r = n2

    var continue = true
    while (continue) {
      l = l.parent.get
      r = r.parent.get
      continue = l.parent.isDefined && r.parent.isDefined

      if (Utils.stripColumnIds(l.detail) == Utils.stripColumnIds(r.detail))
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

}
