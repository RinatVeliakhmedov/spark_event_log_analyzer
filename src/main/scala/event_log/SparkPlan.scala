package org.spark_heuristics
package event_log

case class SparkPlan(
    var isFinal: Boolean = false,
    var root: SparkPlanNode
) {

  override def toString: String = {
    val sb = StringBuilder.newBuilder
    root.print(1, sb)
    s"SparkPlan(isFinal=$isFinal):\n${sb.toString()}"
  }

}
