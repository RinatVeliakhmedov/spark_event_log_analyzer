package org.spark_heuristics
package event_log

class SparkPlanNode(
    val nodeName: String,
    val detail: String,
    val parent: Option[SparkPlanNode] = None,
    var children: Array[event_log.SparkPlanNode] = Array.empty
) {

  def print(offset: Int, sb: StringBuilder): Unit = {
    val offsetStr = "  ".repeat(offset)
    sb.append(offsetStr).append(nodeName).append("  ").append(detail)
    children.foreach { childNode =>
      sb.append('\n')
      childNode.print(offset + 1, sb)
    }
  }

}
