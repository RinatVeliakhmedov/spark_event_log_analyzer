package org.spark_heuristics
package analyzer.heuristics

private[heuristics] object Utils {

  def stripColumnIds(sparkPlanNodeDetailStr: String): String = sparkPlanNodeDetailStr.replaceAll("(#[0-9]+L)", "")

}
