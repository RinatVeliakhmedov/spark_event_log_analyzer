package org.spark_heuristics
package event_log

case class TaskMetrics(
    memorySpillBytes: Long,
    diskSpillBytes: Long
) {

  val totalSpill: Long = memorySpillBytes + diskSpillBytes
}
