package org.spark_heuristics
package event_log

class EventWithDuration(override val startTime: TimestampMs) extends Event(startTime) {

  var successful: Boolean = false
  var finishTimestamp: TimestampMs = _

  def elapsedTimeMs: Option[Long] = finishTime.map(_ - startTime)

  def finishTime: Option[TimestampMs] = Option[TimestampMs](finishTimestamp)

}
