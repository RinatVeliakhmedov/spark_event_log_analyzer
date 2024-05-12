package org.spark_heuristics
package event_log

class Event(val startTime: TimestampMs) extends Ordering[Event] {

  override def compare(lhs: Event, rhs: Event): Int = lhs.startTime compare rhs.startTime

}
