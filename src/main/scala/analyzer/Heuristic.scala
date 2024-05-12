package org.spark_heuristics
package analyzer

import event_log.EventLog

trait Heuristic {

  def check(eventLog: EventLog, parameters: Map[String, String]): TraversableOnce[Report]

}
