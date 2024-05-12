package org.spark_heuristics
package event_log

import event_log.impl.EventLogReaderImpl

import java.io.{ByteArrayInputStream, File, FileInputStream, InputStream}

trait EventLogReader {

  def read(): EventLog

}

object EventLogReader {
  def apply(eventLogFile: File): EventLogReader =
    new EventLogReaderImpl(new FileInputStream(eventLogFile))

  def apply(eventLog: String): EventLogReader =
    new EventLogReaderImpl(new ByteArrayInputStream(eventLog.getBytes))

  def apply(eventLog: InputStream): EventLogReader =
    new EventLogReaderImpl(eventLog)
}
