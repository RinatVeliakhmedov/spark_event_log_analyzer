package org.spark_heuristics
package analyzer.heuristics

import event_log.{EventLog, EventLogReader}

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, RandomAccessFile}
import java.nio.file.Paths

trait HeuristicTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = HeuristicTest.spark

  def runSparkQueriesAndGetEvenLog(f: SparkSession => Unit): EventLog =
    HeuristicTest.synchronized {
      // This is hacky af, but spark does not allow multiple contexts at the same time.
      // Perhaps we can use a spark listener instead of spark even log?
      val files =
        FileUtils.listFiles(HeuristicTest.LogsDir, null, false)
      assert(files.size() == 1)
      val logFile = files.iterator().next()
      var raf = new RandomAccessFile(logFile, "r")
      raf.seek(raf.length())
      val currentLogPos = raf.getFilePointer
      raf.close()
      f(spark)
      Thread.sleep(1000)
      raf = new RandomAccessFile(logFile, "r")
      raf.seek(currentLogPos)
      val events = StringBuilder.newBuilder
      var line = raf.readLine()
      while (line != null) {
        events.append(line).append('\n')
        line = raf.readLine()
      }
      raf.close()
      EventLogReader(events.result).read()
    }

}

object HeuristicTest {

  private val LogsDirPath = Paths
    .get(".", "target", "test", "heuristic-test-spark-even-logs")
    .toAbsolutePath
  private val LogsDir: File = LogsDirPath.toFile

  private lazy val spark: SparkSession = {
    require(SparkSession.getActiveSession.isEmpty)

    LogsDir.mkdirs()
    FileUtils.cleanDirectory(LogsDir)

    Logger.getRootLogger.setLevel(Level.OFF)
    val session = SparkSession
      .builder()
      .master("local[200]")
      .appName("heuristic-tests")
      .config("spark.ui.enabled", false)
      .config("spark.eventLog.enabled", true)
      .config("spark.eventLog.dir", LogsDirPath.toUri.toString)
      .config("spark.eventLog.overwrite", true)
      .config("spark.eventLog.logStageExecutorMetrics", true)
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    session
  }

}
