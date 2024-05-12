package org.spark_heuristics
package event_log.impl

import event_log._

import play.api.libs.json.{JsArray, Json, JsValue}

import java.io.InputStream
import scala.collection.mutable
import scala.io.Source

class EventLogReaderImpl(eventLogStream: InputStream) extends EventLogReader {

  private val sqlExecutions = mutable.HashMap[SqlExecutionId, SqlExecution]()
  private val jobs = mutable.HashMap[JobId, Job]()
  private val stageJob = mutable.HashMap[StageId, JobId]()
  private val skippedJobs = mutable.HashSet[JobId]()
  private val stages = mutable.HashMap[StageId, Stage]()
  private val skippedStages = mutable.HashSet[StageId]()
  private val tasks = mutable.HashMap[TaskId, Task]()
  private var eventLog: EventLog = _

  override def read(): EventLog = {
    if (eventLog != null)
      return eventLog

    Source
      .fromInputStream(eventLogStream)
      .getLines()
      .foreach(processEventLogLine)

    createEventLog()
    eventLog
  }

  private def createEventLog(): Unit = {
    EventLogReaderImpl.this.sqlExecutions.values.foreach(_.sparkPlans.last.isFinal = true)

    eventLog = new EventLog {
      override val sqlExecutions: Map[SqlExecutionId, event_log.SqlExecution] = EventLogReaderImpl.this.sqlExecutions.toMap
      override val jobs: Map[JobId, event_log.Job] = EventLogReaderImpl.this.jobs.toMap
      override val stages: Map[StageId, Stage] = EventLogReaderImpl.this.stages.toMap
      override val tasks: Map[TaskId, Task] = EventLogReaderImpl.this.tasks.toMap
    }
    clear()
  }

  private def clear(): Unit = {
    assert(eventLog != null)
    sqlExecutions.clear()
    jobs.clear()
    stages.clear()
    tasks.clear()
    skippedStages.clear()
  }

  private def processEventLogLine(line: String): Unit = {
    val json = Json.parse(line)
    json("Event").as[String] match {
      case eventType if eventType contains "SparkListenerSQL" =>
        processSqlExecution(eventType, json)
      case eventType if eventType contains "SparkListenerJob" =>
        processJob(eventType, json)
      case eventType if eventType contains "SparkListenerStage" =>
        processStage(eventType, json)
      case eventType if eventType.contains("SparkListenerTask") && eventType != "SparkListenerTaskGettingResult" =>
        processTask(eventType, json)
      case _ =>
    }
  }

  private def processSqlExecution(eventType: String, json: JsValue): Unit = {
    val sqlExecutionId = json("executionId").as[Long]
    eventType match {
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart" =>
        val rootSqlExecutionId = json("rootExecutionId").as[Long]
        val startTime = json("time").as[Long]
        val plan = processSparkPlan(json("sparkPlanInfo"))
        sqlExecutions.put(
          sqlExecutionId,
          new SqlExecution(
            sqlExecutionId = sqlExecutionId,
            rootSqlExecutionId = rootSqlExecutionId,
            startTime = startTime,
            sparkPlans = Array(plan)
          )
        )
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate" =>
        val sqlExecution = sqlExecutions(sqlExecutionId)
        val plan = processSparkPlan(json("sparkPlanInfo"))
        sqlExecution.sparkPlans = sqlExecution.sparkPlans :+ plan
      case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd" =>
        sqlExecutions.get(sqlExecutionId).foreach(_.finishTimestamp = json("time").as[Long])
      case _ =>
    }

  }

  private def processSparkPlanNode(nodeJson: JsValue, parent: Option[SparkPlanNode]): event_log.SparkPlanNode = {
    val name = nodeJson("nodeName").as[String]
    val detail = nodeJson("simpleString").as[String]
    val currentNode = new SparkPlanNode(nodeName = name, detail = detail, parent = parent)
    val children = nodeJson("children").as[JsArray].value.map(node => processSparkPlanNode(node, Some(currentNode))).toArray
    currentNode.children = children
    currentNode
  }

  private def processSparkPlan(json: JsValue): SparkPlan = {
    val rootNode = processSparkPlanNode(json, None)
    SparkPlan(root = rootNode)
  }

  private def processJob(eventType: String, json: JsValue): Unit = {
    val jobId = json("Job ID").as[Long]
    eventType match {
      case "SparkListenerJobStart" =>
        val sqlExecutionId: Long =
          try json("Properties")("spark.sql.execution.id").as[String].toLong
          catch {
            case _: NoSuchElementException =>
              skippedJobs.add(jobId)
              return
          }
        val stageIds = json("Stage Infos").as[JsArray].value.map(_("Stage ID").as[Long])
        stageIds.foreach(stageJob.put(_, jobId))
        jobs.put(jobId, new Job(jobId, sqlExecutionId, json("Submission Time").as[Long], stageIds))
      case "SparkListenerJobEnd" if !skippedJobs.contains(jobId) =>
        val job = jobs(jobId)
        job.finishTimestamp = json("Completion Time").as[Long]
        job.successful = json("Job Result")("Result").as[String] == "JobSucceeded"
      case _ =>
    }
  }

  private def processStage(eventType: String, json: JsValue): Unit =
    eventType match {
      case "SparkListenerStageSubmitted" =>
        val stageInfo = json("Stage Info")
        val stageId = stageInfo("Stage ID").as[Long]
        stageJob.get(stageId) match {
          case Some(jobId) =>
            stages.put(stageId, new Stage(jobId, stageId, stageInfo("Submission Time").as[Long]))
          case None =>
            skippedStages.add(stageId)
        }
      case "SparkListenerStageCompleted" =>
        val stageInfo = json("Stage Info")
        val stageId = stageInfo("Stage ID").as[Long]
        stages.get(stageId) match {
          case Some(stage) =>
            stage.finishTimestamp = stageInfo("Completion Time").as[Long]
            stage.successful = (stageInfo \ "Failure Reason").isEmpty
          case None =>
        }
      case _ =>
    }

  private def processTask(eventType: String, json: JsValue): Unit = {
    val taskInfo = json("Task Info")
    val stageId = json("Stage ID").as[Long]
    if (skippedStages.contains(stageId)) return
    val taskId = taskInfo("Task ID").as[Long]

    eventType match {
      case "SparkListenerTaskStart" =>
        stages(stageId).tasks.append(taskId)
        tasks.put(taskId, new Task(stageId, taskId, taskInfo("Launch Time").as[Long]))
      case "SparkListenerTaskEnd" =>
        val task = tasks(taskId)
        task.finishTimestamp = taskInfo("Finish Time").as[Long]
        task.successful = json("Task End Reason")("Reason").as[String] == "Success"

        val taskMetrics = json \ "Task Metrics"
        task.taskMetrics = null
        if (taskMetrics.isDefined) {
          val memoryBytesSpilled = taskMetrics("Memory Bytes Spilled").as[Long]
          val diskBytesSpilled = taskMetrics("Disk Bytes Spilled").as[Long]
          task.taskMetrics = TaskMetrics(memoryBytesSpilled, diskBytesSpilled)
        }
      case _ =>
    }
  }
}
