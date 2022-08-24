package com.convertlab.data.runtime

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.concurrent._
import java.util.{Properties, UUID}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class Runner(val jobName: String,
             val properties: Properties,
             val sparkSession: SparkSession) extends Runnable {

  private val EXECUTION_TIMEOUT: Long = properties.getProperty("execution_timeout", "600").toLong
  private val KUDU_MASTER: String = properties.getProperty("kudu_master", "")
  private val KUDU_DATABASES: Array[String] = properties.getProperty("kudu_databases", "").split(",")

  private var runnerProcess: Option[Types.RunnerProcess] = None
  private val logger = LoggerFactory.getLogger(this.getClass)

  // create execution context by using single thread executor
  private val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("user-driver-thread-%d").build()
  private val threadPool = Executors.newSingleThreadExecutor(threadFactory)
  private val executionContext = ExecutionContext.fromExecutorService(threadPool)

  override def run(): Unit = {
    assert(runnerProcess.nonEmpty, "runnerProcess must be set")
    logger.info("=== runner {} start ===", jobName)
    try {
      if (sparkSession.sparkContext.isStopped) {
        throw new IllegalStateException
      }

      consume()
    } finally {
      logger.info("=== runner {} exit ===", jobName)
    }
  }

  def setRunnerProcess(runnerProcess: Types.RunnerProcess): Unit = {
    this.runnerProcess = Option(runnerProcess)
  }


  private def consume(): Unit = {

    val taskId = UUID.randomUUID().toString.replace("-", "")
    var execution: Future[Unit] = null
    val startTime = System.currentTimeMillis()

    try {
      sparkSession.sparkContext.setJobGroup(s"$jobName, $taskId", jobName, interruptOnCancel = false)

      logger.info(s"=== $jobName start processing $taskId ===")

      execution = executionContext.submit(new Callable[Unit]() {
        override def call(): Unit = {
          runnerProcess.get(new Context(sparkSession, taskId))
        }
      })
      execution.get(EXECUTION_TIMEOUT, TimeUnit.SECONDS)
    } catch {
      case throwable: Throwable =>
        if (throwable.isInstanceOf[TimeoutException]) {
          execution.cancel(true)
          sparkSession.sparkContext.cancelAllJobs()
        }
        val message = ExceptionUtils.getRootCauseMessage(throwable)
        logger.error(s"====== exception: $message", throwable)
        throw throwable
    } finally {
      sparkSession.catalog.clearCache()
      sparkSession.sparkContext.clearJobGroup()
      val endTime = System.currentTimeMillis()
      logger.info(s"=== $jobName processing $taskId finished, took ${endTime - startTime}ms ===")
    }
  }
}
