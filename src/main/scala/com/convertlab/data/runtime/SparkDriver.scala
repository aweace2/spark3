package com.convertlab.data.runtime

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory, MDC}

import java.util.Properties
import scala.collection.JavaConverters._

class SparkDriver @throws[Exception]
(val properties: Properties, val runnerName: String) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var jobName: String = System.getProperty("job_name", runnerName)

  val sparkSession: SparkSession = SparkSession.builder
    .config(generateSparkConf())
    .appName(jobName)
    .enableHiveSupport
    .getOrCreate

  MDC.put("env", System.getProperty("env", "test"))

  val kerberosEnabled: Boolean = properties.getProperty("kerberos_enabled", "false").toBoolean
  if (kerberosEnabled) {
    System.setProperty("java.security.krb5.conf", properties.getProperty("krb5_conf_path", ""))
    val conf = new Configuration
    conf.set("hadoop.security.authentication", "Kerberos")
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(properties.getProperty("keytab_user", ""), properties.getProperty("keytab_path", ""))
  }

  private val runner: Runner = new Runner(jobName, properties, sparkSession)
  private val runnerThread: Thread = new Thread(runner, "spark-runner")
  private var runnerException: Option[Throwable] = None

  private val exceptionHandler = new Thread.UncaughtExceptionHandler() {
    override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
      throwable match {
        case ie: IllegalStateException =>
          logger.warn(s"==== spark context has been shut down: ${ExceptionUtils.getRootCauseMessage(ie)} ====", ie)
        case default =>
          logger.info(s"=== ${ExceptionUtils.getRootCauseMessage(default)} ===", default)
      }
      runnerException = Some(throwable)
    }
  }
  runnerThread.setUncaughtExceptionHandler(exceptionHandler)

  def setRunnerProcess(runnerProcess: RunnerProcess): Unit = {
    setRunnerProcess(context => runnerProcess.apply(context))
  }

  def setRunnerProcess(runnerProcess: Types.RunnerProcess): Unit = {
    runner.setRunnerProcess(runnerProcess)
  }

  def run(): Unit = {
    try {
      runnerThread.start()
      runnerThread.join()
      logger.debug("==== runner thread has exited ====")
    } catch {
      case ie: InterruptedException =>
        logger.warn(s"==== driver interrupted: ${ExceptionUtils.getRootCauseMessage(ie)} ====", ie)
    } finally {
      logger.debug("==== try to stop spark session and interrupt kafka consumer ====")
      sparkSession.stop()
      runnerException.foreach(t => throw t)
    }
  }

  def run(runnerProcess: RunnerProcess): Unit = {
    setRunnerProcess(runnerProcess)
    run()
  }

  def generateSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sparkConf.set("spark.shuffle.detectCorrupt", "false")
    sparkConf.set("spark.driver.cores", "2")
    sparkConf.set("spark.driver.memory", "1g")
    sparkConf.set("spark.executor.instances", "2")
    sparkConf.set("spark.executor.cores", "4")
    sparkConf.set("spark.executor.memory", "4g")
    sparkConf.set("spark.default.parallelism", "32")
    sparkConf.set("spark.sql.shuffle.partitions", "32")
    sparkConf.set("spark.dynamicAllocation.enabled", "false")
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "4")
    sparkConf.set("spark.dynamicAllocation.minExecutors", "1")
    sparkConf.set("spark.blacklist.enabled", "false")
    sparkConf.set("spark.sql.session.timeZone", "UTC")

    properties
      .stringPropertyNames
      .asScala
      .toList
      .filter(_.startsWith("spark."))
      .foreach(name => sparkConf.set(name, properties.getProperty(name)))

    sparkConf
  }
}
