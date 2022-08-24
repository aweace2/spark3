package com.convertlab.data.runner

import com.convertlab.data.common.JobConf
import com.convertlab.data.common.extension.basic.JavaUtilExtensions._
import com.convertlab.data.executor.dataflow.DataflowOdsDw
import com.convertlab.data.runtime.{Context, SparkDriver}
import com.convertlab.data.util.JsonUtils
import com.convertlab.data.util.crypto.AESECBUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.util.{Properties, TimeZone, Map => JavaMap}
import scala.util.Try

object SparkJobRunner {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val SPARK_JOB_DATAFLOW_ODSDW = "spark-job-dataflow-odsdw"

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    log.info("====== SparkJobRunner start ======")

    val data = JsonUtils.strToObj(args(0), classOf[JavaMap[String, Any]]).get()
    log.info(s"====== run with data: ${args(0)}")
    // 作业名称
    val jobName = data.getV[String]("jobName")

    // 加载配置
    val properties = new Properties
    // SparkJob的全局配置（必需）
    val confFile = data.getV[String]("confFile", "spark-driver.properties")
    log.info(s"====== load global conf from $confFile ======")
    properties.load(new FileInputStream(confFile))
    // 每个作业的自定义配置（可选）
    try {
      properties.load(new FileInputStream(s"$jobName.properties"))
      log.info(s"====== job $jobName append custom conf $jobName.properties ======")
    } catch {
      case _: Throwable =>
        log.warn(s"====== job $jobName use global conf ======")
    }

    val sparkDriver = new SparkDriver(properties, jobName)
    sparkDriver.setRunnerProcess((context: Context) => {
      registerGlobalUDF(context.getSparkSession)
      val jobConf = JobConf(context.getSparkSession, properties, data)
      jobName match {
        case SPARK_JOB_DATAFLOW_ODSDW =>
          new DataflowOdsDw(context, jobConf, data).execute()

        case _ =>
          log.warn(s"====== unsupported job: $jobName ======")
      }
    })
    sparkDriver.run()
  }

  /**
   * 注册Session级别的公共UDF，使用`g_`作为公共UDF名称前缀
   */
  def registerGlobalUDF(sparkSession: SparkSession): Unit = {
    val aesEncryptFunc = (value: String, password: String) => Try(AESECBUtils.encrypt(value, password)).getOrElse(value)
    sparkSession.udf.register("g_aes_encrypt", aesEncryptFunc)
    val aesDecryptFunc = (value: String, password: String) => Try(AESECBUtils.decrypt(value, password)).getOrElse(value)
    sparkSession.udf.register("g_aes_decrypt", aesDecryptFunc)
  }
}
