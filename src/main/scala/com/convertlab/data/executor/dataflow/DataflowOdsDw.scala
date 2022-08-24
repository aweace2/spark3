package com.convertlab.data.executor.dataflow

import com.convertlab.data.common.{JobConf}
import com.convertlab.data.common.extension.basic.JavaUtilExtensions._
import com.convertlab.data.runtime.Context
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.util.{Date, HashMap => JavaHashMap, Map => JavaMap}

class DataflowOdsDw(@transient val context: Context,
              val jobConf: JobConf,
              val data: JavaMap[String, Any]) extends Serializable {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val sparkSession = context.getSparkSession

  def execute(): Unit = {
    if (context.getFailed) {
      log.info("====== DataflowOdsDw has failed ======")
      // 程序抛出未知异常后的处理
      log.info(s"====== exception message: ${context.getFailedMessage}")
      return
    }

    log.info("====== DataflowOdsDw start ======")

    log.info("====== DataflowOdsDw finished ======")
  }
}
