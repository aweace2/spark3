package com.convertlab.data.common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.{Properties, Map => JavaMap}
import scala.collection.JavaConverters._

/**
 * 根据配置文件中的配置信息预处理出的信息
 */
case class JobConf(@transient spark: SparkSession,
                   @transient properties: Properties,
                   @transient data: JavaMap[String, Any]) {

}
