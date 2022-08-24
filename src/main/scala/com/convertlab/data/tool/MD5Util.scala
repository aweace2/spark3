package com.convertlab.data.tool

import org.apache.commons.codec.digest.{DigestUtils}

import java.util.{Map => JavaMap}


object MD5Util {

  def md5Context(data: JavaMap[String, Any]): String = {
    import scala.collection.JavaConverters._
    val value = data.asScala.toSeq.sortBy(_._1).map(_._2).mkString(",")
    DigestUtils.md5Hex(value)
  }

}
