package com.convertlab.data.common.extension.basic

import scala.util.Try

object JavaLangExtensions {

  implicit class StringExtension(val s: String) extends AnyVal {

    def optLong: Option[Long] = Try(s.toLong).toOption
  }
}
