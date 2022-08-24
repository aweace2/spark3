package com.convertlab.data.common.extension.basic

import java.util.{Optional, Properties, Map => JavaMap}

object JavaUtilExtensions {

  implicit class MapExtension(val javaMap: JavaMap[String, Any]) extends AnyVal {

    def getV[T](key: String): T = javaMap.get(key) match {case v: T => v}

    def getV[T](key: String, defaultValue: T): T = javaMap.get(key) match {
      case v: T => v
      case _ => defaultValue
    }

    def optV[T](key: String): Option[T] = javaMap.get(key) match {
      case v: T => Some(v)
      case _ => None
    }
  }

  implicit class OptionalExtension[T](val optional: Optional[T]) extends AnyVal {

    def toOption: Option[T] = if (optional.isPresent) Some(optional.get()) else None
  }

  implicit class PropertiesExtension(val properties: Properties) extends AnyVal {

    def optProperty(key: String): Option[String] = Option(properties.getProperty(key))
  }
}
