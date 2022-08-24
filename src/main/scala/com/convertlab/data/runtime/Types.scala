package com.convertlab.data.runtime

import java.util.function.Consumer
import scala.language.implicitConversions

package object Types {
  type RunnerProcess = Context => Unit

  implicit def toConsumer[A](function: A => Unit): Consumer[A] = new Consumer[A]() {
    override def accept(arg: A): Unit = function.apply(arg)
  }
}
