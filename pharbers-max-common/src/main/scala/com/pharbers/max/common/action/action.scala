package com.pharbers.max.common

import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-05-17 19:16
  */
package object action {
    implicit val packProgress: Int => MapArgs = progress => MapArgs(Map("progress" -> StringArgs(progress.toString)))
    implicit val sendProgress: MapArgs => Unit = { args =>
        val progress = args.getAs[StringArgs]("progress")
        println("progress = " + progress)
    }
}
