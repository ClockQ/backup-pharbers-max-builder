package com.pharbers.spark.listener.helper

import com.pharbers.util.log.phLogTrait
import com.pharbers.pactions.actionbase.NULLArgs
import org.apache.spark.listener.removeListenerAction
import com.pharbers.spark.listener.listenTrait.{MaxSparkListenerTrait, PhListenHelperTrait}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-09 10:33
  */
case class ListenerHelper(start_progress: Int, end_progress: Int)
                         (implicit send: Map[String, Any] => Unit)
        extends PhListenHelperTrait with phLogTrait {

    // 当前进度
    private var current: Double = start_progress
    // 剩余任务数
    private var remainTask: Int = 0
    // 步长
    private var stride: Double = 0.0
    // 返回的取整后的进度
    private var progress: Int = 0

    override def jobStart(taskSum: Int): Unit = {
        remainTask += taskSum
        stride = (end_progress - current) / remainTask
    }

    override def taskEnd(): Unit = {
        remainTask -= 1
        current += stride
        if (current.toInt > progress && current.toInt <= end_progress) {
            progress = current.toInt
            send(Map("progress" -> progress))
        }
    }

    override def jobEnd(app_name: String, listener: MaxSparkListenerTrait): Unit = {
        if (remainTask < 1)
            removeListenerAction(app_name, listener).perform(NULLArgs)
    }
}
