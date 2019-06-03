package org.apache.spark.listener

import com.pharbers.spark.listener.listenTrait.{MaxSparkListenerTrait, PhListenHelperTrait}
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}

/**
  * Created by clock on 18-5-2.
  *
  * Spark 的 SparkListener 提供针对 application, job, task的状态监听。
  * 通过该监听，可以实现：
  * 1.任务执行进度的粗略计算。
  * 2.执行异常失败时，获取异常信息。
  * 3.获取app启动的appId,从而可以控制杀死任务。
  * 4.自定义进度和异常的handle处理（如控制台打印，保存db，或jms传输到web等终端
  *
  */
case class MaxSparkListener(helper: PhListenHelperTrait, override val app_name: String) extends MaxSparkListenerTrait {

    override def onJobStart(job: SparkListenerJobStart): Unit = {
        helper.jobStart(job.stageInfos.map(stageInfo => stageInfo.numTasks).sum)
    }

    override def onTaskEnd(task: SparkListenerTaskEnd): Unit = {
        helper.taskEnd()
    }

    override def onJobEnd(job: SparkListenerJobEnd): Unit = {
        helper.jobEnd(app_name, this)
    }
}
