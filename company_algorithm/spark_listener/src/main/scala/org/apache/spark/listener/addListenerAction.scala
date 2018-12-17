package org.apache.spark.listener

import com.pharbers.spark.phSparkDriver
import org.apache.spark.listener.helper.ListenerHelper
import com.pharbers.pactions.actionbase.{NULLArgs, pActionArgs, pActionTrait}

object addListenerAction {
    def apply(start_progress: Int, end_progress: Int, app_name: String)
             (implicit xp: Map[String, Any] => Unit): pActionTrait =
        new addListenerAction(start_progress, end_progress, app_name)
}

class addListenerAction(start_progress: Int, end_progress: Int, app_name: String)
                       (implicit xp: Map[String, Any] => Unit) extends pActionTrait {
    override val name: String = "addListenerAction"
    override val defaultArgs: pActionArgs = NULLArgs

    override def perform(args: pActionArgs = NULLArgs): pActionArgs = {
        phSparkDriver(app_name).sc.addSparkListener(MaxSparkListener(ListenerHelper(start_progress, end_progress)(xp), app_name))
        args
    }
}