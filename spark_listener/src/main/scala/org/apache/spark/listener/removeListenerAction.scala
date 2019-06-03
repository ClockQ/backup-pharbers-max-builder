package org.apache.spark.listener

import com.pharbers.pactions.actionbase.{NULLArgs, pActionArgs, pActionTrait}
import com.pharbers.spark.listener.listenTrait.MaxSparkListenerTrait
import com.pharbers.spark.phSparkDriver

object removeListenerAction {
    def apply(app_name: String, listener: MaxSparkListenerTrait): pActionTrait =
        new removeListenerAction(app_name, listener)
}

class removeListenerAction(app_name: String, listener: MaxSparkListenerTrait) extends pActionTrait {
    override val name: String = "removeListenerAction"
    override val defaultArgs: pActionArgs = NULLArgs

    override def perform(args: pActionArgs = NULLArgs): pActionArgs = {
        phSparkDriver(app_name).sc.listenerBus.removeListener(listener)
        args
    }
}