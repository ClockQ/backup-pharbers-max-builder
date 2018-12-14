package org.apache.spark.listener

import com.pharbers.spark.phSparkDriver
import org.apache.spark.listener.listenTrait.MaxSparkListenerTrait
import com.pharbers.pactions.actionbase.{NULLArgs, pActionArgs, pActionTrait}

object addListenerAction {
    def apply(listener: MaxSparkListenerTrait, arg_name: String = "addListenerAction"): pActionTrait =
        new addListenerAction(listener, arg_name)
}

class addListenerAction(listener: MaxSparkListenerTrait,
                        override val name: String) extends pActionTrait {

    override val defaultArgs: pActionArgs = NULLArgs

    override def perform(args: pActionArgs): pActionArgs = {
        phSparkDriver(listener.job_name).sc.addSparkListener(listener)
        args
    }
}