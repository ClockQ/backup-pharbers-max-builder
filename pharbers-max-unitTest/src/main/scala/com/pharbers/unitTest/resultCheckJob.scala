package com.pharbers.unitTest

import akka.actor.ActorSystem
import com.pharbers.unitTest.action._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.pactions.actionbase.{MapArgs, pActionArgs, pActionTrait}

object resultCheckJob{
    def apply(args: PhActionArgs)(implicit as: ActorSystem): pActionTrait = new resultCheckJob(args)
}

class resultCheckJob(override val defaultArgs: pActionArgs)
                    (implicit as: ActorSystem) extends sequenceJobWithMap {
    override val name: String = "result_check_job"

    val df = MapArgs(
        Map(
            "checkAction" -> defaultArgs.asInstanceOf[PhActionArgs]
        )
    )

    override val actions: List[pActionTrait] = {
                executeMaxAction(df) ::
                loadOfflineResult(df) ::
                resultCheckAction(df) ::
                writeCheckResultAction(df) ::
                Nil
    }
}
