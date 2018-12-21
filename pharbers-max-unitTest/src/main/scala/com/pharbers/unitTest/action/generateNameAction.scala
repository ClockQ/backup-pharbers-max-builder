package com.pharbers.unitTest.action

import akka.actor.ActorSelection
import com.pharbers.reflect.PhEntity.PhAction
import com.pharbers.unitTest.common.readJsonTrait
import com.pharbers.reflect.PhReflect.{reflect, pActionExec}
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs, pActionArgs, pActionTrait}

case class executeMaxAction(override val defaultArgs: pActionArgs)
                           (implicit val sendActor: ActorSelection) extends pActionTrait with readJsonTrait {
    override val name: String = "max_result"

    val job_id: String = defaultArgs.asInstanceOf[MapArgs].get("job_id").asInstanceOf[StringArgs].get
    val user_id: String = defaultArgs.asInstanceOf[MapArgs].get("user_id").asInstanceOf[StringArgs].get
    val company_id: String = defaultArgs.asInstanceOf[MapArgs].get("company_id").asInstanceOf[StringArgs].get

    val ym: String = defaultArgs.asInstanceOf[MapArgs].get("ym").asInstanceOf[StringArgs].get
    val mkt: String = defaultArgs.asInstanceOf[MapArgs].get("mkt").asInstanceOf[StringArgs].get
    val cpa_file: String = defaultArgs.asInstanceOf[MapArgs].get("cpa_file").asInstanceOf[StringArgs].get
    val gycx_file: String = defaultArgs.asInstanceOf[MapArgs].get("gycx_file").asInstanceOf[StringArgs].get

    override def perform(args: pActionArgs): pActionArgs = {

        // 执行Panel
        val panelConf = action.panelConf.get.head
        reflect(panelConf)(action.panelArgs(0, 0)(panelConf)).exec()

        // 执行Max
        val maxConf = action.calcConf.get.head
        reflect(maxConf)(action.calcArgs(0, 0)(maxConf)).exec()
        StringArgs(maxResult)
    }
}
