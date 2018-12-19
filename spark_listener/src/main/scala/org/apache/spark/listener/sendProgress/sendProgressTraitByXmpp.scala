package org.apache.spark.listener.sendProgress

import akka.util.Timeout
import akka.actor.ActorSelection
import scala.language.postfixOps
import scala.concurrent.duration._
import com.pharbers.util.log.phLogTrait
import com.pharbers.channel.detail.{PhMaxJob, channelEntity}

sealed trait sendProgressTraitByXmpp extends sendProgressTrait {
    def sendProcess(obj: channelEntity)(implicit actorRef: ActorSelection): Unit = {
        implicit val resolveTimeout: Timeout = Timeout(5 seconds)
        actorRef ! obj.asInstanceOf[PhMaxJob]
    }
}

case class sendXmppSingleProgress(company_id: String, user_id: String, call: String, job_id: String)
                                 (implicit sendActor: ActorSelection) extends sendProgressTraitByXmpp with phLogTrait {
    val singleProgress: Map[String, Any] => Unit = { map =>
        val progress = map("progress").asInstanceOf[Int]
        val result = new PhMaxJob
        result.company_id = company_id
        result.user_id = user_id
        result.call = call
        result.job_id = job_id
        result.percentage = progress
        sendProcess(result)(sendActor)
        phLog(s"$company_id $user_id current $call progress = $progress")
    }
}

case class sendXmppMultiProgress(company_id: String, user_id: String, call: String, job_id: String)
                                (p_current: Double, p_total: Double)
                                (implicit sendActor: ActorSelection) extends sendProgressTraitByXmpp with phLogTrait {
    private var previousProgress = 0
    val multiProgress: Map[String, Any] => Unit = { map =>
        val progress = map("progress").asInstanceOf[Int]
        val currentprogress = p_total match {
            case d: Double if d < 1 => 0
            case _ => ((p_current - 1) / p_total * 100 + progress / p_total).toInt
        }

        if(currentprogress > previousProgress){
            val result = new PhMaxJob
            result.company_id = company_id
            result.user_id = user_id
            result.call = call
            result.job_id = job_id
            result.percentage = progress

            sendProcess(result)(sendActor)
            phLog(s"xmpp msg => $company_id $user_id current $call progress = " + currentprogress)
            previousProgress = currentprogress
        }
    }
}
