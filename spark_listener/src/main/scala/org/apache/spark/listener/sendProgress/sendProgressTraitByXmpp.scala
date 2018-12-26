package org.apache.spark.listener.sendProgress

import akka.util.Timeout
import scala.language.postfixOps
import scala.concurrent.duration._
import com.pharbers.util.log.phLogTrait
import com.pharbers.channel.detail.channelEntity
import org.apache.spark.listener.entity.PhMaxJobResult

sealed trait sendProgressTraitByXmpp extends sendProgressTrait {
    def sendProcess(obj: channelEntity)(implicit send: channelEntity => Unit): Unit = {
        implicit val resolveTimeout: Timeout = Timeout(5 seconds)
        send(obj)
    }
}

case class sendXmppSingleProgress(company_id: String, user_id: String, call: String, job_id: String)
                                 (implicit send: channelEntity => Unit) extends sendProgressTraitByXmpp with phLogTrait {
    val singleProgress: Map[String, Any] => Unit = { map =>
        val progress = map("progress").asInstanceOf[Int]
        val result = new PhMaxJobResult
        result.company_id = company_id
        result.user_id = user_id
        result.call = call
        result.job_id = job_id
        result.percentage = progress
        sendProcess(result)(send)
        phLog(s"$company_id $user_id current $call progress = $progress")
    }
}

case class sendXmppMultiProgress(company_id: String, user_id: String, call: String, job_id: String)
                                (p_current: Double, p_total: Double)
                                (implicit send: channelEntity => Unit) extends sendProgressTraitByXmpp with phLogTrait {
    private var previousProgress = -1
    val multiProgress: Map[String, Any] => Unit = { map =>
        val progress = map("progress").asInstanceOf[Int]
        val currentprogress = p_total match {
            case d: Double if d < 1 => 0
            case _ => ((p_current - 1) / p_total * 100 + progress / p_total).toInt
        }

        if(currentprogress > previousProgress){
            val result = new PhMaxJobResult
            result.company_id = company_id
            result.user_id = user_id
            result.call = call
            result.job_id = job_id
            result.percentage = progress

            sendProcess(result)(send)
            phLog(s"xmpp msg => $company_id $user_id current $call progress = " + currentprogress)
            previousProgress = currentprogress
        }
    }
}
