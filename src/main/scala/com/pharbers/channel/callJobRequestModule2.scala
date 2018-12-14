package com.pharbers.channel

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.pharbers.pattern2.detail.{PhMaxJob, commonresult}
import com.pharbers.pattern2.error.commonerror
import com.pharbers.pattern2.steps.commonstep

import scala.concurrent.Await
import scala.concurrent.duration._

case class executeJob(override val args: commonresult)(implicit context: ActorSystem) extends commonstep {
    
    override val module: String = "max calc"
    override val methed: String = "call max job"

    override def processes(pr: Option[commonresult]): (Option[commonresult], Option[commonerror]) = {
        println(s"args $args")
        implicit val t: Timeout = 5 hours
        
        val exec = context.actorOf(doJobActor2.props)
        
        val max_job = args.asInstanceOf[PhMaxJob]
        val job_instance =
            max_job.call match {
                case "ymCalc" => exec ? doJobActor2.msg_doYmCalc2(max_job)
                case "panel" => exec ? doJobActor2.msg_doPanel2(max_job)
                case "max" => exec ? doJobActor2.msg_doCalc2(max_job)
            }
        
        val result = Await.result(job_instance.mapTo[Any], t.duration)
        println(result)
        (Some(args), None)
    }
}
