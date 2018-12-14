//package com.pharbers
//
//import akka.actor.ActorSystem
//import com.pharbers.baseModules.PharbersInjectModule
//import com.pharbers.calc.phMaxScheduleJob
//import com.pharbers.channel.chanelImpl.callJobConsumer
////import com.pharbers.channel.doJobActor2
//import com.pharbers.common.algorithm.alTempLog
//import com.pharbers.pattern2.detail.PhMaxJob
//import com.pharbers.timer.TimerJob
//import com.pharbers.xmpp.xmppClient
//
///**
//  * Created by spark on 18-4-24.
//  */
//
//object main extends App with PharbersInjectModule {
//    val system = ActorSystem("maxActor")
//
//    override val id: String = "max-config"
//    override val configPath: String = "pharbers_config/max-config.xml"
//    override val md = "kafka" :: "xmpp" :: Nil
//
//    lazy val using_kafka = config.mc.find(p => p._1 == "kafka").get._2.toString
//    lazy val using_xmpp = config.mc.find(p => p._1 == "xmpp").get._2.toString
//
////    TimerJob(new phMaxScheduleJob().getClass.getName).start(0, 24 * 60 * 60)
//
//    if (using_kafka == "true") callJobConsumer("max_calc")(system)
//    else if (using_xmpp == "true") xmppClient.startLocalClient(system, new callJobXmppConsumer(system))
//    else Unit
//    alTempLog("MAX Driver started")
//    while (true) {}
//}
//
//
//
////val system = ActorSystem("maxActor")
////val acter_location = xmppClient.startLocalClient(system, new callJobXmppConsumer(system))
////println(acter_location)
////
////val lactor = system.actorSelection(acter_location) // "akka://maxActor/user/xmpp"
////
////val a = sendXmppSingleProgress("company_id", "user_id", "call", "job_id")(lactor).singleProgress
////a(Map("progress" -> 1))