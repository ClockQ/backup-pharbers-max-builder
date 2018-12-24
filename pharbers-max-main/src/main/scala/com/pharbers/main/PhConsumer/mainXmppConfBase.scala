package com.pharbers.main.PhConsumer

import com.pharbers.baseModules.PharbersInjectModule

object mainXmppConfBase extends PharbersInjectModule {

    override val id: String = "xmpp-module"
    override val configPath: String = "pharbers_config/xmpp_config.xml"
    override val md: List[String] = "xmpp_host" :: "xmpp_port" :: "xmpp_user" ::
            "xmpp_pwd" :: "xmpp_listens" :: "xmpp_report" :: "xmpp_pool_num" :: Nil

    val xmpp_host: String = config.mc.find(p => p._1 == "xmpp_host").get._2.toString
    val xmpp_port: String = config.mc.find(p => p._1 == "xmpp_port").get._2.toString
    val xmpp_user: String = config.mc.find(p => p._1 == "xmpp_user").get._2.toString
    val xmpp_pwd: String = config.mc.find(p => p._1 == "xmpp_pwd").get._2.toString
    val xmpp_listens: String = config.mc.find(p => p._1 == "xmpp_listens").get._2.toString
    val xmpp_report: String = config.mc.find(p => p._1 == "xmpp_report").get._2.toString
    val xmpp_pool_num: String = config.mc.find(p => p._1 == "xmpp_pool_num").get._2.toString

    implicit val xmpp_conf_base: Map[String, String] = Map(
        "xmpp_host" -> xmpp_host,
        "xmpp_port" -> xmpp_port,
        "xmpp_user" -> xmpp_user,
        "xmpp_pwd" -> xmpp_pwd,
        "xmpp_listens" -> xmpp_listens,
        "xmpp_report" -> xmpp_report,
        "xmpp_pool_num" -> xmpp_pool_num
    )
}