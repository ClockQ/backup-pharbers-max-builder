package com.pharbers.channel.driver.xmpp.xmppImpl

import com.pharbers.baseModules.PharbersInjectModule

trait xmppBase extends PharbersInjectModule {
    override val id: String = "xmpp-module"
    override val configPath: String = "pharbers_config/xmpp_manager.xml"
    override val md = "xmpp-host" :: "xmpp-port" :: "xmpp-user" ::
            "xmpp-pwd" :: "xmpp-listens" :: "xmpp-report" :: "xmpp-pool-num" :: Nil

    val xmpp_host = config.mc.find(p => p._1 == "xmpp-host").get._2.toString
    val xmpp_port = config.mc.find(p => p._1 == "xmpp-port").get._2.toString.toInt
    val xmpp_user = config.mc.find(p => p._1 == "xmpp-user").get._2.toString
    val xmpp_pwd = config.mc.find(p => p._1 == "xmpp-pwd").get._2.toString

    val xmpp_listens = config.mc.find(p => p._1 == "xmpp-listens").get._2.toString.split("#")
    val xmpp_report = config.mc.find(p => p._1 == "xmpp-report").get._2.toString.split("#")

    val xmpp_pool_num = config.mc.find(p => p._1 == "xmpp-pool-num").get._2.toString.toInt

}
