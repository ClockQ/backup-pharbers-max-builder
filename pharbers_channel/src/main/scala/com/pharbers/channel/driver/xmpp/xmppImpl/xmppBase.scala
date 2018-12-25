package com.pharbers.channel.driver.xmpp.xmppImpl

import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

object xmppBase {
    type XmppConfigType = Map[String, String]
}

trait xmppBase {
    val xmppConfig: XmppConfigType
    protected val xmpp_host: String = xmppConfig("xmpp_host").toString
    protected val xmpp_port: Int = xmppConfig("xmpp_port").toString.toInt
    protected val xmpp_user: String = xmppConfig("xmpp_user").toString
    protected val xmpp_pwd: String = xmppConfig("xmpp_pwd").toString
    protected val xmpp_listens: Array[String] = xmppConfig("xmpp_listens").toString.split("#").filter(_.nonEmpty)
    protected val xmpp_pool_num: Int = xmppConfig("xmpp_pool_num").toString.toInt
}