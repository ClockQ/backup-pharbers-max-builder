package com.pharbers.builder.PhEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

class PhXmppConf extends commonEntity {
    val isUse = false
    val xmppconfig: XmppConfigType = Map(
        "xmpp_host" -> "192.168.100.172",
        "xmpp_port" -> "5222",
        "xmpp_user" -> "cui",
        "xmpp_pwd" -> "cui",
        "xmpp_listens" -> "lu@localhost",
        "xmpp_report" -> "lu@localhost#admin@localhost",
        "xmpp_pool_num" -> "1"
    )
}
