package com.pharbers.reflect.PhEntity.confEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro
import com.pharbers.reflect.PhEntity.confTrait.PhConfTrait

@ToStringMacro
class PhXmppConf extends commonEntity with PhConfTrait {
    var disableSend: Boolean = false
    val xmpp_report: String = ""
}
