package com.pharbers.reflect.PhEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro
import com.pharbers.reflect.PhEntity.confTrait.PhConfTrait

@ToStringMacro
class PhXmppConf extends commonEntity with PhConfTrait {
    var disableSend: Boolean = false
    val conf: Map[String, String] = Map()
}
