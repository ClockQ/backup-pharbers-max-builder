package com.pharbers.builder.PhEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.builder.PhEntity.confTrait.PhConfTrait

class PhXmppConf extends commonEntity with PhConfTrait {
    var disableSend: Boolean = false
    val conf: Map[String, String] = Map()
}
