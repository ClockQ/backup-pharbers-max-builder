package com.pharbers.builder.PhEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.builder.PhEntity.confTrait.PhActionTrait

class PhCalcConf extends commonEntity with PhActionTrait {

    var ym: String = ""
    var mkt: String = ""
    var panel_name: String = ""
    var max_name: String = ""
    var max_search_name: String = ""

    val jar_path: String = ""
    val clazz: String = ""

    val conf: Map[String, String] = Map()
}