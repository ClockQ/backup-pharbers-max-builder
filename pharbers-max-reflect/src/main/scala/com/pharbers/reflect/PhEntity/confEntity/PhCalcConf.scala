package com.pharbers.reflect.PhEntity.jobEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro
import com.pharbers.reflect.PhEntity.confTrait.PhActionTrait

@ToStringMacro
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