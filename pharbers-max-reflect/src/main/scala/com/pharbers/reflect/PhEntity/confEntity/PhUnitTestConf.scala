package com.pharbers.reflect.PhEntity.confEntity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro
import com.pharbers.reflect.PhEntity.confTrait.{PhActionTrait, PhConfTrait}

@ToStringMacro
class PhUnitTestConf extends commonEntity with PhActionTrait {

    var ym: String = ""
    var mkt: String = ""
    var test_path: String = ""
    var test_name: String = ""

    val jar_path: String = ""
    val clazz: String = ""

    val conf: Map[String, String] = Map()
}