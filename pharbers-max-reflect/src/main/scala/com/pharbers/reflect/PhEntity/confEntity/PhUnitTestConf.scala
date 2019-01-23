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

    var jar_path: String = ""
    var clazz: String = ""

    var conf: Map[String, String] = Map()
}