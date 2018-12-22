package com.pharbers.reflect.PhEntity.jobEntity

import com.pharbers.macros.common.connecting.ToStringMacro
import com.pharbers.reflect.PhEntity.confTrait.PhConfTrait

@ToStringMacro
class PhUnitTestConf extends PhConfTrait {

    var ym: String = ""
    var mkt: String = ""
    var test_path: String = ""
    var test_name: String = ""

    val conf: Map[String, String] = Map()
}