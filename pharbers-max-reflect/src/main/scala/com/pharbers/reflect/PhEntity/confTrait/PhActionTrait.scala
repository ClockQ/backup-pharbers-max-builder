package com.pharbers.reflect.PhEntity.confTrait

trait PhActionTrait extends PhConfTrait {
    val jar_path: String
    val clazz: String
    val conf: Map[String, String]
}