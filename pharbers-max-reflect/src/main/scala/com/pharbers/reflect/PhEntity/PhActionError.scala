package com.pharbers.reflect.PhEntity

import com.pharbers.macros.api.errorEntity
import com.pharbers.channel.detail.channelEntity

/**
  * @description: 错误信息实体
  * @author: clock
  * @date: 2018-12-28 22:34
  */
case class PhActionError(status: String, code: String,
                         title: String, detail: String) extends errorEntity with channelEntity