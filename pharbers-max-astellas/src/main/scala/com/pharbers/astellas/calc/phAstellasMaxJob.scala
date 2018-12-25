package com.pharbers.astellas.calc

import com.pharbers.channel.detail.channelEntity
import com.pharbers.common.calc.phCommonMaxJobTrait

case class phAstellasMaxJob(args: Map[String, String])
                           (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait