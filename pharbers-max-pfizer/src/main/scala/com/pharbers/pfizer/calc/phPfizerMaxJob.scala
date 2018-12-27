package com.pharbers.pfizer.calc

import com.pharbers.channel.detail.channelEntity
import com.pharbers.common.calc.phCommonMaxJobTrait

case class phPfizerMaxJob(args: Map[String, String])
                         (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait