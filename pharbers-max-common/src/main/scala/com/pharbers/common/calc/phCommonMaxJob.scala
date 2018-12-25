package com.pharbers.common.calc

import com.pharbers.channel.detail.channelEntity

case class phCommonMaxJob(args: Map[String, String])
                         (override implicit val send: channelEntity => Unit) extends phCommonMaxJobTrait