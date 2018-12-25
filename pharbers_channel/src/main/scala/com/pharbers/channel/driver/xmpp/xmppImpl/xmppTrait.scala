package com.pharbers.channel.driver.xmpp.xmppImpl

import com.pharbers.channel.detail.channelEntity

/**
  * @ ProjectName pharbers-xmppClient.com.pharbers.xmpp.xmppTrait
  * @ author jeorch
  * @ date 18-9-11
  * @ Description: TODO
  */
trait xmppTrait {
    /** 发送 msg */
    val encodeHandler: channelEntity => String
    /** 接受 msg */
    val decodeHandler: String => channelEntity
    /** 处理 msg */
    val consumeHandler: (String, String) => Unit
}
