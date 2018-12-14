package com.pharbers.channel.driver.xmpp.xmppImpl

import com.pharbers.channel.detail.channelEntity

/**
  * @ ProjectName pharbers-xmppClient.com.pharbers.xmpp.xmppTrait
  * @ author jeorch
  * @ date 18-9-11
  * @ Description: TODO
  */
trait xmppTrait {
    val encodeHandler: channelEntity => String
    val decodeHandler: String => channelEntity
    val consumeHandler: String => Unit
}
