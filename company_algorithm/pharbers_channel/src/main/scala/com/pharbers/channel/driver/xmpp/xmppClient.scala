package com.pharbers.channel.driver.xmpp

import org.jivesoftware.smack._
import akka.routing.RoundRobinPool
import com.pharbers.channel.detail.PhMaxJob
import org.jivesoftware.smack.packet.Message
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.pharbers.channel.driver.xmpp.xmppImpl.{xmppBase, xmppMsgPool, xmppTrait}
import com.pharbers.util.log.phLogTrait

object xmppClient extends phLogTrait {
    def props(handler: xmppTrait) = Props(new xmppClient(handler))

    var actorRef: ActorRef = null

    def startLocalClient(as: ActorSystem, handler: xmppTrait): String = {
        if (actorRef == null) {
            actorRef = as.actorOf(props(handler), name = "xmpp")
            phLog("actor address : " + actorRef.path.toString)
            actorRef ! "start"
        }

        actorRef.path.toString
    }
}

class xmppClient(val handler: xmppTrait) extends xmppBase with Actor with ActorLogging with phLogTrait {
    val xmpp_pool: ActorRef = context.actorOf(RoundRobinPool(xmpp_pool_num).props(xmppMsgPool.props(handler)), name = "xmpp-receiver")
    lazy val xmpp_config: ConnectionConfiguration = new ConnectionConfiguration(xmpp_host, xmpp_port)
    lazy val (conn, cm): (XMPPConnection, ChatManager) = {
        try {
            val conn = new XMPPConnection(xmpp_config)
            conn.connect()
            conn.login(xmpp_user, xmpp_pwd)
            (conn, conn.getChatManager)
        } catch {
            case ex: XMPPException => ex.printStackTrace(); (null, null)
        }
    }

    def startXmpp(): Unit = {
        cm.addChatListener(new ChatManagerListener {
            override def chatCreated(chat: Chat, b: Boolean): Unit = {
                phLog("chat created !!!")
                if (!chat.getListeners.isEmpty) return

                chat.addMessageListener(new MessageListener {
                    override def processMessage(chat: Chat, message: Message): Unit = {
                        phLog("receiving message:" + message.getBody)
                        phLog("message from :" + chat.getParticipant)
                        xmpp_pool ! (chat.getParticipant.substring(0, chat.getParticipant.indexOf("/")), message.getBody)
                    }
                })
            }
        })
    }

    def broadcastXmppMsg(reJson: String): Unit = {
        xmpp_report.foreach { userJID =>
            cm.createChat(userJID, null).sendMessage(reJson)
        }
    }

    def stopXmpp(): Unit = conn.disconnect()

    override def receive: Receive = {
        case "start" => startXmpp()
        case msg: PhMaxJob => xmpp_pool ! (msg, this)
        case _ => msg: Any => phLog("发送非法:"+ msg)
    }
}
