package com.pharbers.channel.driver.xmpp

import org.jivesoftware.smack._
import akka.routing.RoundRobinPool
import com.pharbers.util.log.phLogTrait
import org.jivesoftware.smack.packet.Message
import com.pharbers.channel.detail.channelEntity
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, InvalidActorNameException, PoisonPill, Props}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.channel.driver.xmpp.xmppImpl.{xmppBase, xmppMsgPool, xmppTrait}

object xmppClient extends phLogTrait {
    def props(handler: xmppTrait)(implicit xmppConfig: XmppConfigType) = Props(new xmppClient(handler))

    def startLocalClient(handler: xmppTrait)(implicit as: ActorSystem, xmppConfig: XmppConfigType): String = {
        try {
            val actorRef = as.actorOf(props(handler), name = xmppConfig("xmpp_user"))
            phLog("actor address : " + actorRef.path.toString)
            actorRef ! "start"
            actorRef.path.toString
        } catch {
            case _: InvalidActorNameException => s"akka://${as.name}/user/${xmppConfig("xmpp_user")}"
        }
    }

    // 不好使, 有问题, 我真不会了, 老齐立字据了, 真不会, 不会不会
    def stopLocalClient()(implicit as: ActorSystem, xmppConfig: XmppConfigType): Unit = {
        try {
            val actorRef = as.actorSelection(s"akka://${as.name}/user/${xmppConfig("xmpp_user")}")
            actorRef ! "stop"
        } catch {
            case _: Exception => Unit
        }
    }
}

class xmppClient(handler: xmppTrait)(implicit override val xmppConfig: XmppConfigType)
        extends xmppBase with Actor with ActorLogging with phLogTrait {

    val xmpp_pool: ActorRef = context.actorOf(RoundRobinPool(xmpp_pool_num).props(xmppMsgPool.props(handler)), name = "xmpp-receiver")
    lazy val xmpp_config: ConnectionConfiguration = new ConnectionConfiguration(xmpp_host, xmpp_port)
    lazy val (conn, cm): (XMPPConnection, ChatManager) = {
        try {
            val conn = new XMPPConnection(xmpp_config)
            conn.connect()
            conn.login(xmpp_user, xmpp_pwd)
            (conn, conn.getChatManager)
        } catch {
            case ex: XMPPException =>
                ex.printStackTrace()
                (null, null)
        }
    }

    def startXmpp(): Unit = {
        cm.addChatListener(new ChatManagerListener {
            override def chatCreated(chat: Chat, b: Boolean): Unit = {
                phLog("chat created !!!")
                if (!chat.getListeners.isEmpty) return

                chat.addMessageListener(new MessageListener {
                    override def processMessage(chat: Chat, message: Message): Unit = {
                        if (xmpp_listens.contains(chat.getParticipant.split("/").head)) {
                            phLog("receiving message:" + message.getBody)
                            phLog("message from :" + chat.getParticipant)
                            xmpp_pool ! (chat.getParticipant.substring(0, chat.getParticipant.indexOf("/")), message.getBody)
                        }
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

    def stopXmpp(): Unit = {
        conn.disconnect()
        self ! PoisonPill
        xmpp_pool ! PoisonPill
    }

    override def receive: Receive = {
        case "start" => startXmpp()
        case "stop" => stopXmpp()
        case msg: channelEntity => xmpp_pool ! (msg, this)
        case _ => msg: Any => phLog("发送非法:" + msg)
    }
}
