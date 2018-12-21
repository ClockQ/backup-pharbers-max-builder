package com.pharbers.channel.driver.xmpp.xmppImpl

import org.jivesoftware.smack._
import scala.language.postfixOps
import akka.routing.RoundRobinPool
import com.pharbers.util.log.phLogTrait
import org.jivesoftware.smack.packet.Message
import com.pharbers.channel.detail.channelEntity
import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType

class xmppClient(handler: xmppTrait)(implicit override val xmppConfig: XmppConfigType)
        extends xmppBase with Actor with ActorLogging with phLogTrait {
    val xmpp_pool: ActorRef = context.actorOf(
        RoundRobinPool(xmpp_pool_num).props(xmppMsgPool.props(handler)),
        name = "xmpp-receiver"
    )
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
                if (!chat.getListeners.isEmpty) return

                chat.addMessageListener(new MessageListener {
                    override def processMessage(chat: Chat, message: Message): Unit = {
                        if (message.getBody.nonEmpty && // 接受的消息不为空
                                xmpp_listens.isEmpty || // 监听用户为空, 为全部接受
                                xmpp_listens.contains(chat.getParticipant.split("/").head)) // 只接受监听者列表
                        {
                            phLog("receiving message:" + message.getBody)
                            phLog("message from :" + chat.getParticipant)
                            xmpp_pool ! (chat.getParticipant.split("/").head, message.getBody)
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
    }

    override def receive: Receive = {
        case "start" => startXmpp()
        case "stop" => stopXmpp(); sender ! true
        case msg: channelEntity => xmpp_pool ! (msg, this)
        case msg: Any => phLog("发送非法:" + msg)
    }
}
