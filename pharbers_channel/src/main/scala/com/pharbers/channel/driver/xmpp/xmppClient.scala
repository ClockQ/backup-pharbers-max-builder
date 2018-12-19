package com.pharbers.channel.driver.xmpp

import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import org.jivesoftware.smack._
import akka.routing.RoundRobinPool
import com.pharbers.util.log.phLogTrait
import org.jivesoftware.smack.packet.Message
import com.pharbers.channel.detail.channelEntity
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, InvalidActorNameException, PoisonPill, Props}
import com.pharbers.channel.driver.xmpp.xmppImpl.xmppBase.XmppConfigType
import com.pharbers.channel.driver.xmpp.xmppImpl.{xmppBase, xmppMsgPool, xmppTrait}

import scala.language.postfixOps

object xmppClient extends phLogTrait {
    def props(handler: xmppTrait)(implicit xmppConfig: XmppConfigType) = Props(new xmppClient(handler))

    def startLocalClient(handler: xmppTrait)(implicit as: ActorSystem, xmppConfig: XmppConfigType): String = {
        xmppConfig.getOrElse("xmpp_host", throw new Exception("XMPP failed to initialize !!! not found xmpp_host"))
        xmppConfig.getOrElse("xmpp_port", throw new Exception("XMPP failed to initialize !!! not found xmpp_port"))
        xmppConfig.getOrElse("xmpp_user", throw new Exception("XMPP failed to initialize !!! not found xmpp_user"))
        xmppConfig.getOrElse("xmpp_pwd", throw new Exception("XMPP failed to initialize !!! not found xmpp_pwd"))
        xmppConfig.getOrElse("xmpp_listens", throw new Exception("XMPP failed to initialize !!! not found xmpp_listens"))
        xmppConfig.getOrElse("xmpp_report", throw new Exception("XMPP failed to initialize !!! not found xmpp_report"))
        xmppConfig.getOrElse("xmpp_pool_num", throw new Exception("XMPP failed to initialize !!! not found xmpp_pool_num"))

        try {
            startNull()
            val actorRef = as.actorOf(props(handler), name = xmppConfig("xmpp_user"))
            phLog("actor address : " + actorRef.path.toString)
            actorRef ! "start"
            actorRef.path.toString
        } catch {
            case _: InvalidActorNameException => s"akka://${as.name}/user/${xmppConfig("xmpp_user")}"
            case _: Exception => throw new Exception("XMPP failed to initialize")
        }
    }

    def startNull()(implicit as: ActorSystem): Unit = {
        try {
            as.actorOf(xmppNull.props(), name = "null")
        } catch {
            case _: InvalidActorNameException => Unit
        }
    }

    def stopLocalClient()(implicit as: ActorSystem, xmppConfig: XmppConfigType): Unit = {
        import scala.concurrent.duration._
        implicit val t: Timeout = 10 seconds

        try {
            val actorRef = as.actorSelection(s"akka://${as.name}/user/${xmppConfig("xmpp_user")}")
            val stopResult = actorRef ? "stop"
            Await.result(stopResult.mapTo[Boolean], t.duration)
            phLog("xmpp stop result = " + stopResult)
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
        xmpp_pool ! PoisonPill
        self ! PoisonPill
    }

    override def receive: Receive = {
        case "start" => startXmpp()
        case "stop" =>
            stopXmpp()
            sender ! true
        case msg: channelEntity => xmpp_pool ! (msg, this)
        case msg: Any => phLog("发送非法:" + msg)
    }
}
