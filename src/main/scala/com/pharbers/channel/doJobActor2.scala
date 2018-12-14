package com.pharbers.channel

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props}
import com.pharbers.ErrorCode.getErrorCodeByName
import com.pharbers.builder.phBuilder
import com.pharbers.channel.chanelImpl.responsePusher
import com.pharbers.channel.doJobActor2.{msg_doCalc2, msg_doKill2, msg_doPanel2, msg_doYmCalc2}
import com.pharbers.common.algorithm.alTempLog
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}
import com.pharbers.pattern2.detail.{PhMaxJob, PhMaxJobResult, commonresult}
import play.libs.Json
//import com.pharbers.PhMaxJob
import play.api.libs.json.JsValue
import play.api.libs.json.Json.toJson

object doJobActor2 {
    def name = "doJob2"
    def props: Props = Props[doJobActor2]

    case class msg_doYmCalc2(job : PhMaxJob)
    case class msg_doPanel2(job : PhMaxJob)
    case class msg_doCalc2(job : PhMaxJob)
    case class msg_doKill2(job : PhMaxJob)
}

class doJobActor2 extends Actor with ActorLogging {
    implicit val acc: Actor = this

    override def receive: Receive = {
        case msg_doYmCalc2(jv) => doYmCalc(jv)
        case msg_doPanel2(jv) => doPanel(jv)
        case msg_doCalc2(jv) => doCalc(jv)
        case msg_doKill2(jv) => //doKill(jv)
        case _ => ???
    }

    def sendMessage(company: String, user_id: String, call: String, job_id: String, value: String): Unit = {
        implicit val a = context.actorSelection("akka://maxActor/user/xmpp")
        
        val message: String = value.substring(1, value.length-1)
        /**
          * make an instance of PhMaxJobResult
          */
        val result = new PhMaxJob
        result.user_id = user_id
        result.company_id = company
        result.call = call
        result.job_id = job_id
        result.percentage = 100
        result.message = message
        
        a ! result
    }

    def sendError(str: String, str1: String, str2: String, value: JsValue): Unit = {
        // TODO :
    }

    def doYmCalc(jv: PhMaxJob): Unit = {
        val company = jv.company_id //(jv \ "company_id").asOpt[String].get
        val user = jv.user_id //(jv \ "user_id").asOpt[String].get
        val job_id = jv.job_id
        val args: Map[String, String] = Map(
            "cpa" -> jv.cpa,
            "gycx" -> jv.gycx,
            "not_arrival_hosp_file" -> jv.not_arrival_hosp_file
        )
        try{
            alTempLog(s"doYmCalc, company is = $company, user is = $user")

            val ymLst = phBuilder(company, user, job_id).set(args).doCalcYM()
            alTempLog("计算月份完成, result = " + ymLst)
            sendMessage(company, user, "ymCalc", job_id, ymLst.toString())
            sender() ! ymLst
//            responsePusher().callJobResponse(Map("job_id" -> args("job_id")), "done")(jv)// send Kafka message
        } catch {
            case ex: Exception => sendError(company, user, "ymCalc", toJson(Map("code" -> toJson(getErrorCodeByName(ex.getMessage)), "message" -> toJson(ex.getMessage))))
        }
    }
    
//    def doPanel(mapping: Map[String, String]): String = {
////        val panelInstMap = getPanelInst(mkt)
////        val ckArgLst = panelInstMap("source").split("#").toList ::: panelInstMap("args").split("#").toList ::: Nil
////        val args = mapping ++ panelInstMap ++ testData.find(x => company == x("company") && mkt == x("market")).get
////
////        if(!parametCheck(ckArgLst, args)(m => ck_base(m) && ck_panel(m)))
////            throw new Exception("input wrong")
//
//        val clazz: String = panelInstMap("instance")
//        val result = impl(clazz, args).perform(MapArgs(Map().empty))
//                .asInstanceOf[MapArgs]
//                .get("phSavePanelJob")
//                .asInstanceOf[StringArgs].get
//        //        phSparkDriver().sc.stop()
//        result
//    }
    
    def doPanel(jv: PhMaxJob): Unit = {
        val company = jv.company_id // (jv \ "company_id").asOpt[String].get
        val user_id = jv.user_id //(jv \ "user_id").asOpt[String].get
        val job_id = jv.job_id
        val yms = jv.yms
        val args: Map[String, String] = Map(
            "cpa" -> jv.cpa,
            "gycx" -> jv.gycx,
            "not_arrival_hosp_file" -> jv.not_arrival_hosp_file,
            "yms" -> yms
        )
        
        try{
            alTempLog(s"doPanel, company is = $company, user is = $user_id")
//            sendMessage(company, user, "panel", "start", toJson(Map("progress" -> toJson("0"))))

            val panel_result = phBuilder(company, user_id, job_id).set(args).doPanel()

            alTempLog("生成panel完成")
            sendMessage(company, user_id, "panel", job_id, panel_result.toString())
            sender() ! panel_result
//            responsePusher().callJobResponse(Map("job_id" -> args("job_id")), "done")(jv)// send Kafka message
        } catch {
            case ex: Exception => sendError(company, user_id, "panel", toJson(Map("code" -> toJson(getErrorCodeByName(ex.getMessage)), "message" -> toJson(ex.getMessage))))
        }
    }

    def doCalc(jv: PhMaxJob): Unit = {
        val company = jv.company_id //(jv \ "company_id").asOpt[String].get
        val user = jv.user_id //(jv \ "user_id").asOpt[String].get
        val args: Map[String, String] = Map(
            "cpa" -> jv.cpa,
            "gycx" -> jv.gycx,
            "not_arrival_hosp_file" -> jv.not_arrival_hosp_file
        )
        val job_id = jv.job_id //getArgs2Map(jv)("job_id")
        try {
            alTempLog(s"doCalc, company is = $company, user is = $user")
//            sendMessage(company, user, "calc", "start", toJson(Map("progress" -> toJson("0"))))

            val max_result = phBuilder(company, user, job_id).set(args).doMax()

            alTempLog("计算完成")
            sendMessage(company, user, "calc", job_id, max_result.toString())
            sender() ! max_result
//            responsePusher().callJobResponse(Map("job_id" -> job_id), "done")(jv)// send Kafka message
        } catch {
            case ex: Exception => sendError(company, user, "calc", toJson(Map("code" -> toJson(getErrorCodeByName(ex.getMessage)), "message" -> toJson(ex.getMessage))))
        }
    }
}
