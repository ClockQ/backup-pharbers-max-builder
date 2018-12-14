package com.pharbers.builder

import akka.actor.Actor
import com.pharbers.builder.phMarketTable.{Builderimpl, readJsonTrait}
import play.api.libs.json.JsValue
import play.api.libs.json.Json.toJson
import com.pharbers.driver.PhRedisDriver
import com.pharbers.pactions.actionbase.{JVArgs, MapArgs, StringArgs}
import com.pharbers.spark.phSparkDriver

object phBuilder {
    def apply(_company: String, _user: String, _job_id: String)
             (implicit _actor: Actor): phBuilder = {
        new phBuilder {
            override lazy val user: String = _user
            override lazy val job_id: String = _job_id
            override lazy val company: String = _company
            override implicit val actor: Actor = _actor
        }
    }
}

trait phBuilder extends readJsonTrait {
    val user: String
    val job_id: String
    val company: String
    implicit val actor: Actor

    var mapping: Map[String, String] = Map(
        "user_id" -> user,
        "company_id" -> company,
        "job_id" -> job_id
    )

    def set(key: String, value: String): phBuilder = {
        mapping += key -> value
        this
    }

    def set(map: Map[String, String]): phBuilder = {
        mapping ++= map
        this
    }

    val builderimpl = Builderimpl(mapping("company_id"))
    import builderimpl._

    def doCalcYM(): JsValue = {
        val ymInstMap = getYmInst
        val ckArgLst = ymInstMap("source").split("#").toList

        if(!parametCheck(ckArgLst, mapping)(ck_base))
            throw new Exception("input wrong")

        val clazz: String = ymInstMap("instance")
        val result = impl(clazz, mapping).perform(MapArgs(Map().empty))
                .asInstanceOf[MapArgs].get("result").asInstanceOf[JVArgs].get
//        phSparkDriver(job_id).sc.stop()
        //TODO:手动释放本次算能
        phSparkDriver(job_id).stopCurrConn

        result
    }

    def doPanel(): JsValue = {
        val ymLst = mapping("yms").split("#")
        if (ymLst.isEmpty) return toJson(Map("job_id" -> mapping("job_id"), "error" -> "no yms"))
        val jobSum = ymLst.length * mktLst.length
        mapping += "p_total" -> jobSum.toString

        for (ym <- ymLst; mkt <- mktLst) {
            if (ym == "") return toJson(Map("job_id" -> mapping("job_id"), "error" -> "ym error"))
            mapping += "ym" -> ym
            mapping += "mkt" -> mkt
            val panelInstMap = getPanelInst(mkt)
            val ckArgLst = panelInstMap("source").split("#").toList ::: panelInstMap("args").split("#").toList ::: Nil
            mapping ++= panelInstMap
            mapping += "p_current" -> (mapping.getOrElse("p_current", "0").toInt + 1).toString

            //TODO:目前数据库中的配置不是最新的,使用本地配置文件进行替换,json数据中有重复的,文件最下面是最新更新的,所以取last
            testData.filter(x => company == x("company") && mkt == x("market")) match{
                case Nil => println(s"未用json文件更新${company}-${mkt}的配置")
                case lst => mapping ++= lst.last
            }
//            mapping ++= testData.filter(x => company == x("company") && mkt == x("market")).last

            if(!parametCheck(ckArgLst, mapping)(m => ck_base(m) && ck_panel(m)))
                throw new Exception("input wrong")

            val clazz: String = panelInstMap("instance")
            val result = impl(clazz, mapping).perform(MapArgs(Map().empty))
//                    .asInstanceOf[MapArgs]
//                    .get("phSavePanelJob")
//                    .asInstanceOf[StringArgs].get
//            phSparkDriver(job_id).sc.stop()
            //TODO:手动释放本次算能
            phSparkDriver(job_id).stopCurrConn
//            result
        }

        toJson(mapping("job_id"))
    }

    def doMax(): JsValue = {
        val rd = new PhRedisDriver()
        val panelLst = rd.getSetAllValue(mapping("job_id"))
        mapping += "p_total" -> panelLst.size.toString

        val maxResult = panelLst.map { panel =>
            val mkt = rd.getMapValue(panel, "mkt")
            val ym = rd.getMapValue(panel, "ym")
            val maxInstMap = getMaxInst(mkt)
            mapping += "ym" -> ym
            mapping += "mkt" -> mkt
            mapping += "panel_name" -> panel
            mapping += "p_current" -> (mapping.getOrElse("p_current", "0").toInt + 1).toString
            mapping ++= maxInstMap

            val ckArgLst = maxInstMap("args").split("#").toList ::: Nil

            //TODO:目前数据库中的配置不是最新的,使用本地配置文件进行替换,json数据中有重复的,文件最下面是最新更新的,所以取last
            testData.filter(x => company == x("company") && mkt == x("market")) match{
                case Nil => println(s"未用json文件更新${company}-${mkt}的配置")
                case lst => mapping ++= lst.last
            }
//            mapping ++= testData.filter(x => company == x("company") && mkt == x("market")).last

            if(!parametCheck(ckArgLst, mapping)(m => ck_base(m) && ck_panel(m) && ck_max(m)))
                throw new Exception("input wrong")

            val clazz: String = maxInstMap("instance")
            val result = impl(clazz, mapping).perform(MapArgs(Map().empty))
                    .asInstanceOf[MapArgs]
                    .get("max_persistent_action")
                    .asInstanceOf[StringArgs].get
//            phSparkDriver(job_id).sc.stop()
            //TODO:手动释放本次算能
            phSparkDriver(job_id).stopCurrConn
            result
        }.mkString("#")

        toJson(maxResult)
    }

}
