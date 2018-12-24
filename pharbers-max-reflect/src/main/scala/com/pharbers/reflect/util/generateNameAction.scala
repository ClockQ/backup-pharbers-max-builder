package com.pharbers.reflect.util

import java.util.UUID
import scala.io.Source
import java.io.{File, PrintWriter}
import com.pharbers.jsonapi.model.RootObject
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.reflect.PhEntity.confEntity.{PhCalcConf, PhPanelConf, PhUnitTestConf}

object generateNameAction {

    def generateNameAction(actionJob: PhActionJob, tmp_file: String = ""): PhActionJob = {
        import io.circe.syntax._
        import com.pharbers.macros._
        import com.pharbers.macros.convert.jsonapi.JsonapiMacro._

        val panelConf: List[PhPanelConf] = actionJob.panelConf.getOrElse(Nil)
        val calcConf: List[PhCalcConf] = actionJob.calcConf.getOrElse(Nil)
        val unitTestConf: List[PhUnitTestConf] = actionJob.unitTestConf.getOrElse(Nil)

        val tmp_panel = panelConf.map{ panel =>
            val panel_name = UUID.randomUUID().toString
            panel.panel_name = panel_name
            panel
        }

        val tmp_calc = calcConf.map{ calc =>
            val max_name = UUID.randomUUID().toString
            val max_search_name = UUID.randomUUID().toString
            val panel = panelConf.find(x => x.ym == calc.ym && x.mkt == calc.mkt)
            calc.panel_name = panel match {
                case Some(one) => one.panel_name
                case None => UUID.randomUUID().toString
            }
            calc.max_name = max_name
            calc.max_search_name = max_search_name
            calc
        }

        val tmp_unitTest = unitTestConf.map{ unitTest =>
            val test_name = UUID.randomUUID().toString
            unitTest.test_name = test_name
            unitTest
        }

        actionJob.job_id = UUID.randomUUID().toString
        actionJob.panelConf = Some(tmp_panel)
        actionJob.calcConf = Some(tmp_calc)
        actionJob.unitTestConf = Some(tmp_unitTest)

        if(tmp_file != ""){
            val writer = new PrintWriter(new File(tmp_file))
            writer.write(toJsonapi(actionJob).asJson.noSpaces)
            writer.close()
        }

        actionJob
    }

    def generateNameAction(json_file: String, tmp_file: String = ""): PhActionJob = {
        import com.pharbers.macros._
        import com.pharbers.macros.convert.jsonapi.JsonapiMacro._

        val json_str: String = Source.fromFile(json_file).getLines.mkString
        val jsonapi: RootObject = decodeJson[RootObject](parseJson(json_str))
        val actionJob: PhActionJob = formJsonapi[PhActionJob](jsonapi)

        generateNameAction(actionJob, tmp_file)
    }
}