package com.pharbers.reflect.util

import java.util.UUID
import scala.io.Source
import java.io.{File, PrintWriter}
import com.pharbers.jsonapi.model.RootObject
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.reflect.PhEntity.confEntity.{PhCalcConf, PhPanelConf, PhUnitTestConf}

object generateNameAction {

    import Jsonapi._

    def generateNameAction(actionJob: PhActionJob): PhActionJob = {

        val panelConf: List[PhPanelConf] = actionJob.panelConf.getOrElse(Nil)
        val calcConf: List[PhCalcConf] = actionJob.calcConf.getOrElse(Nil)
        val unitTestConf: List[PhUnitTestConf] = actionJob.unitTestConf.getOrElse(Nil)

        val tmp_panel = panelConf.map { panel =>
            panel.panel_name = {
                if (panel.panel_name.isEmpty) UUID.randomUUID().toString
                else panel.panel_name
            }
            panel
        }

        val tmp_calc = calcConf.map { calc =>
            calc.panel_name = {
                if (calc.panel_name.isEmpty) {
                    panelConf.find(x => x.ym == calc.ym && x.mkt == calc.mkt) match {
                        case Some(one) => one.panel_name
                        case None => UUID.randomUUID().toString
                    }
                } else calc.panel_name
            }
            calc.max_name = {
                if (calc.max_name.isEmpty) UUID.randomUUID().toString
                else calc.max_name
            }
            calc.max_search_name = {
                if (calc.max_search_name.isEmpty) UUID.randomUUID().toString
                else calc.max_search_name
            }

            calc
        }

        val tmp_unitTest = unitTestConf.map { unitTest =>
            unitTest.test_name = {
                if (unitTest.test_name.isEmpty) UUID.randomUUID().toString
                else unitTest.test_name
            }
            unitTest
        }

        actionJob.job_id = UUID.randomUUID().toString
        actionJob.panelConf = if(tmp_panel.isEmpty) None else Some(tmp_panel)
        actionJob.calcConf = if(tmp_calc.isEmpty) None else Some(tmp_calc)
        actionJob.unitTestConf = if(tmp_unitTest.isEmpty) None else Some(tmp_unitTest)

        actionJob
    }

    def generateNameAction(json_file: String, tmp_file: String = ""): PhActionJob = {
        import io.circe.syntax._
        import com.pharbers.macros._
        import com.pharbers.macros.convert.jsonapi.JsonapiMacro._

        val json_str: String = Source.fromFile(json_file).getLines.mkString
        val jsonapi: RootObject = json2Jsobj(str2Json(json_str))
        val actionJob: PhActionJob = generateNameAction(formJsonapi[PhActionJob](jsonapi))

        if (tmp_file != "") {
            val writer = new PrintWriter(new File(tmp_file))
            writer.write(toJsonapi(actionJob).asJson.noSpaces)
            writer.close()
        }

        actionJob
    }
}