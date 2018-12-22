package com.pharbers.unitTest.common

import java.util.UUID
import scala.io.Source
import java.io.{File, PrintWriter}
import com.pharbers.jsonapi.model.RootObject
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.reflect.PhEntity.{PhAction, PhCalcConf, PhPanelConf, PhUnitTestConf}

object generateNameAction extends CirceJsonapiSupport  {

    def apply(json_file: String, tmp_file: String): PhAction = {
        import io.circe.syntax._
        import com.pharbers.macros._
        import com.pharbers.macros.convert.jsonapi.JsonapiMacro._

        val json_str: String = Source.fromFile(json_file).getLines.mkString
        val jsonapi: RootObject = decodeJson[RootObject](parseJson(json_str))
        val action: PhAction = formJsonapi[PhAction](jsonapi)

        val panelConf: List[PhPanelConf] = action.panelConf.get
        val calcConf: List[PhCalcConf] = action.calcConf.get
        val unitTestConf: List[PhUnitTestConf] = action.unitTestConf.get

        val tmp: List[(PhPanelConf, PhCalcConf, PhUnitTestConf)] = unitTestConf.map { unitTest =>
            val panel = panelConf.find(x => x.ym == unitTest.ym && x.mkt == unitTest.mkt).get
            val calc = calcConf.find(x => x.ym == unitTest.ym && x.mkt == unitTest.mkt).get

            val panel_name = UUID.randomUUID().toString
            val max_name = UUID.randomUUID().toString
            val max_search_name = UUID.randomUUID().toString
            val test_name = UUID.randomUUID().toString

            panel.panel_name = panel_name

            calc.panel_name = panel_name
            calc.max_name = max_name
            calc.max_search_name = max_search_name

            unitTest.panel_name = panel_name
            unitTest.max_name = max_name
            unitTest.max_search_name = max_search_name
            unitTest.test_name = test_name

            (panel, calc, unitTest)
        }

        action.job_id = UUID.randomUUID().toString
        action.panelConf = Some(tmp.map(_._1))
        action.calcConf = Some(tmp.map(_._2))
        action.unitTestConf = Some(tmp.map(_._3))

//    println(action)
//    println(toJsonapi(action).asJson.noSpaces)

        val writer = new PrintWriter(new File(tmp_file))
        writer.write(toJsonapi(action).asJson.noSpaces)
        writer.close()

        action
    }
}


