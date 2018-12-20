package com.pharbers.main

import scala.io.Source
import akka.actor.ActorSystem
import com.pharbers.spark.phSparkDriver
import com.pharbers.jsonapi.model.RootObject
import com.pharbers.builder.PhEntity.PhAction
import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport

object testReflect extends App with CirceJsonapiSupport {
    import com.pharbers.macros._
    import com.pharbers.builder._
    import com.pharbers.macros.convert.jsonapi.JsonapiMacro._

    val system: ActorSystem = ActorSystem("maxActor")

    val json_file: String = "max_json/astellas-all-1804.json"
    val json_str: String = Source.fromFile(json_file).getLines.mkString
    val json_data = parseJson(json_str)
    val jsonapi = decodeJson[RootObject](json_data)

    val action: PhAction = formJsonapi[PhAction](jsonapi)

    val lactor = startXMPP(action.xmppConf)(system)

    val calcYmResult = reflect(action.calcYmConf.get)(action.ymArgs(action.calcYmConf.get))(lactor).exec()
    println(calcYmResult)

    val panel_length = action.panelConf.get.length
    var pi = 1
    action.panelConf.get.map{ x =>
        val panelResult = reflect(x)(action.panelArgs(pi, panel_length)(x))(lactor).exec()
        pi += 1
        println(panelResult)
    }

    val max_length = action.calcConf.get.length
    var mi = 1
    action.calcConf.get.map{ x =>
        val calcResult = reflect(x)(action.calcArgs(mi, max_length)(x))(lactor).exec()
        mi += 1
        println(calcResult)
    }

    phSparkDriver(action.job_id).stopCurrConn
    stopXMPP(action.xmppConf)(system)
}
