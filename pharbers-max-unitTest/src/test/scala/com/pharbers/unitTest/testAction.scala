package com.pharbers.unitTest

import akka.actor.ActorSystem
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}

object testAction extends App {
    implicit val as: ActorSystem = ActorSystem("maxActor")
    resultCheckJob(StringArgs("max_json/nhwa-mz-1804.json"))(as).perform(MapArgs(Map()))
}
