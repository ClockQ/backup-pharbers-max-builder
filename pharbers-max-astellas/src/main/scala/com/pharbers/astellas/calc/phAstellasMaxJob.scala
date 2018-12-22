package com.pharbers.astellas.calc

import akka.actor.ActorSelection
import com.pharbers.common.calc.phCommonMaxJobTrait

case class phAstellasMaxJob(args: Map[String, String])
                           (override implicit val sendActor: ActorSelection) extends phCommonMaxJobTrait