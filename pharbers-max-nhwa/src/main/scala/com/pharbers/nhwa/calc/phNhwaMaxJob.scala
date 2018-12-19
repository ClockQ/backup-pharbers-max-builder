package com.pharbers.nhwa.calc

import akka.actor.ActorSelection
import com.pharbers.common.calc.phCommonMaxJobTrait

case class phNhwaMaxJob(args: Map[String, String])
                       (override implicit val sendActor: ActorSelection) extends phCommonMaxJobTrait