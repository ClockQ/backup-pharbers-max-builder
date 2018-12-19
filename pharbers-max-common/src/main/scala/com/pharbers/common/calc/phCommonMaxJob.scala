package com.pharbers.common.calc

import akka.actor.ActorSelection

case class phCommonMaxJob(args: Map[String, String])
                         (override implicit val sendActor: ActorSelection) extends phCommonMaxJobTrait