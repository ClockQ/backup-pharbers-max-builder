package com.pharbers.unitTest.action

import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.pactions.actionbase.pActionArgs

case class PhActionArgs(action: PhActionJob) extends pActionArgs {
    type t = PhActionJob
    override def get: PhActionJob = action
}