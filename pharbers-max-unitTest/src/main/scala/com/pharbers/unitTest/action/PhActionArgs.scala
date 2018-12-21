package com.pharbers.unitTest.action

import com.pharbers.reflect.PhEntity.PhAction
import com.pharbers.pactions.actionbase.pActionArgs

case class PhActionArgs(action: PhAction) extends pActionArgs {
    type t = PhAction
    override def get: PhAction = action
}