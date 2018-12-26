package com.pharbers.main.PhUtil

import com.pharbers.reflect.PhEntity.PhActionJob

/**
  * @description: Golang 传递的空关联非None,需根据id是否为空判断
  * @author: clock
  * @date: 2018-12-26 11:20
  */
object FilterIDIsNull {
    implicit class FilterIDIsNull(action: PhActionJob) {
        def filterNullId(): PhActionJob = {
            action.calcYmConf = action.calcYmConf match {
                case Some(one) => if(one.id.isEmpty) None else Some(one)
                case None => None
            }
            action.panelConf = action.panelConf match {
                case Some(lst) =>
                    val tmp = lst.filter(_.id.nonEmpty)
                    if(tmp.isEmpty) None else Some(tmp)
                case None => None
            }
            action.calcConf = action.calcConf match {
                case Some(lst) =>
                    val tmp = lst.filter(_.id.nonEmpty)
                    if(tmp.isEmpty) None else Some(tmp)
                case None => None
            }
            action
        }
    }
}