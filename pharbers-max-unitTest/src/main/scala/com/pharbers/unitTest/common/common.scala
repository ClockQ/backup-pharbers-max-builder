package com.pharbers.unitTest

import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.reflect.PhEntity.confEntity.{PhCalcConf, PhPanelConf, PhUnitTestConf}

package object common {

    implicit class getSingleAction(action: PhActionJob) {
        def toSingleList: List[PhActionJob] = {
            println(action)
            val panelConf: List[PhPanelConf] = action.panelConf.getOrElse(Nil)
            val calcConf: List[PhCalcConf] = action.calcConf.getOrElse(Nil)
            val unitTestConf: List[PhUnitTestConf] = action.unitTestConf.getOrElse(Nil)

            unitTestConf.map { unitTest =>
                val panel = panelConf.find(x => x.ym == unitTest.ym && x.mkt == unitTest.mkt)
                val calc = calcConf.find(x => x.ym == unitTest.ym && x.mkt == unitTest.mkt)
                val tmp = new PhActionJob()
                tmp.job_id = action.job_id
                tmp.user_id = action.user_id
                tmp.company_id = action.company_id
                tmp.panel_path = action.panel_path
                tmp.max_path = action.max_path
                tmp.prod_lst = action.prod_lst
                tmp.xmppConf = action.xmppConf
                tmp.calcYmConf = action.calcYmConf

                tmp.panelConf = panel.map(_ :: Nil)
                tmp.calcConf = calc.map(_ :: Nil)
                tmp.unitTestConf = Some(unitTest :: Nil)
                tmp
            }
        }
    }
}