package com.pharbers.unitTest

import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.reflect.PhEntity.confEntity.{PhCalcConf, PhPanelConf, PhUnitTestConf}

package object common {

    implicit class getSingleAction(action: PhActionJob) {
        def toSingleList: List[PhActionJob] = {
            val panelConf: List[PhPanelConf] = action.panelConf.get
            val calcConf: List[PhCalcConf] = action.calcConf.get
            val unitTestConf: List[PhUnitTestConf] = action.unitTestConf.get

            unitTestConf.map { unitTest =>
                val panel = panelConf.find(x => x.ym == unitTest.ym && x.mkt == unitTest.mkt).get
                val calc = calcConf.find(x => x.ym == unitTest.ym && x.mkt == unitTest.mkt).get
                val tmp = new PhActionJob()
                tmp.job_id = action.job_id
                tmp.user_id = action.user_id
                tmp.company_id = action.company_id
                tmp.panel_path = action.panel_path
                tmp.max_path = action.max_path
                tmp.prod_lst = action.prod_lst
                tmp.xmppConf = action.xmppConf
                tmp.calcYmConf = action.calcYmConf

                tmp.panelConf = Some(panel :: Nil)
                tmp.calcConf = Some(calc :: Nil)
                tmp.unitTestConf = Some(unitTest :: Nil)
                tmp
            }
        }
    }
}