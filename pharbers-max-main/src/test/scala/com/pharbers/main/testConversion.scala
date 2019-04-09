package com.pharbers.main

import akka.actor.ActorSystem
import com.pharbers.main.PhProcess.PhBuilder
import com.pharbers.reflect.PhEntity.PhActionJob
import com.pharbers.reflect.PhEntity.confEntity.{PhDataConversionConf, PhXmppConf}

/**
  * @description:
  * @author: clock
  * @date: 2019-01-23 14:45
  */
object testConversion extends App {
    val company_id = "5b028f95ed925c2c705b85ba"
    val job_id = "c372f1b8-2558-8a18-d6ef-d24ac03f431b"

    def generateAction: PhActionJob = {
        val tmpAction: PhActionJob = new PhActionJob()
        tmpAction.job_id = job_id
        tmpAction.user_id = "user_id"
        tmpAction.company_id = company_id

        val xmppConf = new PhXmppConf
        xmppConf.disableSend = true
        tmpAction.xmppConf = Some(xmppConf)

        import com.pharbers.data.util.ParquetLocation._
        val cpaConf = new PhDataConversionConf()
        cpaConf.jar_path = "/Users/clock/workSpace/Pharbers/PhDataRepository/target/pharbers-data-repository-1.0-SNAPSHOT.jar"
        cpaConf.clazz = "com.pharbers.data.job.CPA2ERDJob"
        cpaConf.conf = Map(
            "cpa_file" -> "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
            , "pha_file" -> HOSP_PHA_LOCATION
            , "hosp_base_file" -> HOSP_BASE_LOCATION
            , "prod_base_file" -> PROD_BASE_LOCATION
            , "prod_delivery_file" -> PROD_DELIVERY_LOCATION
            , "prod_dosage_file" -> PROD_DOSAGE_LOCATION
            , "prod_mole_file" -> PROD_MOLE_LOCATION
            , "prod_package_file" -> PROD_PACKAGE_LOCATION
            , "prod_corp_file" -> PROD_CORP_LOCATION
            , "save_prod_file" -> "/test/qi/qi/save_prod_file"
            , "save_hosp_file" -> "/test/qi/qi/save_hosp_file"
            , "save_pha_file" -> "/test/qi/qi/save_pha_file"
        )

        val gycConf = new PhDataConversionConf()
        gycConf.jar_path = "/Users/clock/workSpace/Pharbers/PhDataRepository/target/pharbers-data-repository-1.0-SNAPSHOT.jar"
        gycConf.clazz = "com.pharbers.data.job.GYC2ERDJob"
        gycConf.conf = Map(
            "gyc_file" -> "/test/CPA&GYCX/Astellas_201804_Gycx_20180703.csv"
            , "pha_file" -> HOSP_PHA_LOCATION
            , "hosp_base_file" -> HOSP_BASE_LOCATION
            , "prod_base_file" -> PROD_BASE_LOCATION
            , "prod_delivery_file" -> PROD_DELIVERY_LOCATION
            , "prod_dosage_file" -> PROD_DOSAGE_LOCATION
            , "prod_mole_file" -> PROD_MOLE_LOCATION
            , "prod_package_file" -> PROD_PACKAGE_LOCATION
            , "prod_corp_file" -> PROD_CORP_LOCATION
            , "save_prod_file" -> "/test/qi/qi/save_prod_file"
            , "save_hosp_file" -> "/test/qi/qi/save_hosp_file"
            , "save_pha_file" -> "/test/qi/qi/save_pha_file"
        )

        tmpAction.dataConversionConf = Some(cpaConf :: gycConf :: Nil)
        tmpAction
    }

    implicit val system: ActorSystem = ActorSystem("maxActor")
    PhBuilder(generateAction).conversionExec().stopSpark()
}
