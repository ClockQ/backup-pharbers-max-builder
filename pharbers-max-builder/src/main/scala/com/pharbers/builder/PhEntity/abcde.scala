package com.pharbers.builder.PhEntity

import java.util.UUID

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.One2OneConn

//@One2OneConn[PhXmppConf]("xmppConf2")
class abcde extends commonEntity {
    type ActionInitConf = Map[String, String]

    val job_id: String = "job_id"
    val panel_name: String = UUID.randomUUID().toString
    println(s"panel_name = $panel_name")
    val max_name: String = UUID.randomUUID().toString
    println(s"max_name = $max_name")
    val max_search_name: String = UUID.randomUUID().toString
    println(s"max_search_name = $max_search_name")

    val xmppConf = new PhXmppConf()

    val calcYmConf: ActionInitConf = Map(
        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_CPA_.csv",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "job_id" -> "job_id"
    )

    val panelConf: ActionInitConf = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "ym" -> "201804",
        "mkt" -> "麻醉市场",
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "p_current" -> "1",
        "p_total" -> "1",
        "job_id" -> job_id,
        "prod_lst" -> "恩华",
        "cpa_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_CPA_.csv",
        "not_arrival_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_201804_not_arrival_hosp.csv",
        "not_published_hosp_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_NotPublishedHosp_20180629.csv",
        "product_match_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_ProductMatchTable_20180629.csv",
        "fill_hos_data_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_FullHosp_20180629.csv",
        "markets_match_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_MarketMatchTable_20180629.csv",
        "hosp_ID_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_If_panel_all_麻醉市场_20180629.csv"
    )

    val calcConf: Map[String, String] = Map(
        "panel_path" -> "hdfs:///workData/Panel/",
        "panel_name" -> panel_name,
        "max_path" -> "hdfs:///workData/Panel/",
        "max_name" -> max_name,
        "max_search_name" -> max_search_name,
        "ym" -> "201804",
        "mkt" -> "麻醉市场",
        "job_id" -> job_id,
        "user_id" -> "user_id",
        "company_id" -> "company_id",
        "prod_lst" -> "恩华",
        "p_current" -> "1",
        "p_total" -> "1",
        "universe_file" -> "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_universe_麻醉市场_20180705.csv"
    )
}
