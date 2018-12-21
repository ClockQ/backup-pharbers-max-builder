package com.pharbers.main.PhConsumer

import com.pharbers.channel.detail.channelEntity
import com.pharbers.macros.api.commonEntity

class PhMaxJob extends commonEntity with channelEntity {
    var user_id : String = ""
    var company_id : String = ""
    var date : String = ""
    var call: String = ""
    var job_id: String = ""
    var message: String = ""
    var percentage: Int = 0
    var cpa: String = ""
    var gycx: String = ""
    var not_arrival_hosp_file: String = ""
    var yms = ""
}

