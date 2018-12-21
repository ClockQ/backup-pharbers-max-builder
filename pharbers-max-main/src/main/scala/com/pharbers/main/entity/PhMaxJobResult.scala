package com.pharbers.main.entity

import com.pharbers.macros.api.commonEntity
import com.pharbers.channel.detail.channelEntity

class PhMaxJobResult extends commonEntity with channelEntity {
    var user_id : String = ""
    var company_id : String = ""
    var job_id : String = ""
    var call : String = ""
    /**
      * message stand for result hdfs path
      * percentage [0.0, 1.0]
      */
    var message : String = ""
    var percentage : Double = 0.0F
}