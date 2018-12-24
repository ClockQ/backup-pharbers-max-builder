package com.pharbers.reflect.util

import com.pharbers.jsonapi.json.circe.CirceJsonapiSupport
import com.pharbers.jsonapi.model.RootObject
import io.circe.Json

object Jsonapi extends CirceJsonapiSupport {
    def str2Json(str: String): Json = parseJson(str)
    def json2Jsobj(json: Json): RootObject = decodeJson[RootObject](json)
}
