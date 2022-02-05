package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import javax.persistence.Converter

class RdbmsDatabase {

    lateinit var driverClass: String

    lateinit var url: String

    lateinit var username: String

    lateinit var password: String
}

@Converter
class RdbmsDatabaseJsonAttributeConverter: AbstractJsonAttributeConverter<RdbmsDatabase>()