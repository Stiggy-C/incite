package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import javax.persistence.Converter

class Field() {

    constructor(name:String): this() {
        this.name = name
    }

    constructor(name: String, function: String): this() {
        this.function = function
        this.name = name
    }

    var function: String? = null

    lateinit var name: String
}

@Converter
class FieldsJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Field>>()