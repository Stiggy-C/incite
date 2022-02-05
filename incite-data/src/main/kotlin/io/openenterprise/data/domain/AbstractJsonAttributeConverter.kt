package io.openenterprise.data.domain

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.springframework.context.ApplicationContextUtils
import javax.persistence.AttributeConverter

abstract class AbstractJsonAttributeConverter<T> : AttributeConverter<T, String> {

    override fun convertToDatabaseColumn(obj: T): String = getObjectMapper().writeValueAsString(obj)

    override fun convertToEntityAttribute(jsonString: String?): T =
        getObjectMapper().readValue(jsonString, object : TypeReference<T>() {})

    protected fun getObjectMapper(): ObjectMapper =
        ApplicationContextUtils.getApplicationContext()!!.getBean(ObjectMapper::class.java)

}