package io.openenterprise.ws.rs.ext

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Autowired
import java.io.InputStream
import java.lang.reflect.Type
import javax.inject.Singleton
import javax.json.Json
import javax.json.JsonMergePatch
import javax.json.JsonObject
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.ext.MessageBodyReader

@Singleton
class JsonMergePatchMessageBodyReader : MessageBodyReader<JsonMergePatch> {

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    override fun isReadable(
        type: Class<*>?,
        genericType: Type?,
        annotations: Array<out Annotation>?,
        mediaType: MediaType?
    ): Boolean = type == JsonMergePatch::class.java

    override fun readFrom(
        type: Class<JsonMergePatch>?,
        genericType: Type?,
        annotations: Array<out Annotation>?,
        mediaType: MediaType?,
        httpHeaders: MultivaluedMap<String, String>?,
        entityInputStream: InputStream?
    ): JsonMergePatch {
        val jsonObject = objectMapper.readValue(entityInputStream, JsonObject::class.java)

        return Json.createMergePatch(jsonObject)
    }
}