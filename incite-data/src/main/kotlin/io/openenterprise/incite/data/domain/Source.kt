package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    value = [
        JsonSubTypes.Type(value = JdbcSource::class, name = "JdbcSource"),
        JsonSubTypes.Type(value = KafkaSource::class, name = "KafkaSource")
    ]
)
abstract class Source: Cloneable {

    var watermark: Watermark? = null

    public override fun clone(): Any {
        return super.clone()
    }

    class Watermark() {

        constructor(eventTimeColumn: String, delayThreshold: String): this() {
            this.delayThreshold = delayThreshold
            this.eventTimeColumn = eventTimeColumn
        }

        lateinit var eventTimeColumn: String

        lateinit var delayThreshold: String
    }
}

class JdbcSource: Source() {

    lateinit var rdbmsDatabase: RdbmsDatabase

    lateinit var query: String
}

class KafkaSource: StreamingSource() {

    var fields: MutableSet<Field>? = null

    lateinit var kafkaCluster: KafkaCluster

    var startingOffset: String = if (streamingRead) "latest" else "earliest"

    lateinit var topic: String
}

abstract class StreamingSource: Source() {

    var streamingRead: Boolean = true
}