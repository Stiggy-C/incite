package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.SaveMode
import java.util.*

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "@type"
)
@JsonSubTypes(
    value = [
        JsonSubTypes.Type(value = EmbeddedIgniteSink::class, name = "EmbeddedIgniteSink"),
        JsonSubTypes.Type(value = IgniteSink::class, name = "IgniteSink"),
        JsonSubTypes.Type(value = JdbcSink::class, name = "JdbcSink"),
        JsonSubTypes.Type(value = KafkaSink::class, name = "KafkaSink")
    ]
)
abstract class Sink {

    lateinit var id: UUID
}

abstract class NonStreamingSink : Sink() {

    var saveMode: SaveMode = SaveMode.Append
}

abstract class StreamingSink : Sink() {

    var outputMode: OutputMode = OutputMode.Append

    var streamingWrite: Boolean = true

    var triggerType: TriggerType = TriggerType.PROCESSING_TIME

    var triggerInterval: Long = 1000L

    enum class OutputMode {

        Append, Complete, Update
    }

    enum class TriggerType {

        CONTINUOUS, ONCE, PROCESSING_TIME
    }
}

class EmbeddedIgniteSink : IgniteSink()

open class IgniteSink : NonStreamingSink() {

    lateinit var primaryKeyColumns: String

    lateinit var table: String

    var tableParameters: String? = null
}

class JdbcSink : NonStreamingSink() {

    var createTableColumnTypes: String? = null

    var createTableOptions: String? = null

    lateinit var rdbmsDatabase: RdbmsDatabase

    lateinit var table: String
}

class KafkaSink : StreamingSink() {

    lateinit var kafkaCluster: KafkaCluster

    lateinit var topic: String
}

class StreamingWrapper() : StreamingSink() {

    constructor(nonStreamingSink: NonStreamingSink): this() {
        this.id = nonStreamingSink.id
        this.nonStreamingSink = nonStreamingSink
    }

    lateinit var nonStreamingSink: NonStreamingSink
}