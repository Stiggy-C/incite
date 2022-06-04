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

    var outputMode: OutputMode = OutputMode.Update

    var streamingWrite: Boolean = true
        set(value) {
            if (value) this.outputMode = OutputMode.Update else OutputMode.Append

            field = value
        }

    var triggerType: TriggerType = TriggerType.ProcessingTime

    var triggerInterval: Long = 1000L

    enum class OutputMode {

        Append, Complete, Update
    }

    enum class TriggerType {

        Continuous, Once, ProcessingTime
    }
}

class FileSink : StreamingSink() {

    var format: Format = Format.Json

    lateinit var path: String

    enum class Format {

        Csv, Json
    }
}

class IgniteSink : JdbcSink() {

    lateinit var primaryKeyColumns: String
}

open class JdbcSink : NonStreamingSink() {

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

    constructor(nonStreamingSink: NonStreamingSink) : this() {
        this.id = nonStreamingSink.id
        this.nonStreamingSink = nonStreamingSink
    }

    lateinit var nonStreamingSink: NonStreamingSink
}