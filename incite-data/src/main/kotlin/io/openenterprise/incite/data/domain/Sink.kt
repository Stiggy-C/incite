package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractMutableEntity
import java.util.*
import javax.persistence.*

@Entity
@Inheritance(strategy = InheritanceType.JOINED)
@DiscriminatorColumn(name = "sub_class")
@Table(name = "sink")
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "@type"
)
@JsonSubTypes(
    value = [
        JsonSubTypes.Type(value = FileSink::class, name = "FileSink"),
        JsonSubTypes.Type(value = IgniteSink::class, name = "IgniteSink"),
        JsonSubTypes.Type(value = JdbcSink::class, name = "JdbcSink"),
        JsonSubTypes.Type(value = KafkaSink::class, name = "KafkaSink"),
        JsonSubTypes.Type(value = StreamingWrapper::class, name = "StreamingWrapper")
    ]
)
abstract class Sink : AbstractMutableEntity<String>() {

    @PrePersist
    override fun prePersist() {
        id = UUID.randomUUID().toString()

        super.prePersist()
    }
}

@Entity
@Inheritance(strategy = InheritanceType.JOINED)
abstract class NonStreamingSink : Sink() {

    @Enumerated(EnumType.STRING)
    var saveMode: SaveMode = SaveMode.APPEND

    enum class SaveMode {

        APPEND, OVERWRITE, ERROR_IF_EXISTS, IGNORE
    }
}

@MappedSuperclass
abstract class StreamingSink : Sink() {

    @Enumerated(EnumType.STRING)
    var outputMode: OutputMode = OutputMode.UPDATE

    var streamingWrite: Boolean = true
        set(value) {
            if (value) this.outputMode = OutputMode.UPDATE else OutputMode.APPEND

            field = value
        }

    @Enumerated(EnumType.STRING)
    var triggerType: TriggerType = TriggerType.ProcessingTime

    var triggerInterval: Long = 10000L

    enum class OutputMode {

        APPEND, COMPLETE, UPDATE
    }

    enum class TriggerType {

        Continuous, Once, ProcessingTime
    }
}

@Entity
@DiscriminatorValue("file_sink")
@Table(name = "file_sink")
class FileSink : StreamingSink() {

    @Enumerated(EnumType.STRING)
    var format: Format = Format.JSON

    lateinit var path: String

    enum class Format {

        CSV, JSON
    }
}

@Entity
@DiscriminatorValue("ignite_sink")
@Table(name = "ignite_sink")
class IgniteSink : JdbcSink() {

    lateinit var primaryKeyColumns: String
}

@Entity
@DiscriminatorValue("jdbc_sink")
@Table(name = "jdbc_sink")
open class JdbcSink : NonStreamingSink() {

    var createTableColumnTypes: String? = null

    var createTableOptions: String? = null

    @Convert(converter = RdbmsDatabaseJsonAttributeConverter::class)
    lateinit var rdbmsDatabase: RdbmsDatabase

    @Column(name = "table_name")
    lateinit var table: String
}

@Entity
@DiscriminatorValue("kafka_sink")
@Table(name = "kafka_sink")
class KafkaSink : StreamingSink() {

    @Convert(converter = KafkaClusterJsonAttributeConverter::class)
    lateinit var kafkaCluster: KafkaCluster

    lateinit var topic: String
}

@Entity
@DiscriminatorValue("streaming_wrapper_sink")
@Table(name = "streaming_wrapper_sink")
class StreamingWrapper() : StreamingSink() {

    constructor(nonStreamingSink: NonStreamingSink) : this() {
        this.id = nonStreamingSink.id
        this.nonStreamingSink = nonStreamingSink
    }

    @OneToOne
    @JoinColumn(name = "non_streaming_sink_id", referencedColumnName = "id")
    lateinit var nonStreamingSink: NonStreamingSink
}