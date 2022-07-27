package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import io.openenterprise.data.domain.AbstractMutableEntity
import java.util.*
import javax.persistence.*

@Entity
@Inheritance(strategy = InheritanceType.JOINED)
@DiscriminatorColumn(name = "sub_class")
@Table(name = "source")
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "@type"
)
@JsonSubTypes(
    value = [
        JsonSubTypes.Type(value = FileSource::class, name = "FileSource"),
        JsonSubTypes.Type(value = JdbcSource::class, name = "JdbcSource"),
        JsonSubTypes.Type(value = KafkaSource::class, name = "KafkaSource")
    ]
)
abstract class Source : Cloneable, AbstractMutableEntity<String>() {

    @Convert(converter = FieldsJsonAttributeConverter::class)
    var fields: MutableSet<Field>? = null

    @Convert(converter = WatermarkJsonAttributeConverter::class)
    var watermark: Watermark? = null

    public override fun clone(): Any {
        return super.clone()
    }

    @PrePersist
    override fun prePersist() {
        id = UUID.randomUUID().toString()

        super.prePersist()
    }

    class Watermark() {

        constructor(eventTimeColumn: String, delayThreshold: String) : this() {
            this.delayThreshold = delayThreshold
            this.eventTimeColumn = eventTimeColumn
        }

        lateinit var eventTimeColumn: String

        lateinit var delayThreshold: String
    }

    @Converter
    class WatermarkJsonAttributeConverter: AbstractJsonAttributeConverter<Watermark>()
}

@MappedSuperclass
abstract class StreamingSource : Source() {

    var streamingRead: Boolean = true
}

@Entity
@DiscriminatorValue("file_source")
@Table(name = "file_source")
open class FileSource : StreamingSource() {

    lateinit var path: String

    @Enumerated(EnumType.STRING)
    var format: Format = Format.JSON

    var maxFilesPerTrigger: Short = 1

    var latestFirst: Boolean = false

    var maxFileAge: String = "7d"

    @Enumerated(EnumType.STRING)
    var cleanSource: CleanSourceOption = CleanSourceOption.OFF

    var sourceArchiveDirectory: String? = null

    enum class CleanSourceOption {

        ARCHIVE, DELETE, OFF
    }

    enum class Format {

        JSON
    }
}

@Entity
@DiscriminatorValue("jdbc_source")
@Table(name = "jdbc_source")
open class JdbcSource : Source() {

    @Convert(converter = RdbmsDatabaseJsonAttributeConverter::class)
    lateinit var rdbmsDatabase: RdbmsDatabase

    lateinit var query: String
}

@Entity
@DiscriminatorValue("kafka_source")
@Table(name = "kafka_source")
open class KafkaSource : StreamingSource() {

    @Convert(converter = KafkaClusterJsonAttributeConverter::class)
    lateinit var kafkaCluster: KafkaCluster

    @Enumerated(EnumType.STRING)
    var startingOffset: Offset = if (streamingRead) Offset.Latest else Offset.Earliest

    lateinit var topic: String

    enum class Offset {

        Earliest, Latest
    }
}