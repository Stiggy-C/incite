package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import io.openenterprise.data.domain.AbstractMutableEntity
import org.apache.spark.sql.SaveMode
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*
import kotlin.collections.ArrayList

@Entity
@Inheritance(strategy=InheritanceType.TABLE_PER_CLASS)
open class Aggregate : AbstractMutableEntity<String>() {

    var description: String? = null

    @Convert(converter = JoinsJsonAttributeConverter::class)
    var joins: MutableList<Join> = ArrayList()

    var fixedDelay: Long = 0

    var lastRunDateTime: OffsetDateTime? = null

    @Convert(converter = SinksJsonAttributeConverter::class)
    var sinks: MutableList<Sink>  = ArrayList()

    @Convert(converter = SourcesJsonAttributeConverter::class)
    var sources: MutableList<Source> = ArrayList()

    @PrePersist
    override fun prePersist() {
        id = UUID.randomUUID().toString()

        super.prePersist()
    }
}

@Converter
class JoinsJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Join>>()

@Converter
class SinksJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Sink>>()

@Converter
class SourcesJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Source>>()