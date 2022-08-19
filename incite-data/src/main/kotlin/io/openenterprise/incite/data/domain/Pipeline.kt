package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import io.openenterprise.data.domain.AbstractMutableEntity
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*
import kotlin.collections.ArrayList

@Entity
@Inheritance(strategy=InheritanceType.TABLE_PER_CLASS)
open class Pipeline : AbstractMutableEntity<String>() {

    open var description: String? = null

    @Convert(converter = JoinsJsonAttributeConverter::class)
    open var joins: MutableList<Join> = ArrayList()

    open var fixedDelay: Long = 0

    open var lastRunDateTime: OffsetDateTime? = null

    // @Convert(converter = SinksJsonAttributeConverter::class)
    @ManyToMany
    @JoinTable(
        name = "pipelines_sinks",
        joinColumns = [JoinColumn(name = "pipeline_id")],
        inverseJoinColumns = [JoinColumn(name = "sink_id")]
    )
    open var sinks: MutableList<Sink>  = ArrayList()

    // @Convert(converter = SourcesJsonAttributeConverter::class)
    @ManyToMany
    @JoinTable(
        name = "pipelines_sources",
        joinColumns = [JoinColumn(name = "pipeline_id")],
        inverseJoinColumns = [JoinColumn(name = "source_id")]
    )
    open var sources: MutableList<Source> = ArrayList()

    @PrePersist
    override fun prePersist() {
        id = UUID.randomUUID().toString()

        super.prePersist()
    }
}

@MappedSuperclass
abstract class MachineLearning<A: MachineLearning.Algorithm, M>: Pipeline() {

    abstract var algorithm: A

    abstract var models: SortedSet<M>

    abstract class Algorithm
}

@Converter
class JoinsJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Join>>()

@Converter
class SinksJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Sink>>()

@Converter
class SourcesJsonAttributeConverter: AbstractJsonAttributeConverter<MutableList<Source>>()