package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*

@Entity
class FrequentPatternMining: MachineLearning<FrequentPatternMining.Algorithm, FrequentPatternMining.Model>() {

    @Convert(converter = AlgorithmJsonAttributeConverter::class)
    override lateinit var algorithm: Algorithm

    @OneToMany
    @OrderBy("createdDateTime DESC")
    override var models: SortedSet<Model> = TreeSet()

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type"
    )
    @JsonSubTypes(
        value = [
            JsonSubTypes.Type(value = FPGrowth::class, name = "FPGrowth")
        ]
    )
    abstract class Algorithm: MachineLearning.Algorithm()

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Algorithm>()

    @Entity
    @Table(name = "frequent_pattern_mining_model")
    class Model: AbstractEntity<String>(), Comparable<Model> {

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed()
                .compare(this, other)
        }
    }

    enum class SupportedAlgorithm(val clazz: Class<*>) {

        FP_GROWTH(FPGrowth::class.java)

    }
}

class FPGrowth : FrequentPatternMining.Algorithm() {

    companion object {

        @JvmStatic
        val ITEMS_COLUMN_DEFAULT : String = "items"

    }

    val itemsColumn = ITEMS_COLUMN_DEFAULT

    val minConfidence: Double = 0.8

    val minSupport: Double = 0.3
}