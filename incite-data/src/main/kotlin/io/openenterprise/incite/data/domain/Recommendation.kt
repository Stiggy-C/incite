package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*

@Entity
class Recommendation: MachineLearning<Recommendation.Model>() {

    @Convert(converter = AlgorithmJsonAttributeConverter::class)
    lateinit var algorithm: Algorithm

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
            JsonSubTypes.Type(value = AlternatingLeastSquares::class, name = "ALS")
        ]
    )
    abstract class Algorithm

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Classification.Algorithm>()

    @Entity
    @Table(name = "collaborative_filtering_model")
    class Model : AbstractEntity<String>(), Comparable<Model> {

        var rootMeanSquaredError: Double? = null

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed().compare(this, other)
        }
    }
}

class AlternatingLeastSquares: Recommendation.Algorithm() {

    var implicitPreference: Boolean = false

    var maxIteration: Int = 10

    var numberOfItemBlocks: Int = 10

    var numberOfUserBlocks: Int = 10

    var regularization: Double = 1.0
}