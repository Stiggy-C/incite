package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*

@Entity
class Classification: Aggregate() {

    @Convert(converter = Clustering.AlgorithmJsonAttributeConverter::class)
    lateinit var algorithm: Algorithm

    @OneToMany
    @OrderBy("createdDateTime DESC")
    var models: SortedSet<Model> = TreeSet()

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type"
    )
    @JsonSubTypes(
        value = [
            JsonSubTypes.Type(value = LogisticRegression::class, name = "LogisticRegression")
        ]
    )
    abstract class Algorithm {

        lateinit var featureColumns: Set<String>

        lateinit var labelColumn: String
    }

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Algorithm>()

    @Entity
    @Table(name = "classification_model")
    class Model : AbstractEntity<String>(), Comparable<Model> {

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed().compare(this, other)
        }
    }
}

class LogisticRegression: Classification.Algorithm() {

    var elasticNetMixing: Double = 0.8

    var family: Family? = null

    var maxIteration: Int = 1

    var regularization: Double = 0.3

    enum class Family {

        Binomial, Multinomial
    }
}