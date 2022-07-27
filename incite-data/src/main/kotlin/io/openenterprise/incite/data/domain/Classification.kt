package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*

@Entity
class Classification: MachineLearning<Classification.Model>() {

    @Convert(converter = Clustering.AlgorithmJsonAttributeConverter::class)
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
            JsonSubTypes.Type(value = LogisticRegression::class, name = "LogisticRegression")
        ]
    )
    abstract class Algorithm {

        var featureColumns: Set<String> = mutableSetOf()

        var labelColumn: String = "label"

    }

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Algorithm>()

    @Entity
    @Table(name = "classification_model")
    class Model : AbstractEntity<String>(), Comparable<Model> {

        var accuracy: Double? = 0.0

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed().compare(this, other)
        }
    }

    enum class SupportedAlgorithm(val clazz: Class<*>) {

        LOGISTIC_REGRESSION(LogisticRegression::class.java)
    }
}

class LogisticRegression: Classification.Algorithm() {

    var elasticNetMixing: Double = 0.8

    var maxIterations: Int = 1

    var regularization: Double = 0.3
}