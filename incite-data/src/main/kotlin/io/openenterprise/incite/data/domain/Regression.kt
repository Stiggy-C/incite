package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*

@Entity
class Regression : MachineLearning<Regression.Algorithm, Regression.Model>(){

    @Convert(converter = AlgorithmJsonAttributeConverter::class)
    override lateinit var algorithm: Algorithm

    @OneToMany
    @OrderBy("createdDateTime DESC")
    override var models: SortedSet<Model> = TreeSet()

    @Transient
    override fun newModelInstance(): Model {
        return Model()
    }

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type"
    )
    @JsonSubTypes(
        value = [
            JsonSubTypes.Type(value = LinearRegression::class, name = "LinearRegression")
        ]
    )
    abstract class Algorithm: MachineLearning.Algorithm()

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Algorithm>()

    @Entity
    @Table(name = "regression_model")
    class Model : MachineLearning.Model<Model>() {

        var rootMeanSquaredError: Double? = null

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed()
                .compare(this, other)
        }
    }

    enum class SupportedAlgorithm(val clazz: Class<out MachineLearning.Algorithm>) {

        LINEAR_REGRESSION(LinearRegression::class.java)
    }
}

class LinearRegression : Regression.Algorithm() {

    var epsilon: Double = 1.35

    var maxIterations: Int = 1

}