package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*
import kotlin.Comparator

@Entity
class Clustering : MachineLearning<Clustering.Algorithm, Clustering.Model>() {

    @Convert(converter = AlgorithmJsonAttributeConverter::class)
    override lateinit var algorithm: Algorithm

    @OneToMany
    @OrderBy("createdDateTime DESC")
    override var models: SortedSet<Model> = TreeSet()

    @Transient
    var latestSilhouette: Double? = null

    override fun postLoad() {
        super.postLoad()

        latestSilhouette =
            models.stream().filter { it.silhouette != null }.findFirst().map { it.silhouette }.orElse(null)
    }

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type"
    )
    @JsonSubTypes(
        value = [
            JsonSubTypes.Type(value = BisectingKMeans::class, name = "BisectingKMeans"),
            JsonSubTypes.Type(value = KMeans::class, name = "KMeans")
        ]
    )
    abstract class Algorithm: MachineLearning.Algorithm() {

        var k: Int = 0

    }

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Algorithm>()

    abstract class FeatureColumnsBasedAlgorithm : Algorithm() {

        var featureColumns: Set<String> = mutableSetOf()

        var maxIterations: Int = 1

        var seed: Long = 1L
    }

    @Entity
    @Table(name = "clustering_model")
    class Model : AbstractEntity<String>(), Comparable<Model> {

        var silhouette: Double? = null

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed().compare(this, other)
        }
    }

    enum class SupportedAlgorithm(val clazz: Class<*>) {

        BISECTING_K_MEANS(BisectingKMeans::class.java),
        K_MEANS(KMeans::class.java)
    }
}

class BisectingKMeans : Clustering.FeatureColumnsBasedAlgorithm()

class KMeans : Clustering.FeatureColumnsBasedAlgorithm()