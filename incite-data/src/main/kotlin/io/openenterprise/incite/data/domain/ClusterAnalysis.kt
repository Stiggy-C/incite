package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.time.OffsetDateTime
import java.util.*
import javax.persistence.*
import kotlin.Comparator

@Entity
class ClusterAnalysis : Aggregate() {

    @Convert(converter = AlgorithmJsonAttributeConverter::class)
    lateinit var algorithm: Algorithm

    @OneToMany
    @OrderBy("createdDateTime DESC")
    var models: SortedSet<Model> = TreeSet()

    @Transient
    var latestSilhouette: Double? = null

    override fun postLoad() {
        super.postLoad()

        if (models.size > 0) {
            latestSilhouette =
                models.stream().filter { it.lastSilhouette != null }.findFirst().map { it.lastSilhouette }.orElse(null)
        }
    }

    abstract class Algorithm {

        var k: Int = 0

        var maxIteration: Int = 1
    }

    abstract class FeatureColumnsBasedAlgorithm : Algorithm() {

        lateinit var featureColumns: Set<String>

        var seed: Long = 1L
    }

    @Entity
    @Table(name = "cluster_analysis_model")
    class Model : AbstractEntity<String>(), Comparable<Model> {

        var lastSilhouette: Double? = null

        override fun compareTo(other: Model): Int {
            return Comparator.comparing<Model?, OffsetDateTime?> {
                if (it.createdDateTime == null) OffsetDateTime.MIN else it.createdDateTime
            }.reversed().compare(this, other)
        }
    }

    @Converter
    class AlgorithmJsonAttributeConverter : AbstractJsonAttributeConverter<Algorithm>()
}

class KMeans : ClusterAnalysis.FeatureColumnsBasedAlgorithm()