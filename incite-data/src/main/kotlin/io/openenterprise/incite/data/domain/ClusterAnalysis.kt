package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.domain.AbstractJsonAttributeConverter
import java.util.*
import javax.persistence.*

@Entity
class ClusterAnalysis: Aggregate() {

    @Convert(converter = AlgorithmJsonAttributeConverter::class)
    lateinit var algorithm: Algorithm

    @OneToMany
    lateinit var models: SortedSet<Model>

    abstract class Algorithm {

        var k: Int = 0

        var maxIteration: Int = 1
    }

    abstract class FeatureColumnsBasedAlgorithm: Algorithm() {

        lateinit var featureColumns: Set<String>

        var seed: Long = 1L
    }

    @Entity
    @Table(name = "cluster_analysis_model")
    class Model: AbstractEntity<String>()

    @Converter
    class AlgorithmJsonAttributeConverter: AbstractJsonAttributeConverter<Algorithm>()
}

class KMeans: ClusterAnalysis.FeatureColumnsBasedAlgorithm()