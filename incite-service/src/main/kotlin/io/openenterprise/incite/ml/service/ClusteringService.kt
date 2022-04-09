package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClusteringFunction
import io.openenterprise.incite.data.domain.BisectingKMeans
import io.openenterprise.incite.data.domain.Clustering
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.persistence.EntityNotFoundException
import kotlin.jvm.Throws

interface ClusteringService : MachineLearningService<Clustering, ClusteringFunction>,
    AbstractMutableEntityService<Clustering, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        /**
         * Build a model for the given [io.openenterprise.incite.data.domain.Clustering] if there is such an entity.
         *
         * @param id The [UUID] of [Clustering] as [String]
         * @return The [UUID] of [Clustering.Model]
         * @throws EntityNotFoundException If no such [Clustering]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "build_clustering_model")
        fun buildModel(id: String): UUID {
            val clusteringService = getBean(ClusteringService::class.java)
            val transactionTemplate = getBean(TransactionTemplate::class.java)

            val clusterAnalysis = clusteringService.retrieve(id)
                ?: throw EntityNotFoundException("ClusterAnalysis with ID, $id, is not found")
            val sparkModel = when (clusterAnalysis.algorithm) {
                is BisectingKMeans -> clusteringService.buildModel<BisectingKMeansModel>(clusterAnalysis)
                is KMeans -> clusteringService.buildModel<KMeansModel>(clusterAnalysis)
                else -> throw UnsupportedOperationException()
            }

            return clusteringService.persistModel(clusterAnalysis, sparkModel)
        }

        /**
         * Perform cluster analysis defined by the given [io.openenterprise.incite.data.domain.Clustering] with the
         * latest [Clustering.Model] if there is any and write the result to the given sinks defined in the given
         * [Clustering]
         *
         * @param id The [UUID] of [Clustering] as [String]
         * @return Number of entries in the result
         * @throws EntityNotFoundException If no such [Clustering]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "clustering_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val clusteringService = getBean(ClusteringService::class.java)
            val clusterAnalysis = clusteringService.retrieve(id)
                ?: throw EntityNotFoundException("ClusterAnalysis with ID, $id, is not found")
            val result = clusteringService.predict(clusterAnalysis, jsonOrSql)

            writeToSinks(result, clusterAnalysis.sinks)

            return result.count()
        }
    }
}