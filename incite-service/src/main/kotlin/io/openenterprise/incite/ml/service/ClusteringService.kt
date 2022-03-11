package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClusteringFunction
import io.openenterprise.incite.data.domain.BisectingKMeans
import io.openenterprise.incite.data.domain.Clustering
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.service.AbstractMutableEntityService
import io.openenterprise.spark.sql.DatasetUtils
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.persistence.EntityNotFoundException
import kotlin.jvm.Throws

interface ClusteringService : AbstractService<Clustering, ClusteringFunction>,
    AbstractMutableEntityService<Clustering, String> {

    companion object : AbstractService.BaseCompanionObject() {

        /**
         * Build a model for the given [io.openenterprise.incite.data.domain.Clustering] if there is such an entity.
         *
         * @param id The [java.util.UUID] of [io.openenterprise.incite.data.domain.Clustering] as [java.lang.String]
         * @return The [java.util.UUID] of [Clustering.Model]
         * @throws EntityNotFoundException If no such [io.openenterprise.incite.data.domain.Clustering]
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
            val modelId = clusteringService.putToCache(sparkModel)
            val model = Clustering.Model()
            model.id = modelId.toString()

            clusterAnalysis.models.add(model)

            transactionTemplate.execute {
                clusteringService.update(clusterAnalysis)
            }

            return modelId
        }

        /**
         * Perform cluster analysis defined by the given [io.openenterprise.incite.data.domain.Clustering] with the
         * latest [io.openenterprise.incite.data.domain.Clustering.Model] if there is any and write the result to the
         * given sinks defined in the given [io.openenterprise.incite.data.domain.Clustering]
         *
         * @param id The [java.util.UUID] of [io.openenterprise.incite.data.domain.Clustering] as [java.lang.String]
         * @return Number of entries in the result
         * @throws EntityNotFoundException If no such [io.openenterprise.incite.data.domain.Clustering]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "clustering_predict")
        fun predict(id: String, jsonOrSql: String): Long {
            val clusteringService = getBean(ClusteringService::class.java)
            val clusterAnalysis = clusteringService.retrieve(id)
                ?: throw EntityNotFoundException("ClusterAnalysis with ID, $id, is not found")
            val result = clusteringService.predict(jsonOrSql, clusterAnalysis)

            writeToSinks(result, clusterAnalysis.sinks)

            return result.count()
        }
    }
}