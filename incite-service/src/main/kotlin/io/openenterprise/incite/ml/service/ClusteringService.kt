package io.openenterprise.incite.ml.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.incite.data.domain.*
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue
import javax.persistence.EntityNotFoundException

interface ClusteringService : MachineLearningService<Clustering>,
    AbstractMutableEntityService<Clustering, String> {

    companion object : MachineLearningService.BaseCompanionObject() {

        /**
         * Make predictions for the given [Clustering] with the latest [Clustering.Model] if there is any and write the
         * result to the given sinks defined in the given [Clustering]
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

        /**
         * Set up a [Clustering] with the given input for performing cluster-analysis later.
         *
         * @param algo The desired clustering algorithm
         * @param algoSpecificParams Clustering algorithm parameters (in JSON format)
         * @param k The desired number of clusters
         * @param sourceSql The select query to build dataset
         * @param sinkTable The table to store the predictions
         * @param primaryKeyColumn The primary key of the table to store the predictions
         */
        @JvmStatic
        @QuerySqlFunction(alias = "set_up_clustering")
        fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID {
            val algorithm: Clustering.Algorithm = mergeParamsIntoAlgorithm(
                Clustering.SupportedAlgorithm.valueOf(algo).clazz.getDeclaredConstructor()
                    .newInstance() as Clustering.Algorithm, algoSpecificParams
            )
            val clustering = setUpMachineLearning(
                ClusteringService::class.java,
                Clustering(),
                algorithm,
                sourceSql,
                sinkTable,
                primaryKeyColumns
            )

            getBean(ClusteringService::class.java).create(clustering)

            return UUID.fromString(clustering.id)
        }

        /**
         * Train a model for the given [Clustering] if it exists.
         *
         * @param id The [UUID] of [Clustering] as [String]
         * @return The [UUID] of [Clustering.Model]
         * @throws EntityNotFoundException If no such [Clustering]
         */
        @JvmStatic
        @Throws(IgniteException::class)
        @QuerySqlFunction(alias = "train_clustering_model")
        fun train(id: String): UUID {
            val clusteringService = getBean(ClusteringService::class.java)
            val clusterAnalysis = clusteringService.retrieve(id)
                ?: throw IgniteException(EntityNotFoundException("ClusterAnalysis (ID: $id) is not found"))
            val sparkModel = when (clusterAnalysis.algorithm) {
                is BisectingKMeans -> clusteringService.train<BisectingKMeansModel>(clusterAnalysis)
                is KMeans -> clusteringService.train<KMeansModel>(clusterAnalysis)
                else -> throw IgniteException(UnsupportedOperationException())
            }

            return clusteringService.persistModel(clusterAnalysis, sparkModel)
        }
    }
}