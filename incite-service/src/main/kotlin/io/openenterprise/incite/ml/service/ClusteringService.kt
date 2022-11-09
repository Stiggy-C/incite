package io.openenterprise.incite.ml.service

import io.openenterprise.incite.data.domain.*
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.ignite.IgniteException
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import java.util.*
import javax.persistence.EntityNotFoundException

interface ClusteringService : MachineLearningService<Clustering, Clustering.Algorithm, Clustering.Model>,
    AbstractMutableEntityService<Clustering, String> {

    companion object :
        MachineLearningService.BaseCompanionObject<Clustering, Clustering.Algorithm, Clustering.Model, ClusteringService>() {

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
        override fun predict(id: String, jsonOrSql: String): Long = super.predict(id, jsonOrSql)

        /**
         * Set up a [Clustering] with the given input for performing cluster-analysis later.
         *
         * @param algo The desired clustering algorithm
         * @param algoSpecificParams Clustering algorithm parameters (in JSON format)
         * @param k The desired number of clusters
         * @param sourceSql The select query to build dataset
         * @param sinkTable The table to store the predictions
         * @param primaryKeyColumns The primary key of the table to store the predictions
         */
        @JvmStatic
        @QuerySqlFunction(alias = "set_up_clustering")
        override fun setUp(
            algo: String,
            algoSpecificParams: String,
            sourceSql: String,
            sinkTable: String,
            primaryKeyColumns: String
        ): UUID = super.setUp(algo, algoSpecificParams, sourceSql, sinkTable, primaryKeyColumns)

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
        override fun train(id: String): UUID = super.train(id)

        override fun getMachineLearningClass(): Class<Clustering> = Clustering::class.java

        override fun getMachineLearningAlgorithmClass(algo: String): Class<out MachineLearning.Algorithm> {
            return Clustering.SupportedAlgorithm.valueOf(algo).clazz
        }

        override fun getMachineLearningService(): ClusteringService =
            getBean(ClusteringService::class.java)
    }
}