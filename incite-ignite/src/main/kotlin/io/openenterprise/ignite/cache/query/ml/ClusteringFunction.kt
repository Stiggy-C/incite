package io.openenterprise.ignite.cache.query.ml

import org.apache.commons.lang3.StringUtils
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.shared.HasFeaturesCol
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import java.util.*
import javax.inject.Named

@Named
open class ClusteringFunction : AbstractFunction() {

    companion object : BaseCompanionObject() {

        /**
         * Build a [org.apache.spark.ml.clustering.BisectingKMeansModel] from given input.
         *
         * @return The [java.util.UUID] of the model
         */
        @JvmStatic
        @QuerySqlFunction(alias = "build_bisecting_k_means_model")
        fun buildBisectingKMeansModel(
            sql: String,
            featuresColumns: String,
            k: Int,
            maxIteration: Int,
            seed: Long
        ): UUID {
            val clusteringFunction = getBean(ClusteringFunction::class.java)
            val dataset = clusteringFunction.loadDatasetFromSql(sql)
            val bisectingKMeansModel =
                clusteringFunction.buildBisectingKMeansModel(dataset, featuresColumns, k, maxIteration, seed)

            return clusteringFunction.putToCache(bisectingKMeansModel)
        }

        /**
         * Build a [org.apache.spark.ml.clustering.KMeansModel] from given input.
         *
         * @return The [java.util.UUID] of the model
         */
        @JvmStatic
        @QuerySqlFunction(alias = "build_k_means_model")
        fun buildKMeansModel(sql: String, featuresColumns: String, k: Int, maxIteration: Int, seed: Long): UUID {
            val clusteringFunction = getBean(ClusteringFunction::class.java)
            val dataset = clusteringFunction.loadDatasetFromSql(sql)
            val kMeansModel = clusteringFunction.buildKMeansModel(dataset, featuresColumns, k, maxIteration, seed)

            return clusteringFunction.putToCache(kMeansModel)
        }

        /**
         * Perform bisecting k-means predict with given model and given json or SQL query.
         *
         * @return Number of entries in the resulting dataset
         */
        @JvmStatic
        @QuerySqlFunction(alias = "bisecting_k_means_predict")
        fun bisectingKMeansPredict(modelId: String, jsonOrSql: String, table: String, primaryKeyColumn: String): Long {
            val clusteringFunction = getBean(ClusteringFunction::class.java)
            val bisectingKMeansModel: BisectingKMeansModel =
                clusteringFunction.getFromCache(UUID.fromString(modelId))
            val result = clusteringFunction.predict(bisectingKMeansModel, jsonOrSql)

            writeToTable(result, table, primaryKeyColumn, SaveMode.Append)

            return result.count()
        }

        /**
         * Perform k-means predict with given model and given json or SQL query.
         *
         * @return Result in JSON format
         */
        @JvmStatic
        @QuerySqlFunction(alias = "k_means_predict")
        fun kMeansPredict(modelId: String, jsonOrSql: String, table: String, primaryKeyColumn: String): Long {
            val clusteringFunction = getBean(ClusteringFunction::class.java)
            val kMeansModel: KMeansModel = clusteringFunction.getFromCache(UUID.fromString(modelId))
            val dataset = clusteringFunction.predict(kMeansModel, jsonOrSql)

            writeToTable(dataset, table, primaryKeyColumn, SaveMode.Append)

            return dataset.count()
        }
    }

    fun buildBisectingKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): BisectingKMeansModel {
        val bisectingKMeans = BisectingKMeans()
        bisectingKMeans.k = k
        bisectingKMeans.maxIter = maxIteration
        bisectingKMeans.seed = seed

        return buildModel(bisectingKMeans, dataset, StringUtils.split(featuresColumns, ","))
    }

    fun buildKMeansModel(
        dataset: Dataset<Row>,
        featuresColumns: String,
        k: Int,
        maxIteration: Int,
        seed: Long
    ): KMeansModel {
        val kMeans = KMeans()
        kMeans.k = k
        kMeans.maxIter = maxIteration
        kMeans.seed = seed

        return buildModel(kMeans, dataset, StringUtils.split(featuresColumns, ","))
    }

    private fun <A : Estimator<M>, M : Model<M>> buildModel(
        algorithm: A,
        dataset: Dataset<Row>,
        featuresColumns: Array<String>
    ): M {
        assert(algorithm is HasFeaturesCol)

        @Suppress("unchecked_cast")
        val transformedDataset =
            VectorAssembler().setInputCols(featuresColumns).setOutputCol(((algorithm as HasFeaturesCol).featuresCol))
                .transform(dataset)

        return algorithm.fit(transformedDataset)
    }
}