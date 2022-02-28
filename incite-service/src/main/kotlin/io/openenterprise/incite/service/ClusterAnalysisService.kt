package io.openenterprise.incite.service

import io.openenterprise.incite.data.domain.ClusterAnalysis
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.service.AbstractMutableEntityService
import io.openenterprise.spark.sql.DatasetUtils
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.ignite.cache.query.annotations.QuerySqlFunction
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.transaction.support.TransactionTemplate
import java.util.*
import javax.persistence.EntityNotFoundException
import kotlin.jvm.Throws

interface ClusterAnalysisService : AbstractMutableEntityService<ClusterAnalysis, String> {

    companion object {

        /**
         * Build model for a specific [io.openenterprise.incite.data.domain.ClusterAnalysis] if there is such entity.
         *
         * @param id The [java.util.UUID] of [io.openenterprise.incite.data.domain.ClusterAnalysis] as [java.lang.String]
         * @throws EntityNotFoundException If no such [io.openenterprise.incite.data.domain.ClusterAnalysis]
         */
        @JvmStatic
        @Throws(EntityNotFoundException::class)
        @QuerySqlFunction(alias = "build_cluster_analysis_model")
        fun buildModel(id: String): UUID {
            val clusterAnalysisService = getBean(ClusterAnalysisService::class.java)
            val transactionTemplate = getBean(TransactionTemplate::class.java)

            val clusterAnalysis = clusterAnalysisService.retrieve(id)
                ?: throw EntityNotFoundException("ClusterAnalysis with ID, $id, is not found")
            val kMeansModel = clusterAnalysisService.buildModel<KMeansModel>(clusterAnalysis)
            val modelId = clusterAnalysisService.putToCache(kMeansModel)
            val model = ClusterAnalysis.Model()
            model.id = modelId.toString()

            clusterAnalysis.models.add(model)

            transactionTemplate.execute {
                clusterAnalysisService.update(clusterAnalysis)
            }

            return modelId
        }

        /**
         * Perform the given [io.openenterprise.incite.data.domain.ClusterAnalysis] with the latest
         * [io.openenterprise.incite.data.domain.ClusterAnalysis.Model] if there is any.
         *
         * @param id The [java.util.UUID] of [io.openenterprise.incite.data.domain.ClusterAnalysis] as [java.lang.String]
         * @throws EntityNotFoundException If no such [io.openenterprise.incite.data.domain.ClusterAnalysis]
         */
        @JvmStatic
        @QuerySqlFunction(alias = "cluster_analysis_predict")
        fun clusterAnalysisPredict(id: String, jsonOrSql: String): String {
            val clusterAnalysisService = getBean(ClusterAnalysisService::class.java)
            val clusterAnalysis = clusterAnalysisService.retrieve(id)
                ?: throw EntityNotFoundException("ClusterAnalysis with ID, $id, is not found")
            val dataset = clusterAnalysisService.predict(jsonOrSql, clusterAnalysis)

            return DatasetUtils.toJson(dataset)
        }

        protected fun <T> getBean(clazz: Class<T>): T =
            ApplicationContextUtils.getApplicationContext()!!.getBean(clazz)
    }

    fun <M: Model<M>> buildModel(clusterAnalysis: ClusterAnalysis): M

    fun <M: Model<M>> getFromCache(modelId: UUID,): M

    fun predict(jsonOrSql: String, clusterAnalysis: ClusterAnalysis): Dataset<Row>

    fun putToCache(model: MLWritable): UUID
}