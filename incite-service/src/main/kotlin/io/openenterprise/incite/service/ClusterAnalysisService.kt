package io.openenterprise.incite.service

import io.openenterprise.incite.data.domain.ClusterAnalysis
import io.openenterprise.service.AbstractMutableEntityService
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*

interface ClusterAnalysisService : AbstractMutableEntityService<ClusterAnalysis, String> {

    fun buildKMeansModel(clusterAnalysis: ClusterAnalysis): KMeansModel

    fun <T : MLWritable> getFromCache(modelId: UUID, clazz: Class<T>): T

    fun kMeansPredict(jsonOrSql: String, clusterAnalysis: ClusterAnalysis): Dataset<Row>

    fun kMeansPredict(jsonOrSql: String, kMeansModel: KMeansModel): Dataset<Row>

    fun kMeansPredict(jsonOrSql: String, modelId: UUID): Dataset<Row>

    fun putToCache(model: MLWritable): UUID
}