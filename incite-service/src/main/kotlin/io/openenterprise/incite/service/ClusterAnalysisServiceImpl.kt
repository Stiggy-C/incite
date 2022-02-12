package io.openenterprise.incite.service

import io.openenterprise.ignite.cache.query.ml.ClusterAnalysisFunction
import io.openenterprise.incite.data.domain.ClusterAnalysis
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named

@Named
class ClusterAnalysisServiceImpl(
    @Inject private val aggregateService: AggregateService,
    @Inject private val clusterAnalysisFunction: ClusterAnalysisFunction
) :
    ClusterAnalysisService,
    AbstractAbstractMutableEntityServiceImpl<ClusterAnalysis, String>() {

    override fun buildKMeansModel(clusterAnalysis: ClusterAnalysis): KMeansModel {
        assert(aggregateService is AggregateServiceImpl)
        assert(clusterAnalysis.algorithm is KMeans)

        val aggregateServiceImpl = aggregateService as AggregateServiceImpl
        val datasets = aggregateServiceImpl.loadSources(clusterAnalysis.sources, Collections.emptyMap<String, Any>())
        val aggregatedDataset = aggregateServiceImpl.joinSources(datasets, clusterAnalysis.joins)
        val kMeans = clusterAnalysis.algorithm as KMeans

        return clusterAnalysisFunction.buildKMeansModel(
            aggregatedDataset,
            kMeans.featureColumns.stream().collect(Collectors.joining(",")),
            kMeans.k,
            kMeans.maxIteration,
            kMeans.seed
        )
    }

    override fun <T : MLWritable> getFromCache(modelId: UUID, clazz: Class<T>): T =
        clusterAnalysisFunction.getFromCache(modelId, clazz)

    override fun kMeansPredict(jsonOrSql: String, clusterAnalysis: ClusterAnalysis): Dataset<Row> {
        if (clusterAnalysis.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(aggregateService is AggregateServiceImpl)

        val model = clusterAnalysis.models.stream()
            .sorted(Comparator.comparing<ClusterAnalysis.Model?, OffsetDateTime?> { it.createdDateTime }.reversed())
            .findFirst().get()

        val dataset = kMeansPredict(jsonOrSql, UUID.fromString(model.id))
        val aggregateServiceImpl = aggregateService as AggregateServiceImpl

        aggregateServiceImpl.writeSinks(dataset, clusterAnalysis.sinks, false)

        return dataset
    }

    override fun kMeansPredict(jsonOrSql: String, kMeansModel: KMeansModel): Dataset<Row> =
        clusterAnalysisFunction.kMeansPredict(jsonOrSql, kMeansModel)

    override fun kMeansPredict(jsonOrSql: String, modelId: UUID): Dataset<Row> =
        kMeansPredict(jsonOrSql, getFromCache(modelId, KMeansModel::class.java))

    override fun putToCache(model: MLWritable): UUID = clusterAnalysisFunction.putToCache(model)
}