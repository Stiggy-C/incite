package io.openenterprise.incite.service

import io.openenterprise.ignite.cache.query.ml.ClusterAnalysisFunction
import io.openenterprise.incite.data.domain.BisectingKMeans
import io.openenterprise.incite.data.domain.ClusterAnalysis
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
class ClusterAnalysisServiceImpl(
    @Inject private val aggregateService: AggregateService,
    @Inject private val clusterAnalysisFunction: ClusterAnalysisFunction
) :
    ClusterAnalysisService,
    AbstractAbstractMutableEntityServiceImpl<ClusterAnalysis, String>() {

    override fun <M : Model<M>> buildModel(clusterAnalysis: ClusterAnalysis): M {
        val dataset = getAggregatedDataset(clusterAnalysis)

        @Suppress("UNCHECKED_CAST")
        return when (val algorithm = clusterAnalysis.algorithm) {
            is ClusterAnalysis.FeatureColumnsBasedAlgorithm -> {
                when (algorithm) {
                    is BisectingKMeans -> {
                        clusterAnalysisFunction.buildBisectingKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIteration,
                            algorithm.seed
                        )
                    }
                    is KMeans -> {
                        clusterAnalysisFunction.buildKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIteration,
                            algorithm.seed
                        )
                    }
                    else -> throw UnsupportedOperationException()
                }
            }
            else -> throw UnsupportedOperationException()
        } as M
    }

    override fun <M : Model<M>> getFromCache(modelId: UUID): M = clusterAnalysisFunction.getFromCache(modelId)

    override fun predict(jsonOrSql: String, clusterAnalysis: ClusterAnalysis): Dataset<Row> {
        if (clusterAnalysis.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(aggregateService is AggregateServiceImpl)

        val model = clusterAnalysis.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> =
            when (clusterAnalysis.algorithm) {
                is BisectingKMeans -> getFromCache<BisectingKMeansModel>(UUID.fromString(model.id))
                is KMeans -> getFromCache<KMeansModel>(UUID.fromString(model.id))
                else -> throw UnsupportedOperationException()
            }

        val dataset = clusterAnalysisFunction.predict(jsonOrSql, sparkModel)
        val aggregateServiceImpl = aggregateService as AggregateServiceImpl

        aggregateServiceImpl.writeSinks(dataset, clusterAnalysis.sinks, false)

        return dataset
    }

    override fun putToCache(model: MLWritable): UUID = clusterAnalysisFunction.putToCache(model)

    private fun getAggregatedDataset(clusterAnalysis: ClusterAnalysis): Dataset<Row> {
        val aggregateServiceImpl = aggregateService as AggregateServiceImpl
        val datasets = aggregateServiceImpl.loadSources(clusterAnalysis.sources, Collections.emptyMap<String, Any>())

        return aggregateServiceImpl.joinSources(datasets, clusterAnalysis.joins)
    }
}