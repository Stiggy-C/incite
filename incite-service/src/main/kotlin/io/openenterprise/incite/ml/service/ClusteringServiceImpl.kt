package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.ClusteringFunction
import io.openenterprise.incite.data.domain.BisectingKMeans
import io.openenterprise.incite.data.domain.Clustering
import io.openenterprise.incite.data.domain.KMeans
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.BisectingKMeansModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.EntityNotFoundException

@Named
class ClusteringServiceImpl(
    @Inject private val aggregateService: AggregateService,
    @Inject private val clusteringFunction: ClusteringFunction
) :
    ClusteringService,
    AbstractServiceImpl<Clustering, String, ClusteringFunction>(aggregateService, clusteringFunction) {

    override fun <M : Model<M>> buildModel(entity: Clustering): M {
        val dataset = getAggregatedDataset(entity)

        @Suppress("UNCHECKED_CAST")
        return when (val algorithm = entity.algorithm) {
            is Clustering.FeatureColumnsBasedAlgorithm -> {
                when (algorithm) {
                    is BisectingKMeans -> {
                        clusteringFunction.buildBisectingKMeansModel(
                            dataset,
                            algorithm.featureColumns.stream().collect((Collectors.joining(","))),
                            algorithm.k,
                            algorithm.maxIteration,
                            algorithm.seed
                        )
                    }
                    is KMeans -> {
                        clusteringFunction.buildKMeansModel(
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

    override fun predict(jsonOrSql: String, entity: Clustering): Dataset<Row> {
        if (entity.models.isEmpty()) {
            throw IllegalStateException("No models have been built")
        }

        assert(aggregateService is AggregateServiceImpl)

        val model = entity.models.stream().findFirst().orElseThrow { EntityNotFoundException() }
        val sparkModel: Model<*> =
            when (entity.algorithm) {
                is BisectingKMeans -> getFromCache<BisectingKMeansModel> (UUID.fromString(model.id))
                is KMeans -> getFromCache<KMeansModel>(UUID.fromString(model.id))
                else -> throw UnsupportedOperationException()
            }

        val dataset = clusteringFunction.predict(jsonOrSql, sparkModel)
        val aggregateServiceImpl = aggregateService as AggregateServiceImpl

        aggregateServiceImpl.writeSinks(dataset, entity.sinks, false)

        return dataset
    }
}