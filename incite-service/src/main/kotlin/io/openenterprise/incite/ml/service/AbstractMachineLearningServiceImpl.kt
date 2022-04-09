package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.AbstractFunction
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*

abstract class AbstractMachineLearningServiceImpl<T: MachineLearning<*>, F: AbstractFunction>(
    private val aggregateService: AggregateService,
    private val datasetService: DatasetService,
    private val function: F
) :
    MachineLearningService<T, F>,
    AbstractAbstractMutableEntityServiceImpl<T, String>() {

    override fun <M : Model<M>> getFromCache(modelId: UUID): M = function.getFromCache(modelId)

    override fun putToCache(model: MLWritable): UUID = function.putToCache(model)

    protected fun getAggregatedDataset(entity: T): Dataset<Row> {
        val datasets = datasetService.load(entity.sources, Collections.emptyMap<String, Any>())

        return (aggregateService as AggregateServiceImpl).joinSources(datasets, entity.joins)
    }
}