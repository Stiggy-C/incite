package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.AbstractFunction
import io.openenterprise.incite.data.domain.Aggregate
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.Serializable
import java.util.*

abstract class AbstractMLServiceImpl<T: Aggregate, ID: Serializable, F: AbstractFunction>(
    private val aggregateService: AggregateService,
    private val function: F
) :
    AbstractMLService<T, F>,
    AbstractAbstractMutableEntityServiceImpl<T, String>() {

    override fun <M : Model<M>> getFromCache(modelId: UUID): M = function.getFromCache(modelId)

    override fun putToCache(model: MLWritable): UUID = function.putToCache(model)

    protected fun getAggregatedDataset(entity: T): Dataset<Row> {
        val aggregateServiceImpl = aggregateService as AggregateServiceImpl
        val datasets = aggregateServiceImpl.loadSources(entity.sources, Collections.emptyMap<String, Any>())

        return aggregateServiceImpl.joinSources(datasets, entity.joins)
    }
}