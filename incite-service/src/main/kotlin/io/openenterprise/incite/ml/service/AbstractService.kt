package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.AbstractFunction
import io.openenterprise.incite.data.domain.Aggregate
import io.openenterprise.incite.data.domain.Sink
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*

interface AbstractService<T : Aggregate, F: AbstractFunction> {

    fun <M : Model<M>> buildModel(entity: T) : M

    fun predict(jsonOrSql: String, entity: T): Dataset<Row>

    fun <M : Model<M>> getFromCache(modelId: UUID): M

    fun putToCache(model: MLWritable): UUID

    abstract class BaseCompanionObject {

        protected fun <T> getBean(clazz: Class<T>): T  =
            ApplicationContextUtils.getApplicationContext()!!.getBean(clazz)

        protected fun writeToSinks(dataset: Dataset<Row>, sinks: List<Sink>) {
            var aggregateService = getBean(AggregateService::class.java)

            assert(aggregateService is AggregateServiceImpl)

            aggregateService = aggregateService as AggregateServiceImpl
            aggregateService.writeSinks(dataset, sinks, false)
        }
    }
}