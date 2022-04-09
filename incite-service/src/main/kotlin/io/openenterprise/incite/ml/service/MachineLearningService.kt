package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.ml.AbstractFunction
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.Sink
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.incite.service.AggregateServiceImpl
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*

interface MachineLearningService<T : MachineLearning<*>, F : AbstractFunction> {

    fun <M : Model<M>> buildModel(entity: T): M

    fun persistModel(entity: T, sparkModel: MLWritable): UUID

    fun predict(entity: T, jsonOrSql: String): Dataset<Row>

    fun <M : Model<M>> getFromCache(modelId: UUID): M

    fun putToCache(model: MLWritable): UUID

    abstract class BaseCompanionObject {

        protected fun <T> getBean(clazz: Class<T>): T =
            ApplicationContextUtils.getApplicationContext()!!.getBean(clazz)

        protected fun writeToSinks(dataset: Dataset<Row>, sinks: List<Sink>) =
            getBean(DatasetService::class.java).write(dataset, sinks, false)
    }
}