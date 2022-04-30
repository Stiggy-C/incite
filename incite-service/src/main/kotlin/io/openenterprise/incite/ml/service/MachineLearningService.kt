package io.openenterprise.incite.ml.service

import io.openenterprise.ignite.cache.query.Function
import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.data.domain.Sink
import io.openenterprise.incite.spark.sql.service.DatasetService
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.*

interface MachineLearningService<T : MachineLearning<*>> {

    fun <M : Model<M>> getFromCache(modelId: UUID): M

    fun persistModel(entity: T, sparkModel: MLWritable): UUID

    fun predict(entity: T, jsonOrSql: String): Dataset<Row>

    fun putToCache(model: MLWritable): UUID

    fun <M : Model<M>> train(entity: T): M

    abstract class BaseCompanionObject: Function.BaseCompanionObject() {

        protected fun writeToSinks(dataset: Dataset<Row>, sinks: List<Sink>) =
            getBean(DatasetService::class.java).write(dataset, sinks, false)
    }
}