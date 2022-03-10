package io.openenterprise.incite.ml.service

import io.openenterprise.data.domain.AbstractMutableEntity
import io.openenterprise.ignite.cache.query.ml.AbstractFunction
import io.openenterprise.incite.data.domain.Aggregate
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
    }
}