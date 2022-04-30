package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.incite.ml.service.MachineLearningService
import io.openenterprise.incite.spark.sql.service.DatasetService
import io.openenterprise.ws.rs.AbstractAbstractMutableEntityResourceImpl
import kotlinx.coroutines.launch
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.MLWritable
import org.springframework.beans.factory.annotation.Autowired
import javax.persistence.EntityNotFoundException
import javax.ws.rs.container.AsyncResponse

abstract class AbstractMachineLearningResourceImpl<T : MachineLearning<*>> : MachineLearningResource<T>,
    AbstractAbstractMutableEntityResourceImpl<T, String>() {

    @Autowired
    lateinit var datasetService: DatasetService

    @Autowired
    lateinit var machineLearningService: MachineLearningService<T>

    override fun buildModel(id: String, asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            val entity: T = abstractMutableEntityService.retrieve(id) ?: throw EntityNotFoundException()
            val sparkModel: Model<*> = machineLearningService.train(entity)
            val modelId = machineLearningService.persistModel(entity, sparkModel as MLWritable)

            asyncResponse.resume(modelId)
        }
    }

    override fun predict(id: String, jsonOrSql: String, asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            val entity: T = abstractMutableEntityService.retrieve(id) ?: throw EntityNotFoundException()
            val result = machineLearningService.predict(entity, jsonOrSql)

            datasetService.write(result, entity.sinks, false)

            asyncResponse.resume(result.count())
        }
    }
}