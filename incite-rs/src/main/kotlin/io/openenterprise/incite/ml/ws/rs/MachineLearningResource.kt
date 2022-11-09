package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.MachineLearning
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.container.AsyncResponse

interface MachineLearningResource<T: MachineLearning<out MachineLearning.Algorithm, out MachineLearning.Model<M>>, M : MachineLearning.Model<M>>: AbstractMutableEntityResource<T, String> {

    fun buildModel(id: String, asyncResponse: AsyncResponse)

    fun predict(id: String, jsonOrSql: String, asyncResponse: AsyncResponse)
}
