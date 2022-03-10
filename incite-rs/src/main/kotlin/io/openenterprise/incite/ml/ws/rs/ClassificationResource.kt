package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.Classification
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.container.AsyncResponse

interface ClassificationResource: AbstractMutableEntityResource<Classification, String> {

    fun buildModel(id: String, asyncResponse: AsyncResponse)

    fun predict(id: String, jsonOrSql: String, asyncResponse: AsyncResponse)
}