package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.Clustering
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.container.AsyncResponse

interface ClusteringResource: AbstractMutableEntityResource<Clustering, String> {

    fun buildModel(id: String, asyncResponse: AsyncResponse)

    fun predict(id: String, jsonOrSql: String, asyncResponse: AsyncResponse)
}