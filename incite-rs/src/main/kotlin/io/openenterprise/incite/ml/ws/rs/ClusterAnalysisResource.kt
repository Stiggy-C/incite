package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.ClusterAnalysis
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.container.AsyncResponse

interface ClusterAnalysisResource: AbstractMutableEntityResource<ClusterAnalysis, String> {

    fun buildModel(id: String, asyncResponse: AsyncResponse)

    fun predict(id: String, contentType: String, jsonOrSql: String, asyncResponse: AsyncResponse)
}