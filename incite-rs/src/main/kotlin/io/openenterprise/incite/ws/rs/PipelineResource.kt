package io.openenterprise.incite.ws.rs

import io.openenterprise.incite.data.domain.Pipeline
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended

@Path("/pipelines")
interface PipelineResource: AbstractMutableEntityResource<Pipeline, String> {

    @POST
    @Path("/{id}/start")
    fun start(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse)

    @GET
    @Path("/{id}/status")
    fun status(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse)
}