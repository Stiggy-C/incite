package io.openenterprise.incite.ws.rs

import io.openenterprise.incite.PipelineContext
import io.openenterprise.incite.data.domain.Pipeline
import io.openenterprise.incite.service.PipelineService
import io.openenterprise.ws.rs.AbstractAbstractMutableEntityResourceImpl
import io.openenterprise.ws.rs.core.Status
import kotlinx.coroutines.launch
import org.springframework.stereotype.Component
import javax.json.JsonMergePatch
import javax.persistence.EntityNotFoundException
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

@Path("/pipelines")
@Component
class PipelineResourceImpl : PipelineResource, AbstractAbstractMutableEntityResourceImpl<Pipeline, String>() {

    @POST
    @Path("/{id}/start")
    override fun start(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            val aggregateService = abstractMutableEntityService as PipelineService

            try {
                val aggregate = aggregateService.retrieve(id) ?: throw EntityNotFoundException()

                aggregateService.start(aggregate)
            } catch (e: Exception) {
                asyncResponse.resume(e)

                return@launch
            }

            val pipelineContext: PipelineContext = aggregateService.getContext(id)!!
            val response = when (pipelineContext.status) {
                PipelineContext.Status.PROCESSING -> Response.status(Status.PROCESSING).build()
                PipelineContext.Status.STOPPED -> Response.status(Response.Status.OK).build()
            }

            asyncResponse.resume(response)
        }
    }

    @GET
    @Path("/{id}/status")
    override fun status(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            val aggregateService = abstractMutableEntityService as PipelineService
            val pipelineContext: PipelineContext = try {
                aggregateService.retrieve(id) ?: throw EntityNotFoundException()
                aggregateService.getContext(id) ?: throw EntityNotFoundException()
            } catch (e: Exception) {
                asyncResponse.resume(e)

                return@launch
            }

            val response = when (pipelineContext.status) {
                PipelineContext.Status.PROCESSING -> Response.status(Status.PROCESSING).build()
                PipelineContext.Status.STOPPED -> Response.status(Response.Status.OK).build()
            }

            asyncResponse.resume(response)
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    override fun create(entity: Pipeline, @Suspended asyncResponse: AsyncResponse) {
        super.create(entity, asyncResponse)
    }

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    override fun retrieve(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        super.retrieve(id, asyncResponse)
    }

    @DELETE
    @Path("/{id}")
    override fun delete(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        super.delete(id, asyncResponse)
    }

    @PATCH
    @Path("/{id}")
    @Consumes("application/merge-patch+json")
    override fun update(
        @PathParam("id") id: String,
        jsonMergePatch: JsonMergePatch,
        @Suspended asyncResponse: AsyncResponse
    ) {
        super.update(id, jsonMergePatch, asyncResponse)
    }
}