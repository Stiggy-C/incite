package io.openenterprise.incite.ws.rs

import io.openenterprise.incite.data.domain.Aggregate
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.ws.rs.AbstractAbstractMutableEntityResourceImpl
import kotlinx.coroutines.launch
import javax.inject.Named
import javax.persistence.EntityNotFoundException
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

@Named
@Path("/aggregates")
class AggregateResourceImpl: AggregateResource, AbstractAbstractMutableEntityResourceImpl<Aggregate, String>() {

    @POST
    @Path("/{id}/aggregate")
    override fun aggregate(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            try {
                val aggregateService = abstractMutableEntityService as AggregateService
                val aggregate = aggregateService.retrieve(id) ?: throw EntityNotFoundException()

                aggregateService.aggregate(aggregate)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.ok().build())
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    override fun create(entity: Aggregate, @Suspended asyncResponse: AsyncResponse) {
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
    override fun update(@PathParam("id") id: String, entity: Aggregate, @Suspended asyncResponse: AsyncResponse) {
        super.update(id, entity, asyncResponse)
    }
}