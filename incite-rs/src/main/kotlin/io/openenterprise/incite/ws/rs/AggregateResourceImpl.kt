package io.openenterprise.incite.ws.rs

import io.openenterprise.incite.data.domain.Aggregate
import io.openenterprise.incite.service.AggregateService
import io.openenterprise.ws.rs.AbstractAbstractMutableEntityResourceImpl
import kotlinx.coroutines.launch
import javax.inject.Named
import javax.persistence.EntityNotFoundException
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.Response

@Named
@Path("/aggregates")
class AggregateResourceImpl: AbstractAbstractMutableEntityResourceImpl<Aggregate, String>(), AggregateResource {

    @POST
    @Path("/{id}/aggregate")
    override fun aggregate(id: String, asyncResponse: AsyncResponse) {
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
}