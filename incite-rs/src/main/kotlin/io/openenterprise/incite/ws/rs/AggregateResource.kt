package io.openenterprise.incite.ws.rs

import io.openenterprise.incite.data.domain.Aggregate
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended

@Path("/aggregates")
interface AggregateResource: AbstractMutableEntityResource<Aggregate, String> {

    @POST
    @Path("/{id}/aggregate")
    fun aggregate(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse)
}