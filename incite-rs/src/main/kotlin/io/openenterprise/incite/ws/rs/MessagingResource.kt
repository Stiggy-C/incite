package io.openenterprise.incite.ws.rs

import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType

@Path("/messaging")
interface MessagingResource {

    @Path("/topics/{topic}")
    @Consumes(MediaType.TEXT_PLAIN)
    fun produce(
        @PathParam("topic") topic: String,
        message: String,
        @QueryParam("ordered") @DefaultValue("true") ordered: Boolean,
        @QueryParam("replicated") @DefaultValue("true") replicated: Boolean,
        @Suspended asyncResponse: AsyncResponse
    )

}