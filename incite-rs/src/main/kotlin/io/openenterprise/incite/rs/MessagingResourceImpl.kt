package io.openenterprise.incite.rs

import io.openenterprise.incite.service.MessagingService
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import java.time.Duration
import javax.inject.Inject
import javax.inject.Named
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

@Named
@Path("/messaging")
class MessagingResourceImpl : MessagingResource {

    @Inject
    private lateinit var messagingService: MessagingService

    @Path("/topics/{topic}")
    @Consumes(MediaType.TEXT_PLAIN)
    override fun produce(
        @PathParam("topic") topic: String,
        message: String,
        @QueryParam("ordered") @DefaultValue("true") ordered: Boolean,
        @QueryParam("replicated") @DefaultValue("true") replicated: Boolean,
        @Suspended asyncResponse: AsyncResponse
    ) {
        GlobalScope.launch {
            try {
                messagingService.produce(topic, message, ordered, replicated)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.noContent().build())
        }
    }
}