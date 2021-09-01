package io.openenterprise.incite.rs

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
class MessagingResourceImpl(
    @Inject private var ignite: Ignite,
    @Inject private var igniteCluster: IgniteCluster
) : MessagingResource {

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
                val igniteMessaging =
                    if (replicated) {
                        ignite.message(igniteCluster.forPredicate { node -> !node.isClient && !node.isDaemon })
                    } else {
                        ignite.message(
                            igniteCluster.forNodeId(
                                igniteCluster.nodes().stream().filter { node -> !node.isClient && !node.isDaemon }
                                    .findFirst()
                                    .get()
                                    .id()
                            )
                        )
                    }

                if (ordered) {
                    igniteMessaging.sendOrdered(topic, message, Duration.ofSeconds(5).toMillis())
                } else {
                    igniteMessaging.send(topic, message)
                }
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.noContent().build())
        }
    }
}