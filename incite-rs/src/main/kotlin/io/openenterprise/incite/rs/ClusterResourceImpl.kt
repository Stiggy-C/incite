package io.openenterprise.incite.rs

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.IgniteException
import org.apache.ignite.cluster.ClusterState
import javax.inject.Inject
import javax.inject.Named
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.Response

@Named
@Path("/cluster")
class ClusterResourceImpl(@Inject private var igniteCluster: IgniteCluster) : ClusterResource {

    @Path("/state/{clusterState}")
    @POST
    override fun changeState(
        @PathParam("clusterState") clusterState: ClusterState,
        @Suspended asyncResponse: AsyncResponse
    ) {
        GlobalScope.launch {
            try {
                igniteCluster.state(clusterState)
            } catch (igniteException: IgniteException) {
                asyncResponse.resume(igniteException)
            }

            asyncResponse.resume(Response.ok().build())
        }
    }
}