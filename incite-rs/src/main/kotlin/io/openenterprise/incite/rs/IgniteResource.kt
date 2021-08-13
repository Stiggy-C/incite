package io.openenterprise.incite.rs

import org.apache.ignite.cluster.ClusterState
import org.springframework.web.bind.annotation.PathVariable
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended

@Path("/ignite")
interface IgniteResource {

    @Path("/cluster/cluster-state/{clusterState}")
    @POST
    fun changeClusterState(
        @PathParam("clusterState") clusterState: ClusterState,
        @Suspended asyncResponse: AsyncResponse
    )
}