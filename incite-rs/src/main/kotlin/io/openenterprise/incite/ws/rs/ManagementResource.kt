package io.openenterprise.incite.ws.rs

import org.apache.ignite.cluster.ClusterState
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended

@Path("/management")
interface ManagementResource {

    @Path("/ignite/cluster-state/{clusterState}")
    @POST
    fun setIgniteClusterState(@PathParam("clusterState") clusterState: ClusterState, @Suspended asyncResponse: AsyncResponse)

}