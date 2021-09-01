package io.openenterprise.incite.rs

import org.apache.ignite.cluster.ClusterState
import org.springframework.web.bind.annotation.PathVariable
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended

@Path("/cluster")
interface ClusterResource {

    @Path("/state/{clusterState}")
    @POST
    fun changeState(@PathParam("clusterState") clusterState: ClusterState, @Suspended asyncResponse: AsyncResponse)

}