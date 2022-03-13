package io.openenterprise.incite.ws.rs

import io.openenterprise.incite.data.domain.Route
import io.openenterprise.ws.rs.AbstractMutableEntityResource
import javax.ws.rs.Consumes
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.MediaType

@Path("/routes")
interface RouteResource : AbstractMutableEntityResource<Route, String> {

    @POST
    @Consumes(value = [MediaType.TEXT_PLAIN, MediaType.TEXT_XML])
    @Produces(MediaType.APPLICATION_JSON)
    fun create(body: String, asyncResponse: AsyncResponse)

    @POST
    @Path("/{id}/resume")
    fun resume(id: String, asyncResponse: AsyncResponse)

    @POST
    @Path("/{id}/start")
    fun start(id: String, asyncResponse: AsyncResponse)

    @POST
    @Path("/{id}/stop")
    fun stop(id: String, asyncResponse: AsyncResponse)

    @POST
    @Path("/{id}/suspend")
    fun suspend(id: String, asyncResponse: AsyncResponse)
}