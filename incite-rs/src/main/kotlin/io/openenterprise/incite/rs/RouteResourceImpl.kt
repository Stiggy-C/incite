package io.openenterprise.incite.rs

import io.openenterprise.incite.data.domain.Route
import io.openenterrpise.rs.AbstractAbstractMutableEntityResourceImpl
import java.util.*
import javax.inject.Named
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType

@Named
@Path("/routes")
class RouteResourceImpl : RouteResource, AbstractAbstractMutableEntityResourceImpl<Route, UUID>() {

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    override fun create(entity: Route, @Suspended asyncResponse: AsyncResponse) {
        super.create(entity, asyncResponse)
    }

    @Path("/{id}")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    override fun retrieve(@PathParam("id") id: UUID, @Suspended asyncResponse: AsyncResponse) {
        super.retrieve(id, asyncResponse)
    }

    @Path("/{id}")
    @DELETE
    override fun delete(@PathParam("id") id: UUID, @Suspended asyncResponse: AsyncResponse) {
        super.delete(id, asyncResponse)
    }

    @Path("/{id}")
    @PATCH
    @Consumes("application/merge-patch+json")
    override fun update(@PathParam("id") id: UUID, entity: Route, @Suspended asyncResponse: AsyncResponse) {
        super.update(id, entity, asyncResponse)
    }
}