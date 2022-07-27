package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.Clustering
import org.springframework.stereotype.Component
import javax.inject.Named
import javax.json.JsonMergePatch
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType

@Path("/cluster-analyses")
@Component
class ClusteringResourceImpl : ClusteringResource,
    AbstractMachineLearningResourceImpl<Clustering>() {

    @GET
    @Path("/{id}/model")
    @Produces(MediaType.APPLICATION_JSON)
    override fun buildModel(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
       super.buildModel(id, asyncResponse)
    }

    @POST
    @Path("/{id}/predict")
    @Consumes(MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    override fun predict(@PathParam("id") id: String, jsonOrSql: String, @Suspended asyncResponse: AsyncResponse) {
        super.predict(id, jsonOrSql, asyncResponse)
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    override fun create(entity: Clustering, @Suspended asyncResponse: AsyncResponse) {
        super.create(entity, asyncResponse)
    }

    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    override fun retrieve(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        super.retrieve(id, asyncResponse)
    }

    @DELETE
    @Path("/{id}")
    override fun delete(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        super.delete(id, asyncResponse)
    }

    @PATCH
    @Path("/{id}")
    @Consumes("application/merge-patch+json")
    override fun update(
        @PathParam("id") id: String,
        jsonMergePatch: JsonMergePatch,
        @Suspended asyncResponse: AsyncResponse
    ) {
        super.update(id, jsonMergePatch, asyncResponse)
    }
}