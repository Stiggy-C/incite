package io.openenterprise.incite.rs

import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import io.openenterprise.incite.data.domain.Route
import io.openenterprise.incite.data.domain.SpringXmlRoute
import io.openenterprise.incite.data.domain.YamlRoute
import io.openenterprise.rs.AbstractAbstractMutableEntityResourceImpl
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.io.IOException
import java.util.*
import javax.inject.Inject
import javax.inject.Named
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

@Named
@Path("/routes")
class RouteResourceImpl : RouteResource, AbstractAbstractMutableEntityResourceImpl<Route, UUID>() {

    @Inject
    private lateinit var xmlMapper: XmlMapper

    @Inject
    private lateinit var yamlMapper: YAMLMapper

    @POST
    @Consumes(value = [MediaType.TEXT_PLAIN, MediaType.TEXT_XML])
    @Produces(MediaType.APPLICATION_JSON)
    override fun create(body: String, @Suspended asyncResponse: AsyncResponse) {
        GlobalScope.launch(Dispatchers.IO) {
            val route: Route?

            if (isXml(body)) {
                val springXmlRoute = SpringXmlRoute()
                springXmlRoute.xml = body

                route = springXmlRoute
            } else if (isYaml(body)) {
                val yamlRoute = YamlRoute()
                yamlRoute.yaml = body

                route = yamlRoute
            } else {
                asyncResponse.resume(Response.status(Response.Status.NOT_IMPLEMENTED).build())

                return@launch
            }

            create(route, asyncResponse)
        }
    }

    override fun create(entity: Route, asyncResponse: AsyncResponse) {
        asyncResponse.resume(Response.status(Response.Status.NOT_IMPLEMENTED).build())
    }

    @POST
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    override fun retrieve(@PathParam("id") id: UUID, @Suspended asyncResponse: AsyncResponse) {
        super.retrieve(id, asyncResponse)
    }

    @DELETE
    @Path("/{id}")
    override fun delete(@PathParam("id") id: UUID, @Suspended asyncResponse: AsyncResponse) {
        super.delete(id, asyncResponse)
    }

    @PATCH
    @Path("/{id}")
    @Consumes("application/merge-patch+json")
    override fun update(@PathParam("id") id: UUID, entity: Route, @Suspended asyncResponse: AsyncResponse) {
        super.update(id, entity, asyncResponse)
    }

    private fun isXml(string: String): Boolean {
        try {
            xmlMapper.readTree(string)
        } catch (e: IOException) {
            return false
        }

        return true
    }

    private fun isYaml(string: String): Boolean {
        try {
            yamlMapper.readTree(string)
        } catch (e: IOException) {
            return false
        }

        return true
    }
}