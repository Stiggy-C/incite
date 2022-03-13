package io.openenterprise.incite.ws.rs

import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import io.openenterprise.incite.data.domain.Route
import io.openenterprise.incite.data.domain.SpringXmlRoute
import io.openenterprise.incite.data.domain.YamlRoute
import io.openenterprise.incite.service.RouteService
import io.openenterprise.ws.rs.AbstractAbstractMutableEntityResourceImpl
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Autowired
import java.io.IOException
import java.util.*
import javax.inject.Named
import javax.persistence.EntityNotFoundException
import javax.ws.rs.*
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.container.Suspended
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

@Named
@Path("/routes")
class RouteResourceImpl : RouteResource, AbstractAbstractMutableEntityResourceImpl<Route, String>() {

    @Autowired
    private lateinit var xmlMapper: XmlMapper

    @Autowired
    private lateinit var yamlMapper: YAMLMapper

    @POST
    @Consumes(MediaType.TEXT_PLAIN, MediaType.TEXT_XML)
    @Produces(MediaType.APPLICATION_JSON)
    override fun create(body: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch(Dispatchers.IO) {
            val route: Route = when (determineType(body)) {
                Route.Type.SPRING_XML -> {
                    val springXmlRoute = SpringXmlRoute()
                    springXmlRoute.xml = body

                    springXmlRoute
                }
                Route.Type.YAML -> {
                    val yamlRoute = YamlRoute()
                    yamlRoute.yaml = body

                    yamlRoute
                }
                else -> {
                    asyncResponse.resume(Response.status(501).build())

                    return@launch
                }
            }

            val authentication = getAuthentication()
            val username = if (authentication == null) "Anonymous" else authentication.name

            route.createdBy = username

            super.create(route, asyncResponse)
        }
    }

    @POST
    @Path("/{id}/start")
    override fun start(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            if (abstractMutableEntityService.retrieve(id) == null) {
                asyncResponse.resume(EntityNotFoundException())

                return@launch
            }


            (abstractMutableEntityService as RouteService).startRoute(UUID.fromString(id))
        }
    }

    @POST
    @Path("/{id}/stop")
    override fun stop(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            if (abstractMutableEntityService.retrieve(id) == null) {
                asyncResponse.resume(EntityNotFoundException())

                return@launch
            }

            (abstractMutableEntityService as RouteService).stopRoute(UUID.fromString(id))
        }
    }

    @POST
    @Path("/{id}/resume")
    override fun resume(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            if (abstractMutableEntityService.retrieve(id) == null) {
                asyncResponse.resume(EntityNotFoundException())

                return@launch
            }

            (abstractMutableEntityService as RouteService).resumeRoute(UUID.fromString(id))
        }
    }

    @POST
    @Path("/{id}/suspend")
    override fun suspend(@PathParam("id") id: String, @Suspended asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            if (abstractMutableEntityService.retrieve(id) == null) {
                asyncResponse.resume(EntityNotFoundException())

                return@launch
            }

            (abstractMutableEntityService as RouteService).suspendRoute(UUID.fromString(id))
        }
    }

    override fun create(entity: Route, asyncResponse: AsyncResponse) {
        asyncResponse.resume(Response.status(Response.Status.NOT_IMPLEMENTED).build())
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
    override fun update(@PathParam("id") id: String, entity: Route, @Suspended asyncResponse: AsyncResponse) {
        super.update(id, entity, asyncResponse)
    }

    private fun determineType(string: String): Route.Type? =
        if (isXml(string)) Route.Type.SPRING_XML else if (isYaml(string)) Route.Type.YAML else null

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