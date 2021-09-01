package io.openenterprise.incite.service

import io.openenterprise.camel.dsl.yaml.YamlRoutesBuilderLoader
import io.openenterprise.incite.data.domain.Route
import io.openenterprise.incite.data.domain.YamlRoute
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.camel.CamelContext
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.impl.engine.AbstractCamelContext
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.BooleanUtils.isFalse
import org.apache.ignite.IgniteMessaging
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.*
import java.util.stream.Collectors
import javax.annotation.PostConstruct
import javax.inject.Inject
import javax.inject.Named
import javax.persistence.DiscriminatorValue

@Named
class RouteServiceImpl : RouteService, AbstractAbstractMutableEntityServiceImpl<Route, UUID>() {

    companion object {

        val LOG: Logger = LoggerFactory.getLogger(RouteServiceImpl::class.java)

    }

    @Inject
    lateinit var camelContext: CamelContext

    @Inject
    lateinit var igniteMessaging: IgniteMessaging

    @Inject
    lateinit var transactionTemplate: TransactionTemplate

    @Inject
    lateinit var yamlRoutesBuilderLoader: YamlRoutesBuilderLoader

    override fun addRoute(id: UUID) {
        val route = abstractEntityRepository.getOne(id)

        val routeBuilder = when (route.javaClass.getAnnotation(DiscriminatorValue::class.java).value) {
            "YAML" -> {
                yamlRoutesBuilderLoader.builder(route as YamlRoute)
            }
            else -> {
                throw NotImplementedError()
            }
        }

        camelContext.addRoutes(routeBuilder)
    }

    override fun removeRoute(id: UUID) {
        camelContext.removeRoute(id.toString())
    }

    override fun resumeRoute(id: UUID) {
        (camelContext as AbstractCamelContext).resumeRoute(id.toString())
    }

    override fun startRoute(id: UUID) {
        val hasRoute = camelContext.routes.stream().anyMatch { StringUtils.equals(id.toString(), it.routeId) }

        if (isFalse(hasRoute)) {
            this.addRoute(id)
        }

        (camelContext as AbstractCamelContext).startRoute(id.toString())
    }

    override fun stopRoute(id: UUID) {
        (camelContext as AbstractCamelContext).stopRoute(id.toString())
    }

    override fun suspendRoute(id: UUID) {
        (camelContext as AbstractCamelContext).suspendRoute(id.toString())
    }

    override fun create(entity: Route): Route {
        transactionTemplate.execute { super.create(entity) }
        igniteMessaging.sendOrdered("route_created", entity.id, Duration.ofMinutes(1).toMillis())

        return entity
    }

    override fun create(entities: Collection<Route>): List<Route> {
        transactionTemplate.execute { super.create(entities) }

        entities.forEach{
            igniteMessaging.sendOrdered("route_created", it.id, Duration.ofMinutes(1).toMillis())
        }

        return entities.stream().collect(Collectors.toList())
    }

    override fun delete(entity: Route) {
        assert (entity.id != null)

        this.delete(entity.id!!)
    }

    override fun delete(id: UUID) {
        transactionTemplate.execute { super.delete(id) }
        igniteMessaging.sendOrdered("route_deleted", id, Duration.ofMinutes(1).toMillis())
    }

    override fun update(entity: Route) {
        transactionTemplate.execute { super.update(entity) }
        igniteMessaging.sendOrdered("route_updated", entity.id, Duration.ofMinutes(1).toMillis())
    }

    @PostConstruct
    fun postConstruct() {
        igniteMessaging.remoteListen("route_created") { nodeId, id ->
            LOG.info("{} received {} from topic, {}", nodeId, id, "route_created")
            this.startRoute(id as UUID)
            true
        }
        igniteMessaging.remoteListen("route_deleted") { nodeId, id ->
            LOG.info("{} received {} from topic, {}", nodeId, id, "route_deleted")
            this.stopRoute(id as UUID)
            this.removeRoute(id)
            true
        }
        igniteMessaging.remoteListen("route_updated") { nodeId, id ->
            LOG.info("{} received {} from topic, {}", nodeId, id, "route_updated")
            this.stopRoute(id as UUID)
            this.removeRoute(id)
            this.startRoute(id)
            true
        }
    }
}