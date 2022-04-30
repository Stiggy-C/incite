package io.openenterprise.incite.service

import io.openenterprise.camel.dsl.yaml.YamlRoutesBuilderLoader
import io.openenterprise.incite.data.domain.Route
import io.openenterprise.incite.data.domain.YamlRoute
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.camel.CamelContext
import org.apache.camel.impl.engine.AbstractCamelContext
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.BooleanUtils.isFalse
import org.apache.ignite.IgniteMessaging
import org.apache.ignite.lang.IgniteBiPredicate
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
open class RouteServiceImpl : RouteService, AbstractAbstractMutableEntityServiceImpl<Route, String>() {

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
        val route = abstractEntityRepository.getById(id.toString())

        val routeBuilder = when (route.javaClass.getAnnotation(DiscriminatorValue::class.java).value) {
            "YAML" -> {
                val yamlRoute = route as YamlRoute
                yamlRoutesBuilderLoader.builder(yamlRoute.id.toString(), yamlRoute.yaml)
            }
            else -> {
                throw NotImplementedError()
            }
        }

        camelContext.addRoutes(routeBuilder)
    }

    override fun hasRoute(id: UUID): Boolean =
        camelContext.routes.stream().anyMatch { StringUtils.equals(id.toString(), it.routeId) }

    override fun isStarted(id: UUID): Boolean = camelContext.getRoute(id.toString()).uptimeMillis > 0

    override fun removeRoute(id: UUID) {
        if (hasRoute(id)) {
            camelContext.removeRoute(id.toString())
        }
    }

    override fun resumeRoute(id: UUID) {
        if (hasRoute(id)) {
            (camelContext as AbstractCamelContext).resumeRoute(id.toString())
        }
    }

    override fun startRoute(id: UUID) {
        if (isFalse(hasRoute(id))) {
            this.addRoute(id)
        }

        if (isFalse(isStarted(id))) {
            (camelContext as AbstractCamelContext).startRoute(id.toString())
        }
    }

    override fun stopRoute(id: UUID) {
        if (hasRoute(id) && isStarted(id)) {
            (camelContext as AbstractCamelContext).stopRoute(id.toString())
        }
    }

    override fun suspendRoute(id: UUID) {
        if (hasRoute(id) && isStarted(id)) {
            (camelContext as AbstractCamelContext).suspendRoute(id.toString())
        }
    }

    override fun create(entity: Route): Route {
        transactionTemplate.execute { super.create(entity) }
        igniteMessaging.sendOrdered("start_route", entity.id, Duration.ofMinutes(1).toMillis())

        return entity
    }

    override fun create(entities: Collection<Route>): List<Route> {
        transactionTemplate.execute { super.create(entities) }

        entities.forEach {
            igniteMessaging.sendOrdered("start_route", it.id, Duration.ofMinutes(1).toMillis())
        }

        return entities.stream().collect(Collectors.toList())
    }

    override fun delete(entity: Route) {
        assert(entity.id != null)

        this.delete(entity.id!!)
    }

    override fun delete(id: String) {
        transactionTemplate.execute { super.delete(id) }
        igniteMessaging.sendOrdered("stop_route", id, Duration.ofMinutes(1).toMillis())
    }

    override fun update(entity: Route) {
        transactionTemplate.execute { super.update(entity) }
        igniteMessaging.sendOrdered("restart_route", entity.id, Duration.ofMinutes(1).toMillis())
    }

    @PostConstruct
    fun postConstruct() {
        igniteMessaging.remoteListen("restart_route", RestartRouteEventHandler())
        igniteMessaging.remoteListen("start_route", StartRouteEventHandler())
        igniteMessaging.remoteListen("stop_route", StopRouteEventHandler())
    }

    private class RestartRouteEventHandler : IgniteBiPredicate<UUID, UUID> {

        override fun apply(nodeId: UUID?, id: UUID?): Boolean {
            LOG.info("{} received {} from topic, {}", nodeId, id, "restart_route")

            val routeService = ApplicationContextUtils.getApplicationContext()!!.getBean(RouteService::class.java)
            routeService.stopRoute(id as UUID)
            routeService.removeRoute(id)
            routeService.startRoute(id)

            return true
        }
    }

    private class StartRouteEventHandler : IgniteBiPredicate<UUID, UUID> {

        override fun apply(nodeId: UUID?, id: UUID?): Boolean {
            LOG.info("{} received {} from topic, {}", nodeId, id, "start_route")
            ApplicationContextUtils.getApplicationContext()!!.getBean(RouteService::class.java).startRoute(id as UUID)

            return true
        }
    }

    private class StopRouteEventHandler : IgniteBiPredicate<UUID, UUID> {

        override fun apply(nodeId: UUID?, id: UUID?): Boolean {
            LOG.info("{} received {} from topic, {}", nodeId, id, "stop_route")

            val routeService = ApplicationContextUtils.getApplicationContext()!!.getBean(RouteService::class.java)
            routeService.stopRoute(id as UUID)
            routeService.removeRoute(id)

            return true
        }
    }
}