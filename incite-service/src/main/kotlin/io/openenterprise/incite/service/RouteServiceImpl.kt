package io.openenterprise.incite.service

import io.openenterprise.incite.data.domain.Route
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import org.apache.ignite.IgniteMessaging
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject
import javax.inject.Named

@Named
class RouteServiceImpl : RouteService, AbstractAbstractMutableEntityServiceImpl<Route, UUID>() {

    @Inject
    lateinit var igniteMessaging: IgniteMessaging

    @Inject
    lateinit var transactionTemplate: TransactionTemplate

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
}