package io.openenterprise.incite.context

import io.openenterprise.glassfish.jersey.spring.SpringBridgeFeature
import org.apache.commons.collections4.SetUtils
import org.glassfish.jersey.server.ResourceConfig
import org.reflections.Reflections
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import java.util.stream.Collectors
import javax.annotation.PostConstruct
import javax.ws.rs.ApplicationPath
import javax.ws.rs.Path

@ApplicationPath("/rs")
@Configuration
@DependsOn("jaxRsConfiguration")
@Order(Ordered.LOWEST_PRECEDENCE)
class JerseyConfiguration : ResourceConfig() {

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @PostConstruct
    fun postConstruct() {
        // Providers:
        registerInstances(SpringBridgeFeature(applicationContext))

        // Resources:
        val mlResources =
            Reflections("io.openenterprise.incite.ml.ws.rs").getTypesAnnotatedWith(Path::class.java).stream()
                .filter { !it.isInterface }
                .collect(Collectors.toSet())

        val resources =
            Reflections("io.openenterprise.incite.ws.rs").getTypesAnnotatedWith(Path::class.java).stream()
                .filter { !it.isInterface }
                .collect(Collectors.toSet())

        registerClasses(SetUtils.union(mlResources, resources))
    }
}