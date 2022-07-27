package io.openenterprise.incite.context

import io.openenterprise.ws.rs.AbstractAbstractMutableEntityResourceImpl
import io.openenterprise.ws.rs.ext.JsonMergePatchMessageBodyReader
import org.glassfish.jersey.server.ResourceConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
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
        registerClasses(JsonMergePatchMessageBodyReader::class.java)

        // Resources:
        registerInstances(applicationContext.getBeansWithAnnotation(Path::class.java).values.toTypedArray())
    }
}