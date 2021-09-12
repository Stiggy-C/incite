package io.openenterprise.incite.context

import io.openenterprise.glassfish.jersey.spring.SpringBridgeFeature
import org.glassfish.jersey.server.ResourceConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Configuration
import java.util.stream.Collectors
import javax.annotation.PostConstruct
import javax.ws.rs.ApplicationPath
import javax.ws.rs.Path

@ApplicationPath("/rs")
@ConditionalOnBean(ResourcesConfiguration::class)
@Configuration
class JerseyConfiguration(private var applicationContext: ApplicationContext) : ResourceConfig() {

    @PostConstruct
    fun postConstruct() {
        registerInstances(applicationContext.getBean(SpringBridgeFeature::class.java))
        registerInstances(
            applicationContext.getBeansWithAnnotation(Path::class.java).values.stream().collect(Collectors.toSet())
        )
    }
}