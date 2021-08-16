package io.openenterprise.incite.context

import org.glassfish.jersey.server.ResourceConfig
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct
import javax.ws.rs.ApplicationPath
import javax.ws.rs.Path

@ApplicationPath("/rs")
@ConditionalOnBean(ResourcesConfiguration::class)
@Configuration
class JerseyConfiguration(private var applicationContext: ApplicationContext) : ResourceConfig() {

    @PostConstruct
    fun postConstruct() {
        applicationContext.getBeansWithAnnotation(Path::class.java).values.forEach { register(it) }
    }
}