package io.openenterprise.incite.context

import org.glassfish.jersey.server.ResourceConfig
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import javax.annotation.PostConstruct
import javax.ws.rs.ApplicationPath
import javax.ws.rs.Path

@ApplicationPath("/rs")
@Configuration
@DependsOn("resourceConfiguration")
class JerseyConfiguration(private var applicationContext: ApplicationContext) : ResourceConfig() {

    @PostConstruct
    fun postConstruct() {
        applicationContext.getBeansWithAnnotation(Path::class.java).values.forEach { register(it) }
    }
}