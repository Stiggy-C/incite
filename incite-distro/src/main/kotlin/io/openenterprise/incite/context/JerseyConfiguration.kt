package io.openenterprise.incite.context

import org.glassfish.jersey.server.ResourceConfig
import org.springframework.context.annotation.Configuration
import javax.ws.rs.ApplicationPath

@Configuration
@ApplicationPath("/rs")
class JerseyConfiguration : ResourceConfig()