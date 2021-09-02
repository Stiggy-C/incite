package io.openenterprise.incite.context

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan("io.openenterprise.camel.dsl.yaml")
class CamelConfiguration {
}