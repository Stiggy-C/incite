package io.openenterprise.incite.context

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan("io.openenterprise.incite.service")
class ServiceConfiguration