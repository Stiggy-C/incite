package io.openenterprise.incite.context

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan(basePackages = ["io.openenterprise.glassfish.jersey.spring", "io.openenterprise.incite.rs"])
class ResourcesConfiguration