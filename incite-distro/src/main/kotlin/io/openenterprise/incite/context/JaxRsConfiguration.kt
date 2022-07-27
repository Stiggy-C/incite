package io.openenterprise.incite.context

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan(value = ["io.openenterprise.incite.ml.ws.rs", "io.openenterprise.incite.ws.rs"])
class JaxRsConfiguration