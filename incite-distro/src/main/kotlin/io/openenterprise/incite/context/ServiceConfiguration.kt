package io.openenterprise.incite.context

import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ComponentScan("io.openenterprise.incite.ml.service", "io.openenterprise.incite.service", "io.openenterprise.incite.spark.sql.service")
class ServiceConfiguration