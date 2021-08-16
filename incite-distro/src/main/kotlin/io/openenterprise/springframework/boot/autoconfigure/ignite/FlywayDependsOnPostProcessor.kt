package io.openenterprise.springframework.boot.autoconfigure.ignite

import io.openenterprise.springframework.jdbc.support.IgniteStartupValidator
import org.flywaydb.core.Flyway
import org.springframework.boot.autoconfigure.AbstractDependsOnBeanFactoryPostProcessor

class FlywayDependsOnPostProcessor :
    AbstractDependsOnBeanFactoryPostProcessor(Flyway::class.java, IgniteStartupValidator::class.java) {
}