package io.openenterprise.springframework.boot.autoconfigure.ignite

import io.openenterprise.springframework.jdbc.support.IgniteStartupValidator
import org.apache.commons.lang.BooleanUtils
import org.apache.commons.lang.StringUtils
import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.IgniteJdbcThinDataSource
import org.apache.ignite.configuration.ClientConnectorConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.configuration.SqlConfiguration
import org.flywaydb.core.Flyway
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.*
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import java.util.Objects.isNull
import javax.sql.DataSource

@Configuration
@ComponentScan("io.openenterprise.ignite.cache.query.ml")
@ConditionalOnClass(Ignite::class)
@EnableConfigurationProperties
class IgniteAutoConfiguration : org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration() {

    /*@Bean
    @ConditionalOnBean(Flyway::class)
    @DependsOn("igniteJdbcThinDateSource")
    fun flywayDependsOnPostProcessor(): FlywayDependsOnPostProcessor {
        return FlywayDependsOnPostProcessor()
    }*/

    @Bean
    protected fun igniteCluster(ignite: Ignite): IgniteCluster {
        val igniteCluster = ignite.cluster()

        if (ignite.configuration().clusterStateOnStart == null) {
            igniteCluster.state(IgniteConfiguration.DFLT_STATE_ON_START)
        } else {
            igniteCluster.state(ignite.configuration().clusterStateOnStart)
        }

        ignite.configuration().cacheConfiguration.asSequence()
            .filter { BooleanUtils.isFalse(StringUtils.equals(it.name, "ignite-sys-cache")) }
            .forEach { ignite.addCacheConfiguration(it) }

        return igniteCluster
    }

    /*@Bean
    fun igniteDatabaseStartupValidator(igniteJdbcThinDataSource: IgniteJdbcThinDataSource): IgniteStartupValidator {
        val igniteStartupValidator = IgniteStartupValidator()
        igniteStartupValidator.setDataSource(igniteJdbcThinDataSource)

        return igniteStartupValidator
    }*/

    @Bean
    @DependsOn("igniteCluster")
    @Primary
    fun igniteJdbcThinDataSource(ignite: Ignite): DataSource {
        val igniteConfiguration = ignite.configuration()
        val clientConnectorConfiguration = igniteConfiguration.clientConnectorConfiguration
        val clientConnectorPort =
            if (isNull(clientConnectorConfiguration)) ClientConnectorConfiguration.DFLT_PORT else clientConnectorConfiguration!!.port
        val sqlConfiguration = igniteConfiguration.sqlConfiguration
        val sqlSchema = if (sqlConfiguration.sqlSchemas.isEmpty()) "incite" else sqlConfiguration.sqlSchemas[0]

        val igniteJdbcThinDataSource = IgniteJdbcThinDataSource()
        igniteJdbcThinDataSource.password = "ignite"
        igniteJdbcThinDataSource.username = "ignite"
        igniteJdbcThinDataSource.setUrl(
            "jdbc:ignite:thin://localhost:${clientConnectorPort}/${sqlSchema}?lazy=true"
        )

        return igniteJdbcThinDataSource
    }
}