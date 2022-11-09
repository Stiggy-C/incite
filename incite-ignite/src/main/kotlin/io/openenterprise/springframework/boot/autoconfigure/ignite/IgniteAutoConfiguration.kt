package io.openenterprise.springframework.boot.autoconfigure.ignite

import org.apache.commons.lang3.ArrayUtils
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.*
import org.apache.ignite.configuration.ClientConnectorConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.springframework.boot.autoconfigure.IgniteConfigurer
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.*
import java.util.*
import java.util.Objects.isNull
import javax.sql.DataSource

@Configuration
@ComponentScan("io.openenterprise.ignite.cache.query.ml")
@ConditionalOnClass(Ignite::class)
@EnableConfigurationProperties
class IgniteAutoConfiguration : org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration() {

    @Bean
    @ConditionalOnMissingBean
    @Primary
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    protected fun ignite(igniteConfiguration: IgniteConfiguration, igniteConfigurer: IgniteConfigurer): Ignite {
        igniteConfigurer.accept(igniteConfiguration)

        return Ignition.start(igniteConfiguration)
    }

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
    @ConditionalOnMissingBean
    @ConfigurationProperties(prefix = "ignite")
    protected fun igniteConfiguration(): IgniteConfiguration = IgniteConfiguration()*/

    @Bean
    @ConditionalOnMissingBean
    @Primary
    protected fun igniteConfigurer(): IgniteConfigurer =
        IgniteConfigurer { igniteConfig ->
            igniteConfig.sqlConfiguration?.let { sqlConfig ->
                // If none of the query engine is being set as default, set the first one as default
                if (
                    ArrayUtils.isNotEmpty(sqlConfig.queryEnginesConfiguration) &&
                    Arrays.stream(sqlConfig.queryEnginesConfiguration).allMatch { !it.isDefault }
                ) sqlConfig.queryEnginesConfiguration[0].isDefault = true
            }
        }

    @Bean
    @DependsOn("igniteCluster")
    @Primary
    protected fun igniteJdbcThinDataSource(ignite: Ignite): DataSource {
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
            "jdbc:ignite:thin://127.0.0.1:${clientConnectorPort}/${sqlSchema}?lazy=true"
        )

        return igniteJdbcThinDataSource
    }

    @Bean
    protected fun igniteMessaging(ignite: Ignite, igniteCluster: IgniteCluster): IgniteMessaging {
        val clusterGroup = igniteCluster.forPredicate { node -> !node.isClient }.forPredicate { node -> !node.isDaemon }

        return ignite.message(clusterGroup)
    }
}