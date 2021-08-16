package io.openenterprise.ignite.springframework.boot.autoconfigure

import org.apache.ignite.Ignite
import org.apache.ignite.IgniteCluster
import org.apache.ignite.cluster.ClusterState
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.DependsOn
import org.springframework.context.annotation.Role

@Configuration
@EnableConfigurationProperties
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
class IgniteAutoConfiguration: org.apache.ignite.springframework.boot.autoconfigure.IgniteAutoConfiguration() {

    @Bean
    @ConditionalOnMissingBean(IgniteCluster::class)
    protected fun igniteCluster(ignite: Ignite): IgniteCluster {
        val igniteCluster = ignite.cluster()
        igniteCluster.state(ignite.configuration().clusterStateOnStart)

        return igniteCluster
    }
}