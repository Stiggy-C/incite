package io.openenterprise.incite.context

import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.jpa.repository.config.EnableJpaRepositories

@Configuration
@EnableJpaRepositories("io.openenterprise.incite.data.repository")
@EntityScan("io.openenterprise.data.domain", "io.openenterprise.incite.data.domain")
class RepositoriesConfiguration