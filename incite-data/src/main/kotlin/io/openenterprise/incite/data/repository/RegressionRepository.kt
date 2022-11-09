package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.Regression
import org.springframework.stereotype.Repository

@Repository
interface RegressionRepository: AbstractEntityRepository<Regression, String>