package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.Aggregate
import org.springframework.stereotype.Repository

@Repository
interface AggregateRepository: AbstractEntityRepository<Aggregate, String>