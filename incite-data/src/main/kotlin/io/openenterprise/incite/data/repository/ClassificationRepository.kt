package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.Classification
import org.springframework.stereotype.Repository

@Repository
interface ClassificationRepository: AbstractEntityRepository<Classification, String>