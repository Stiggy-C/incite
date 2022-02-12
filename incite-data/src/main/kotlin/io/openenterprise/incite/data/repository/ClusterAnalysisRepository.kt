package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.ClusterAnalysis
import org.springframework.stereotype.Repository

@Repository
interface ClusterAnalysisRepository: AbstractEntityRepository<ClusterAnalysis, String>