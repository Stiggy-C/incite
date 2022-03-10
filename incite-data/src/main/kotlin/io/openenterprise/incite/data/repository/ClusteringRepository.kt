package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.Clustering
import org.springframework.stereotype.Repository

@Repository
interface ClusteringRepository: AbstractEntityRepository<Clustering, String>