package io.openenterprise.incite.data.repository

import io.openenterprise.incite.data.domain.Recommendation
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface RecommendationRepository: JpaRepository<Recommendation, String>