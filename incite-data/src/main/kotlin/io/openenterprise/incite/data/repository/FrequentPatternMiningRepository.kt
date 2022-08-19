package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.FrequentPatternMining
import org.springframework.stereotype.Repository

@Repository
interface FrequentPatternMiningRepository: AbstractEntityRepository<FrequentPatternMining, String>