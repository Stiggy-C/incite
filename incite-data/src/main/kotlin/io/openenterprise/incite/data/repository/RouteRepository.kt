package io.openenterprise.incite.data.repository

import io.openenterprise.data.repoistory.AbstractEntityRepository
import io.openenterprise.incite.data.domain.Route
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface RouteRepository: AbstractEntityRepository<Route, String>