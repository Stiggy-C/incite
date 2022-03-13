package io.openenterprise.incite.service

import io.openenterprise.incite.data.domain.Route
import io.openenterprise.service.AbstractMutableEntityService
import java.util.*

interface RouteService: AbstractMutableEntityService<Route, String> {

    fun addRoute(id: UUID)

    fun hasRoute(id: UUID): Boolean

    fun isStarted(id: UUID): Boolean

    fun removeRoute(id: UUID)

    fun resumeRoute(id: UUID)

    fun startRoute(id: UUID)

    fun stopRoute(id: UUID)

    fun suspendRoute(id: UUID)
}