package io.openenterprise.incite.service

import io.openenterprise.incite.data.domain.Route
import io.openenterprise.service.AbstractAbstractMutableEntityServiceImpl
import java.util.*
import javax.inject.Named

@Named
class RouteServiceImpl : RouteService, AbstractAbstractMutableEntityServiceImpl<Route, UUID>()