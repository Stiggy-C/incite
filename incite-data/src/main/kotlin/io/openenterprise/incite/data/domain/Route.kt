package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractMutableEntity
import java.util.*

class Route: AbstractMutableEntity<UUID>() {

    var yaml: String? = null
}