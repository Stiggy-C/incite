package io.openenterprise.service

import io.openenterprise.data.domain.AbstractMutableEntity
import java.io.Serializable

interface AbstractMutableEntityService<T: AbstractMutableEntity<ID>, ID: Serializable>: AbstractEntityService<T, ID> {

    fun update(entity: T)
}