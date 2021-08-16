package io.openenterprise.service

import io.openenterprise.data.domain.AbstractEntity
import java.io.Serializable

interface AbstractEntityService<T : AbstractEntity<ID>, ID: Serializable> {

    fun create(entity: T): T

    fun create(entities: Collection<T>): List<T>

    fun retrieve(id: ID): T?

    fun delete(entity: T)

    fun delete(id: ID)
}