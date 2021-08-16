package io.openenterprise.service

import io.openenterprise.data.domain.AbstractMutableEntity
import java.io.Serializable

abstract class AbstractAbstractMutableEntityServiceImpl<T : AbstractMutableEntity<ID>, ID : Serializable> :
    AbstractAbstractEntityServiceImpl<T, ID>(), AbstractMutableEntityService<T, ID> {

    @kotlin.jvm.Throws(IllegalArgumentException::class)
    override fun update(entity: T) {
        if (entity.id == null) {
            throw IllegalArgumentException()
        }

        abstractEntityRepository.save(entity)
    }
}