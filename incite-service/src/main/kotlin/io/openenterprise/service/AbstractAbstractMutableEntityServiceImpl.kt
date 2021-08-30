package io.openenterprise.service

import io.openenterprise.data.domain.AbstractMutableEntity
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

abstract class AbstractAbstractMutableEntityServiceImpl<T : AbstractMutableEntity<ID>, ID : Serializable> :
    AbstractAbstractEntityServiceImpl<T, ID>(), AbstractMutableEntityService<T, ID> {

    @kotlin.jvm.Throws(IllegalArgumentException::class)
    @Transactional
    override fun update(entity: T) {
        if (entity.id == null) {
            throw IllegalArgumentException()
        }

        abstractEntityRepository.save(entity)
    }
}