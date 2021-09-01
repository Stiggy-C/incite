package io.openenterprise.service

import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.repoistory.AbstractEntityRepository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import javax.inject.Inject

abstract class AbstractAbstractEntityServiceImpl<T: AbstractEntity<ID>, ID: Serializable>: AbstractEntityService<T, ID> {

    @Inject
    lateinit var abstractEntityRepository: AbstractEntityRepository<T, ID>

    @Transactional
    override fun create(entity: T): T {
        return abstractEntityRepository.save(entity)
    }

    @Transactional
    override fun create(entities: Collection<T>): List<T> {
        return abstractEntityRepository.saveAll(entities)
    }

    @Transactional(readOnly = true)
    override fun retrieve(id: ID): T? {
        return abstractEntityRepository.getOne(id)
    }

    @Transactional
    override fun delete(entity: T) {
        abstractEntityRepository.delete(entity)
    }

    @Transactional
    override fun delete(id: ID) {
        abstractEntityRepository.deleteById(id)
    }

}