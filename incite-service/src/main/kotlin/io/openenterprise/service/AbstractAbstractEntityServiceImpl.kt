package io.openenterprise.service

import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.data.repoistory.AbstractEntityRepository
import java.io.Serializable
import javax.inject.Inject

abstract class AbstractAbstractEntityServiceImpl<T: AbstractEntity<ID>, ID: Serializable>: AbstractEntityService<T, ID> {

    @Inject
    lateinit var abstractEntityRepository: AbstractEntityRepository<T, ID>

    override fun create(entity: T): T {
        return abstractEntityRepository.save(entity)
    }

    override fun create(entities: Collection<T>): List<T> {
        return abstractEntityRepository.saveAll(entities)
    }

    override fun retrieve(id: ID): T? {
        return abstractEntityRepository.getOne(id)
    }

    override fun delete(entity: T) {
        abstractEntityRepository.delete(entity)
    }

    override fun delete(id: ID) {
        abstractEntityRepository.deleteById(id)
    }

}