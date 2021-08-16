package io.openenterprise.data.repoistory

import io.openenterprise.data.domain.AbstractEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.repository.NoRepositoryBean
import java.io.Serializable

@NoRepositoryBean
interface AbstractEntityRepository<T: AbstractEntity<ID>, ID: Serializable> : JpaRepository<T, ID>