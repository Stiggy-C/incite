package io.openenterprise.data.domain

import java.io.Serializable
import java.time.OffsetDateTime
import javax.persistence.Id
import javax.persistence.MappedSuperclass
import javax.persistence.PrePersist

@MappedSuperclass
abstract class AbstractEntity<ID: Serializable> {

    @Id
    var id: ID? = null

    lateinit var createdBy: String

    var createdDateTime: OffsetDateTime? = null

    @PrePersist
    protected open fun prePersist() {
        createdDateTime = if (createdDateTime == null) OffsetDateTime.now() else createdDateTime
    }
}