package io.openenterprise.data.domain

import java.io.Serializable
import java.time.OffsetDateTime
import javax.persistence.Id
import javax.persistence.MappedSuperclass
import javax.persistence.PrePersist
import javax.validation.constraints.Size

@MappedSuperclass
abstract class AbstractEntity<ID: Serializable> {

    @Id
    open var id: ID? = null

    @Size(max = 320)
    var createdBy: String? = null

    var createdDateTime: OffsetDateTime? = null

    @PrePersist
    protected open fun prePersist() {
        createdDateTime = if (createdDateTime == null) OffsetDateTime.now() else createdDateTime
    }
}