package io.openenterprise.data.domain

import java.io.Serializable
import java.time.OffsetDateTime
import javax.persistence.PrePersist

abstract class AbstractMutableEntity<ID: Serializable> : AbstractEntity<ID>() {

    var updatedBy: String? = null

    var updatedOffsetDateTime: OffsetDateTime? = null

    @PrePersist
    override fun prePersist() {
        super.prePersist()

        updatedOffsetDateTime = if (updatedOffsetDateTime == null) OffsetDateTime.now() else updatedOffsetDateTime
    }
}