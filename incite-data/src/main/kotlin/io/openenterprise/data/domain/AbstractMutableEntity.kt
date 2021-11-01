package io.openenterprise.data.domain

import java.io.Serializable
import java.time.OffsetDateTime
import javax.persistence.PostLoad
import javax.persistence.PrePersist
import javax.validation.constraints.Max
import javax.validation.constraints.Size

abstract class AbstractMutableEntity<ID: Serializable> : AbstractEntity<ID>() {

    var before: AbstractMutableEntity<ID>? = null

    @Size(max = 320)
    var updatedBy: String? = null

    var updatedOffsetDateTime: OffsetDateTime? = null

    @PrePersist
    override fun prePersist() {
        super.prePersist()

        updatedOffsetDateTime = if (updatedOffsetDateTime == null) OffsetDateTime.now() else updatedOffsetDateTime
    }

    @PostLoad
    open fun postLoad() {
        before = this
    }
}