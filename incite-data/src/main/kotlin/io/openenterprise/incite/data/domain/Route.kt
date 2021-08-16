package io.openenterprise.incite.data.domain

import io.openenterprise.data.domain.AbstractMutableEntity
import java.util.*
import javax.persistence.Entity
import javax.persistence.PrePersist

@Entity
class Route: AbstractMutableEntity<UUID>() {

    var yaml: String? = null

    @PrePersist
    override fun prePersist() {
        super.prePersist()

        id = UUID.randomUUID()
    }
}